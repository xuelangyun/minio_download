# 开发文档

> 适用版本：2.2.x。配套文档：[HTTP 接口](API.md) · [数据与配置参考](DATA.md) · [AGENTS.md](../AGENTS.md)（改代码前必读的约定清单）· [CHANGELOG.md](../CHANGELOG.md)。
>
> 这份文档回答三类问题：**系统怎么组成的**（§1–§3）、**关键流程怎么运转的**（§4–§6）、**怎么改、怎么测、怎么排查**（§7–§11）。
> 文中函数名均可在源码里直接搜索到；为避免过期，不使用行号。

## 目录

1. [设计目标与原则](#1-设计目标与原则)
2. [架构总览](#2-架构总览)
3. [模块参考](#3-模块参考)
4. [核心流程](#4-核心流程)
5. [Episode 状态机](#5-episode-状态机)
6. [跨平台设计](#6-跨平台设计)
7. [前端结构](#7-前端结构)
8. [开发环境与测试](#8-开发环境与测试)
9. [扩展指南（配方）](#9-扩展指南配方)
10. [约定、版本与发布](#10-约定版本与发布)
11. [调试手册](#11-调试手册)
12. [已知限制与改进方向](#12-已知限制与改进方向)

---

## 1. 设计目标与原则

**目标**：把 MinIO 中的机器人采集数据（episode）按“每个任务下载 N 条”的规则，下载到多块本地磁盘；全程可暂停 / 续传；任何时刻本地都不会出现残缺数据；用 Web 页面完成配置与监控；Windows 与 Linux / macOS 通用。

**设计原则**（按重要性排序，代码评审时以此为准）：

| # | 原则 | 落实在哪里 |
|---|---|---|
| P1 | **原子性**：正式路径只由原子操作产生（临时文件 → 校验 → fsync → `os.replace`；整条 episode 先放 `.partial` 暂存目录，再整体 `rename`） | `minio_store.download_prefix` / `publish_dir` |
| P2 | **先落盘，后记账**：SQLite 写 `completed` 一定发生在 `publish_dir` 成功之后；崩溃在两者之间由下次运行修复 | `download_one`、`find_local_copy` |
| P3 | **暂时性故障 ≠ 缺文件**：网络 / 5xx 只记 `failed`（可重试），绝不写 `missing_*`（默认被永久跳过） | `check_required_files`、`download_one` |
| P4 | **本地已有的数据不依赖网络** | `download_one`（先查本地，再访问 MinIO） |
| P5 | **配置单一来源**：所有配置在 SQLite 配置库；环境变量只用于“首次创建配置库时导入” | `config_db`、`apply_config`、`build_seed` |
| P6 | **Web 只做控制，不做下载**：下载在独立子进程；Web 对状态库只读，仅写“暂停请求” | `web_app.api_run` / `api_stop` |
| P7 | **密钥不出服务端**：Secret Key / PG 密码永不通过接口返回；口令只存哈希 | `ConfigStore.public_settings`、`set_web_token` |
| P8 | **跨平台逻辑集中在 `compat`**，业务代码不出现平台分支 | `compat.py` |
| P9 | **标签是纯元数据**：下载器不读取标签 | `ConfigStore.get_tasks` 不含标签 |

## 2. 架构总览

### 2.1 组件与数据流

```
┌────────────────────────────┐
│ 浏览器  index.html (单页)   │
└──────────────┬─────────────┘
               │ HTTP / JSON，Authorization: Bearer <口令>
               ▼
┌────────────────────────────────────────────┐        ┌─────────────────────────────┐
│ web_app.py  (Flask / waitress, 多线程)      │──读写─▶│ 配置库  config.sqlite3       │
│  guard() 鉴权·CSRF·限流 / 配置与方案接口     │        │  settings · scenes · tasks   │
│  进度统计 / 日志 / 进程控制                  │        │  task_aliases · plans · disks│
└───────┬───────────────┬────────────────────┘        └─────────────────────────────┘
        │ 只读 + 写 control │ subprocess.Popen（脱离父进程）
        ▼               ▼
┌───────────────┐   ┌──────────────────────────────────────────────────────────┐
│ 状态库         │◀─▶│ minio_task_downloader_by_count.py （下载器进程）           │
│ state.sqlite3 │   │  选条数 → 分盘 → 预留空间 → 下载 → 发布 → 记账             │
│ downloads …   │   │  watcher 线程：轮询 control.stop_requested → store.STOP   │
└───────────────┘   └───────┬────────────────┬──────────────────┬──────────────┘
                            │                │                  │
                            ▼                ▼                  ▼
                    PostgreSQL          minio_store.py      本地磁盘
                  (任务元数据,           (minio SDK)       .<episode>.partial/  → <episode>/
                   psycopg2)                │
                                            ▼
                                          MinIO
   compat.py：被 downloader / store / web 共用（进程锁、PID 文件、存活检查、结束进程树）
```

### 2.2 进程与线程模型

* **Web 进程**：一个进程，多线程处理请求；`child` 全局保存由它启动的下载器 `Popen`，另有一个线程 `_wait_child` 等待其退出以记录退出码。
* **下载器进程**（独立会话 / 进程组，关闭网页或终端不会连带结束）：
  * 每块磁盘一个**协调线程**（`process_rows_parallel` 中的 `minio-disk-coordinator` 线程池），按任务顺序处理该盘上的任务；
  * 一个任务内用线程池并发下载 episode，并发数 = **每盘并发写入数**（`run.max_writes_per_disk`）；
  * 每条 episode 内用线程池并行下载文件，并发数 = **单条内并行文件数**（`transfer.file_workers`）；
  * 一个 `stop-watcher` 守护线程轮询暂停请求。
* **最大并发连接数** ≈ 磁盘数 × 每盘并发 × 单条内并行文件数（默认 3 × 2 × 4 = 24）。`minio_store._build_client` 的 urllib3 连接池 `maxsize=32`：**调大并发参数时请同步调大它**，否则连接会排队。
* **单实例**：`compat.FileLock(minio_download.lock)` 由操作系统保证互斥（进程崩溃自动释放）；PID 与进程创建时间写在 `minio_download.pid`。

### 2.3 依赖方向（禁止反向 import）

```
compat  ◀──  minio_store  ◀──  minio_task_downloader_by_count  ◀──  web_app
              config_db  ◀────────────────┘                  ◀──────────┘
```

* `config_db` 与 `minio_store` 不得 import 下载脚本；下载脚本不得 import `web_app`。
* `minio_store` 只依赖标准库 + `compat`（`minio` / `urllib3` 在 `_build_client` 内惰性导入），因此没有 minio 包也能跑测试。

### 2.4 运行时文件

| 文件 | 位置（默认） | 说明 |
|---|---|---|
| `minio_downloader_config.sqlite3` | 脚本目录，`MINIO_DOWNLOAD_CONFIG_DB` 可改 | 配置库（含密钥，POSIX 权限 0600） |
| `minio_download_state.sqlite3` | 脚本目录，`MINIO_DOWNLOAD_STATE_DB` 可改 | 状态库 |
| `logs/minio_task_downloader.log` | `MINIO_DOWNLOAD_LOG_DIR` 可改 | 总日志，50 MB 轮转，保留 5 份 |
| `logs/episode_<id>.log` | 同上 | 单条 episode 的下载日志 |
| `logs/web_last_run.stderr` | 同上 | Web 启动的下载器的 stderr（排查“一启动就退出”） |
| `minio_download.lock` / `.pid` | 脚本目录 | 进程锁 / 运行中进程信息 |

详细表结构见 [DATA.md](DATA.md)。

## 3. 模块参考

### 3.1 `minio_store.py` — MinIO 访问层

唯一访问 MinIO 的地方。

| 类别 | 名称 | 说明 |
|---|---|---|
| 配置 | `StoreConfig`、`load_config`、`config_from_settings`、`set_config`、`apply_tuning`、`set_legacy_alias` | 连接配置与调优参数。运行时由下载器 `apply_config` 注入；`load_config`（环境变量 / `minio_config.json` / `~/.mc/config.json`）仅用于首次导入和独立命令行 |
| 客户端 | `get_client`（每进程一个）、`use_client`（注入，测试用）、`_build_client` | urllib3 `PoolManager`：连接超时 / 读超时、`Retry(total=5, 5xx)`、`maxsize=32` |
| 查询 | `stat_object`（不存在返回 `None`）、`object_exists`、`list_objects`、`list_names`、`verify_connection`、`test_connection` | 前缀一律规范成带结尾 `/` |
| 下载 | `download_prefix(bucket, prefix, dest, log_line, file_workers) -> DownloadReport` | 把一个前缀完整、原子地下载到暂存目录（§4.5） |
| 发布 | `publish_dir(staging, final)` | 同文件系统 `rename`；`final` 已存在抛 `FileExistsError` |
| 暂停 | `STOP`（`threading.Event`）、`StoreStopped`、`_check_abort` | §4.6 |
| 异常 | `StoreError` ← `StoreUnavailable` / `IntegrityError` / `DiskFull` / `StoreStopped` | 见下表 |
| 命令行 | `python minio_store.py check / ls / get` | 排查连接；`get` 与下载器使用同一套原子逻辑 |

异常分类（处理策略不同，**不要混用**）：

| 异常 | 含义 | 处理 |
|---|---|---|
| `StoreUnavailable` | 网络 / 超时 / 5xx / 提前断开 | 重试并续传 |
| `IntegrityError` | 字节数不符、MD5 与 ETag 不符、对象下载期间被改动 | 丢弃临时文件，重试；仍失败则整条失败 |
| `DiskFull` | `ENOSPC` / `EDQUOT` | 不重试；下载器清理暂存目录并换盘 |
| `StoreStopped` | 暂停请求 | 保留临时文件，向上传播，**不是失败** |
| `StoreError` | 其它不可重试（权限、对象名不安全、前缀为空） | 直接失败 |

内部要点：`_as_store_error` 把 SDK / urllib3 / 网络层 `OSError` 归类为 `StoreUnavailable`，**程序错误（TypeError 等）原样抛出**；`_local_io_error` 把本地写盘错误归类（`ENOSPC` → `DiskFull`）。

### 3.2 `config_db.py` — 配置库

* **设置项定义**：`SETTINGS`（`Setting` 数据类：键、默认值、类型、分组、标签、说明、是否密钥、范围）、`GROUPS`、`coerce`（类型转换 + 范围校验，错误信息为中文）、`schema()`（给前端生成表单）。
* **`ConfigStore`**：每次操作使用**短连接**，线程安全；写用 `BEGIN IMMEDIATE` 事务，读用只读事务 `_read()`（WAL 下互不阻塞）。主要方法：

  | 方法 | 说明 |
  |---|---|
  | `snapshot()` | **一个只读事务内**读出 `settings / data_version / scene_tasks / disks / tags`，下载器和 Web 都用它 |
  | `get_settings` / `set_settings(updates, skip_invalid)` | 密钥项传 `None` = 不修改，`""` = 清除 |
  | `public_settings()` | 给浏览器：密钥项一律返回空串，另给 `secrets_set` |
  | `get_tasks` / `get_tasks_dict` / `set_tasks(dv, scene_tasks, tags=None)` | `tags=None` 沿用同名任务原有标签 |
  | `get_task_tags` | 任务名 → 标签 |
  | `get_disks` / `set_disks` | |
  | `list_plans / get_plan / save_plan / update_plan / delete_plan` | 任务方案（§4.10） |
  | `has_web_token / set_web_token / verify_web_token / clear_web_token` | PBKDF2-SHA256，12 万次迭代 |
  | `seed(...)` / `is_seeded()` | 首次初始化，只执行一次 |

* **纯函数**（无数据库依赖，便于测试）：`normalize_tags`、`tasks_to_dict` / `tasks_from_dict`、`canonical_payload`、`plan_summary`、`filter_by_tags`、`merge_payloads`。
* **升级**：`ConfigStore.__init__` 里 `CREATE TABLE IF NOT EXISTS` + `PRAGMA table_info` 检查补列（schema 1 → 2 增加 `tasks.tags`）。新增列 / 表请沿用这个模式并提升 `SCHEMA_VERSION`。

### 3.3 `compat.py` — 跨平台

`FileLock`（POSIX `fcntl.flock`；Windows `msvcrt.locking` 锁第 0 字节）· `write_pidfile / read_pidfile / remove_pidfile` · `process_alive(pid, created)`（`psutil`，用进程创建时间防 PID 复用）· `kill_tree(pid, force)` · `detached_popen_kwargs()`（POSIX `start_new_session`；Windows `CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW`）。`IS_WINDOWS` 常量供其它模块判断。

### 3.4 `minio_task_downloader_by_count.py` — 下载器

约 2200 行，按职责分段（顺序即文件内顺序）：

| 段落 | 主要符号 | 说明 |
|---|---|---|
| 内置默认值 | `DATA_VERSION`、`SCENE_TASKS`、`DEST_DISKS`、`MIN_FREE_GB` … | 仅作首次导入的初始默认值和 import 兜底；运行时被 `apply_config` 覆盖 |
| 任务配置 | `_build_task_config`、`task_dir_name`、`tasks_to_json / tasks_from_json` | `_build_task_config` 校验（重名、别名冲突、目录冲突、条数非法），**所有任务配置变更都经过它** |
| SQL | `SQL_TEMPLATE`、`build_sql(include_null_quality)` | 质量状态过滤由它生成，不要直接改 SQL 文本 |
| 配置库接入 | `build_seed`、`open_config_store`、`config_snapshot`、`apply_config` | §4.8 |
| 状态库 | `StateDB`（`downloads` / `downloaded_episodes` / `task_disk_assignments` / `control`） | 每个线程自己一个连接，WAL |
| 路径与磁盘 | `safe_component`、`normalize_parent_key`、`build_disk_configs`、`target_paths`、`DiskReservationManager`、`candidate_disks` | 挂载点校验、空间预留 |
| MinIO 接入 | `mirror_prefix`、`check_required_files`、`verify_store` | 薄封装，调用 `minio_store` |
| 下载一条 | `download_one`、`download_worker` | §4.4 |
| 调度 | `_assign_tasks_to_disks`、`process_rows_parallel` | §4.3 |
| 选条数 | `fetch_rows`、`find_local_copy`、`sync_downloaded_record`、`pick_valid_rows`、`select_rows_for_target_count`、`run_fill_rounds` | §4.2 |
| 清理 | `sweep_stale_partials`、`cleanup_missing_mcap`、`local_has_mcap` | |
| 暂停与入口 | `install_signal_handlers`、`start_stop_watcher`、`record_last_run`、`request_stop`、`parse_args`、`main` | §4.1、§4.6 |

`download_one` 返回字符串状态码，`process_rows_parallel` 按它统计：`completed / skipped / stopped / failed / waiting_space / missing_task_info / missing_mcap / invalid_source / invalid_size`。

### 3.5 `web_app.py` — Web 后端

按区段：配置库访问（`get_cs`、`snapshot`）→ 安全（`guard`、`security_headers`）→ 下载器进程控制（`downloader_state`、`api_run`、`api_stop`）→ 进度统计（`_collect` 带缓存、`_collect_live` 实时）→ 日志 → 配置接口 → 任务方案接口 → 页面与 `healthz` → `main`。接口清单见 [API.md](API.md)。

### 3.6 `index.html` — 前端

单文件，无构建、无外部依赖，见 [§7](#7-前端结构)。

## 4. 核心流程

### 4.1 一次下载运行：`main()`

```
 1. apply_config(config_snapshot(open_config_store()))      读配置库；任务配置不合法 → 打印并退出 1
 2. parse_args()                                            --stop 则 request_stop() 后退出
 3. setup_logging()
 4. FileLock.acquire()                                      失败 → "另一个实例在运行"，退出 2
 5. write_pidfile; store.STOP.clear(); install_signal_handlers()
 6. db = StateDB(); db.clear_control("stop_requested"); start_stop_watcher()
 7. verify_store()                                          SDK 已装 / 凭据有效 / bucket 可读  (SetupError)
 8. build_disk_configs()                                    挂载点必须真实存在 (SetupError)
 9. [--cleanup-missing-mcap] cleanup_missing_mcap(...)      --dry-run 则到此结束
10. all_rows = fetch_rows()                                 PostgreSQL → 候选 episode（别名并入、按 create_time 排序）
11. run_fill_rounds(all_rows, disks, db, max_rounds)        选条数 + 下载，缺文件则改选补齐
12. STOP 置位 → record_last_run("paused") 返回 0；否则 log_target_report，record_last_run("finished")
    SetupError / NoSpaceAvailable / 其它异常 → record_last_run("error")
13. finally: remove_pidfile; FileLock.release()
```

退出码：0 正常 / 已暂停；1 有失败、配置错误或异常；2 已有实例；3 无可用空间。

### 4.2 选条数

`run_fill_rounds` 最多 `max_rounds` 轮，每轮：

1. `select_rows_for_target_count`，**对每个任务**（按配置顺序）：
   * 先把**已下载的**计入目标（`find_local_copy`：台账 → 状态库 `completed` → 扫描所有磁盘上的正式目录；只有真实存在的目录才算，被删掉的会重新下载），并 `sync_downloaded_record` 补记；
   * 以前记录为 `missing_*` 的默认直接跳过（`--recheck-missing` 才重查）；
   * 缺口 `gap = 目标 − 已下载`，由 `pick_valid_rows` 从“最早的、通过必需文件检查的”候选里补齐（并发检查，不合格的记录并跳过，直到凑够或遍历完）。
2. 第 1 轮前 `sweep_stale_partials`：删除**本轮不会续传**的 `.xxx.partial` 暂存目录（只允许删除目标磁盘内、名字形如 `.xxx.partial` 的目录）。
3. `process_rows_parallel` 下载。
4. 若有 `failed` / `waiting_space` / 已暂停 → 停止；若本轮有新增的 `missing_*` → 把它们排除后再来一轮，选后面的数据补齐。

**候选排序**：`fetch_rows` 在把历史别名并入当前任务后，按 `(任务名, create_time, episode_id)` 全局排序——“取最早的 N 条”依赖这一点（SQL 是按原任务名分组排的，不能依赖 SQL 顺序）。`create_time` 为空的排最后。随后先按 `episode_id` 去重，再按 `parent_key` 去重（不同 episode 指向同一路径时只保留最早的一条）。

**质量过滤**：`build_sql` 默认排除 `不合格 / 未解析 / 未质检`，并因 SQL 的 `NOT IN` 语义同时排除 `NULL`（沿用原行为）；设置 `run.include_null_quality` 开启后才包含 `NULL`。

### 4.3 磁盘分配与调度

* **单任务单盘**：`_assign_tasks_to_disks` 按任务顺序分配——已有数据的任务沿用数据所在的盘；数据同时出现在多块盘则标记冲突并跳过（需人工合并）；新任务分给“负载最小”的盘（并列时按配置顺序）；共用同一目录名的任务必须同盘。分配结果写入 `task_disk_assignments`。
* `process_rows_parallel`：每块盘一个协调线程，顺序处理其任务；任务内线程池并发 `max_writes_per_disk` 个 `download_worker`。每个 worker **自己打开一个 `StateDB` 连接**。
* **空间预留**：`DiskReservationManager.try_reserve` 原子地预留 `预计大小 × 安全系数`，并保证下载后仍剩余 `run.min_free_gb`；`download_one` 在拿不到空间时等待其它任务释放，或记为 `waiting_space`。
* 路径：`<磁盘路径>/<版本目录>/<场景>/<目录名>/<episode>`，暂存目录是同级的 `.<episode>.partial`（`target_paths`）。每一级都经过 `safe_component`（非法字符替换，Windows 保留名加 `_` 前缀）。

### 4.4 下载一条：`download_one`

```
 0. STOP 已置位 → return "stopped"（不改任何记录）
 1. 解析 parent_key（非法 → invalid_source）
 2. 本地扫描：已有完整正式目录 → 修复状态为 completed → return "skipped"        ← P4：此时不访问 MinIO
 3. 远端必需文件检查（一次列目录）：
      列目录失败          → 记 failed（保留 partial 信息），return "failed"      ← P3
      缺 task_info.json   → 记 missing_task_info，return
      缺 mcap（若要求）   → 记 missing_mcap，return
 4. 循环选盘（candidate_disks：优先续传已有暂存目录所在盘，其次按配置顺序）：
      try_reserve → 失败则等待 / 记 waiting_space
      记 running；mirror_prefix → store.download_prefix(...)
        "stopped"  → 记 paused（保留 partial_path），return "stopped"
        "ok"       → store.publish_dir(partial, final) → 记 completed → return "completed"
        "no_space" → 清理暂存目录，换下一块盘
        "failed"   → 记 failed（保留暂存目录，下次续传），raise RuntimeError
 5. finally: 释放预留
```

`download_worker` 在 `completed / skipped` 后把记录写入 `downloaded_episodes` 台账。

### 4.5 原子下载：`store.download_prefix`

```
list_objects(prefix, recursive) ─▶ 清单 [(key, rel, size, etag, mtime)]
   │  ├ 每个 rel 经 _safe_rel（拒绝 ..、绝对路径；Windows 还拒绝非法字符 / 保留名）
   │  └ 清单为空 → StoreError（绝不创建空目录）
   ▼
_sweep_foreign(dest)：删除暂存目录里“不在本次清单内”的文件（含旧临时文件、多余文件）
   ▼
线程池并行 _fetch_one(obj)：
   ├ _already_complete：正式名存在且 大小==对象大小 且 |mtime−对象mtime|<2s → 跳过
   ├ 临时文件  .<文件名(≤60字节)>.<sha1(etag|size|mtime)[:12]>.minio-tmp
   │    对象在 MinIO 上被替换 → 标记不同 → 旧临时文件自动作废
   ├ 续传偏移：
   │    本进程首次尝试：offset = max(0, 已有 − RESUME_BACKOFF(64 MiB))   ← 防断电脏尾
   │    同进程内重试：  offset = 已有
   ├ _pump：get_object(offset,length) → 逐块写；每 32 MiB flush+fsync；每块之前 _check_abort
   │    ETag 为 32 位十六进制（非分片）且 VERIFY_MD5 → 边写边算 MD5（续传时先对已有前缀算）
   ├ 校验：字节数 == size；MD5 == ETag；再 stat_object 一次，etag/size 不变（否则 IntegrityError）
   ├ os.utime(tmp, 对象 mtime)；_fs_retry(os.replace, tmp, 正式名)      ← 文件级原子
   └ 重试：最多 FILE_RETRIES 次，间隔 min(RETRY_BASE_DELAY × 2^次数, 30)s（可被暂停打断）
   ▼
最终核对：每个文件存在且大小一致；再次 _sweep_foreign；fsync 目录树
   ▼
（返回给 download_one）publish_dir：fsync 目录树 → rename 暂存目录 → 正式目录 → fsync 父目录   ← 目录级原子
```

错误传播：多个文件并行时，第一个错误置位内部 `abort`，其余文件在下一个数据块边界停下；最终按 `DiskFull` > `StoreStopped` > 其它 的优先级抛出。

### 4.6 暂停与继续

三个来源，**都只做一件事：`store.STOP.set()`**：

| 来源 | 机制 |
|---|---|
| Web“暂停” / `--stop` / `stop_download.*` | 往状态库写 `control.stop_requested`；下载器的 `stop-watcher` 线程每秒轮询 |
| SIGINT / SIGTERM / Windows `SIGBREAK` | `install_signal_handlers`：第一次 → 安全暂停；第二次 → `os._exit(130)`（数据仍安全） |
| `kill_tree(force=True)`（Web“强制结束”） | 直接结束进程；不走暂停路径，靠原子性保证安全 |

传播链：

```
STOP.set()
  → _pump 在下一个数据块边界 _check_abort → raise StoreStopped（先 flush+fsync 已写入的数据）
  → _fetch_one / download_prefix 向上抛（不被当成失败，不重试；重试等待也会被打断）
  → mirror_prefix 返回 "stopped"
  → download_one：记 paused（保留 partial_path），return "stopped"
  → process_one_disk 不再开始新任务；run_fill_rounds 不再开始新一轮
  → main：record_last_run("paused")，返回 0
```

**继续**：再次启动。`paused` 的 episode 照常被选中（它们不在 `MISSING_STATUSES` 里），暂存目录因“属于本轮选中的、未下载的 episode”而被 `sweep_stale_partials` 保留，`download_prefix` 按 §4.5 续传。

### 4.7 崩溃与异常恢复矩阵

| 场景 | 崩溃后的磁盘 / 状态 | 下次运行 |
|---|---|---|
| 写文件中途被 kill / 断网 | 正式名下无该文件；有 `.minio-tmp`；状态 `running` | 续传该文件（回退 64 MiB） |
| 所有文件写完，改名目录前崩溃 | 暂存目录完整；状态 `running` | 文件均“已完成”被跳过，直接发布目录 |
| 目录已 rename，SQLite 未写 `completed` | 正式目录完整；状态 `running` | `find_local_copy` 发现正式目录 → 修复为 `completed` |
| 磁盘写满 | 暂存目录被清理；状态 `retrying_space` / `waiting_space` | 换盘或等待空间 |
| 暂停 | 临时文件已 fsync；状态 `paused` | 续传 |
| 断电 | 同“被 kill”，且大文件尾部可能脏 | 回退 64 MiB 再续；ETag 可校验时 MD5 兜底 |
| 状态库被删 | 磁盘数据不受影响 | `find_local_copy` 扫描所有磁盘，把正式目录重新计入；`running/paused` 信息丢失，但被选中的暂存目录仍会被续传 |
| 配置库被删 | 下载数据不受影响 | 重新初始化（默认任务 + 环境变量 / 旧文件导入），连接 / 磁盘 / 方案需重新配置 |

### 4.8 配置的生命周期

```
首次创建配置库 ──▶ build_seed()：代码默认值 ← 旧 tasks_config.json ← minio_config.json / ~/.mc ← MINIO_* 环境变量
                       └─▶ ConfigStore.seed(...)  （仅一次；同时把初始任务存成方案“初始配置”）
Web 页面修改 ───────▶ ConfigStore.set_* （校验 → 事务写入）
下载器启动 ─────────▶ config_snapshot() → apply_config(snapshot)
                       ├ 先 _build_task_config 校验，通过才赋值（不合法则整体不生效）
                       ├ 覆盖模块级变量（MIN_FREE_GB、DEST_DISKS、PG_*、MINIO_BUCKET …）
                       └ store.apply_tuning / store.set_config(config_from_settings)
```

要点：**配置改动在“下一次启动下载”时生效**；Web 进程内**不要调用 `apply_config`**（它会改下载脚本的模块全局变量），Web 直接使用 `snapshot()` 返回的字典。

### 4.9 Web 后端要点

* **`guard()`（`before_request`，仅 `/api/*`）**，按顺序：① 写方法必须 `application/json`（否则 415）→ ② 带 `Origin` 必须同源（403）→ ③ 未设口令：只允许本机直连，且无 `X-Forwarded-For`、`Host` 必须是 localhost / 127.0.0.1 / ::1（403）→ ④ 已设口令：先看是否被锁定（同一地址 5 分钟内错误 8 次 → 429，锁 60 秒）→ 校验 `Authorization: Bearer`（通过校验的口令的 sha256 缓存在内存，改口令时清空；**没带口令不计入失败**）。
* `security_headers()`：`X-Frame-Options`、`nosniff`、`Referrer-Policy`、CSP（页面）、`Cache-Control: no-store`（接口）。
* **`downloader_state()`**：先看本进程的 `child`，再看 PID 文件 + `process_alive(pid, created)`（所以命令行启动的下载器也能被识别和暂停）。
* **`api_run`**：先 `setup_problems`（缺 MinIO 连接 / 磁盘 / 任务 → 400）；`MODES` 白名单拼参数；清掉遗留 `stop_requested`；`Popen(..., **detached_popen_kwargs())`，stderr 重定向到文件。
* **`api_stop`**：默认写 `stop_requested`（= 暂停）；`force=true` 才 `kill_tree`。
* **进度统计**：`_collect(snap)` 较重（逐条检查本地目录是否还在），带缓存（下载中 4 秒 / 空闲 15 秒；配置变化或 `?fresh=1` 立即失效）；正在下载的明细和控制项 `_collect_live()` 每次实时读取，前端据此计算速度。
* 状态库只读连接用 `Path.resolve().as_uri() + "?mode=ro"`（Windows 路径含空格 / 中文 / 盘符也正确）。

### 4.10 任务方案

* **数据模型**：`plans(id, name UNIQUE, note, payload, created_at, updated_at)`；`payload` 是任务 JSON 的**快照**（版本目录 + 场景 + 任务 + 条数 + 别名 + 目录名 + 标签），写入前经 `canonical_payload` 规范化。方案内允许 `data_version` 为空。
* **校验**：保存 / 导入 / 载入前都过 `web_app._check_tasks`（重名、别名冲突、目录冲突、名称为空、标签长度 / 数量）。
* **载入**（`api_plan_apply`）：先 `filter_by_tags`；
  * `replace`：整体替换任务配置，版本目录取方案的（空则沿用当前），标签取方案的；
  * `merge`：`merge_payloads` 只追加当前**没有**（按任务名）的任务，**已有任务和条数绝不改动**；每追加一个任务都用 `validate` 回调试校验，冲突的任务跳过并返回原因，不影响其它任务；
* **导入 / 导出**：导出为 `{"format": "minio-downloader-plan", "version": 1, "name", "note", "payload"}`；导入内容不可信，必须校验；同名自动加序号，不覆盖。
* **Go 兼容实现**：Go Web 已实现同一组方案列表 / 详情 / CRUD / replace / merge / 标签筛选 / JSON 导入导出接口。Go 校验允许方案 `data_version` 为空；`replace` 时继承当前配置版本目录，当前配置也为空则拒绝应用。对应测试覆盖空版本保存与载入。

## 5. Episode 状态机

`downloads.status`（只由下载器写入；Web 只读）：

```
                    ┌─────────────── (暂停) ───────────────┐
                    │                                       ▼
 (无记录) ──▶ running ──▶ completed                     paused ──(继续)──▶ running
              │  │
              │  ├─▶ failed ────(下次运行重试)────▶ running
              │  ├─▶ retrying_space ──(换盘)──────▶ running
              │  └─▶ waiting_space ─(等到空间)────▶ running
              │
 (校验阶段) ──┼─▶ missing_task_info ┐
              ├─▶ missing_mcap      ├─ 默认被跳过；--recheck-missing 才重新检查
              ├─▶ invalid_source    ┘  （invalid_* 需要修数据）
              └─▶ invalid_size
```

| 状态 | 写入者 / 时机 | 下次运行 |
|---|---|---|
| `running` | 开始下载时。下载器没在运行时残留的 `running` 表示上次被强制中断 | 续传 |
| `paused` | 暂停后 | 续传 |
| `completed` | `publish_dir` 成功之后 | 跳过；目录被删则重新下载 |
| `failed` | 下载失败 / 暂时连不上 MinIO（P3） | 重试 |
| `waiting_space` / `retrying_space` | 空间不足 | 重试 |
| `missing_task_info` / `missing_mcap` | 确认 MinIO 上缺必需文件 | 默认跳过（`MISSING_STATUSES`） |
| `invalid_source` / `invalid_size` | `parent_key` 非法 / `file_size` 无效 | 需修数据 |

新增状态码的清单见 [§9.2](#92-新增-episode-状态码)。

## 6. 跨平台设计

| 关注点 | POSIX | Windows | 位置 |
|---|---|---|---|
| 进程互斥 | `fcntl.flock` | `msvcrt.locking`（锁文件第 0 字节，PID 另存文件，避免读被锁挡住） | `compat.FileLock` |
| 进程存活 / 结束 | `psutil` | `psutil`；无 psutil 时 ctypes / `taskkill /T /F` | `compat.process_alive / kill_tree` |
| 脱离父进程 | `start_new_session` | `CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW` | `compat.detached_popen_kwargs` |
| 优雅暂停 | 状态库标志（不依赖信号） | 同左（Windows 无可靠的信号机制） | `stop_requested` |
| 磁盘用量 / 目录大小 | `shutil.disk_usage`、`os.scandir`（不用 `statvfs` / `du`） | 同左 | `directory_size_bytes` |
| 目录 fsync | `os.open(dir)` + `fsync` | 不支持，吞掉 `OSError` | `_fsync_dir` |
| 改名被占用 | — | `PermissionError`（杀毒 / 索引）重试 6 次 | `_fs_retry` |
| 文件名 | 仅拒绝 `..` / 绝对路径 | 另拒绝 `<>:"\|?*`、控制字符、末尾空格 / 点、保留名；本地目录名遇保留名加 `_` | `_safe_rel`、`safe_component` |
| 路径比较 | 区分大小写 | `os.path.normcase` | 挂载点校验 |
| 控制台编码 | UTF-8 | 子进程 `PYTHONUTF8=1`；`stdout.reconfigure(errors="replace")` | `setup_logging`、`api_run` |
| 脚本 | `.sh`（可执行） | `.bat`（纯 ASCII + CRLF，用 `.venv\Scripts\python.exe`） | |

**验证程度**：开发环境为 Linux。Windows 分支用 `mock.patch` 模拟测试逻辑（`tests/test_compat.py::WindowsBranchTests`），**未在真实 Windows 上实测**，见 [§8.4](#84-手动验证清单真实-minio-与真实-windows)。

## 7. 前端结构

* **单文件** `index.html`（约 1000 行）：CSS（设计令牌 + 组件）→ HTML（侧栏 + 顶栏 + 各页 `<section>`）→ JS。无构建、无 CDN；CSP 为 `default-src 'self'`。
* **主题**：`:root` 定义浅色令牌，`prefers-color-scheme: dark` 与 `[data-theme="dark"]` 覆盖；侧栏深色固定。
* **JS 分区**（顺序即文件内顺序）：基础（`$`、`esc`、`setHtml`、`api`、`confirmBox`、`formDialog`）→ 主题 → 路由（`route` / `applyRoute`）→ 状态轮询（`refresh`、`renderStatus`、`renderOverview`）→ 运行控制 → 配置页公共（`loadConfig`、`renderConfigPages`、`setState`、`reloadPage`）→ 按 schema 生成表单（`renderForms`、`fieldHtml`、`collect`）→ 任务编辑 → 任务方案 → 磁盘编辑 → 日志。
* **约定**：
  * 所有请求走 `api()`：统一加 `Content-Type: application/json`（后端对写操作强制要求）与 `Authorization`；401 弹出口令框后重试。
  * 输出到 HTML 的动态文本**一律 `esc()`**。
  * 会在“鼠标按下→松开”之间被重绘的区域用 `setHtml(el, html)`（内容没变就不动 DOM），否则点击会丢。
  * 配置页的未保存状态用 `dirtyPages` 跟踪（只有 `#cfg-scenes` / `#cfg-version` 等编辑器输入触发）：离开页面前确认；`renderConfigPages` 只重画**没有未保存修改**的页面；保存 / 放弃后先 `reloadPage()` 再刷新。
  * 设置表单**由 `/api/config` 返回的 `schema` 自动生成**，新增设置项通常无需改前端（§9.1）。
* **轮询节奏**：状态 2 秒（下载中）/ 6 秒（空闲）；日志 2 秒（仅日志页）。实时速度 = 相邻两次 `partial_bytes` 之差 / 时间，指数平滑（`updateRates`）。

## 8. 开发环境与测试

### 8.1 环境

```bash
python3 -m venv .venv && . .venv/bin/activate      # Windows: install.bat
pip install -r requirements.txt                    # minio、psycopg2-binary、flask、psutil、waitress
python web_app.py                                  # http://127.0.0.1:8765/
```

跑测试**不需要** MinIO、PostgreSQL、`minio` 包或网络（用内存版 MinIO 与 psycopg2 桩）：

```bash
python -m unittest discover -s tests -v            # 117 个测试，约 6 秒
python -m unittest tests.test_pause_resume         # 只跑一个文件
```

### 8.2 测试矩阵

| 文件 | 测试数 | 覆盖 |
|---|---|---|
| `test_minio_store.py` | 26 | 完整下载、Range 续传、MD5 损坏重下、对象被替换、磁盘写满、路径穿越、暂存目录清理、原子发布、连接配置解析 |
| `test_downloader_integration.py` | 9 | `download_one`：完成 / 跳过 / 缺文件 / 列目录失败不记缺文件 / 中断续传 / 磁盘写满换盘；脚本不含 mc 字样 |
| `test_pause_resume.py` | 11 | store 层暂停；完整 `main()`：暂停 → 继续、`--stop`、遗留暂停请求清理、实例互斥、配置缺失 / 非法 |
| `test_crash_recovery.py` | 1 | **真实子进程 `kill -9`** 后恢复，最终内容与源逐字节一致 |
| `test_selection_rules.py` | 9 | SQL 过滤、别名全局排序、PG 连接错误、本地已有数据不依赖 MinIO |
| `test_config_db.py` | 9 | 种子、事务回滚、设置项校验、密钥保护、口令哈希、文件权限、并发、快照一致性 |
| `test_plans.py` | 19 | 标签、旧库升级、方案 CRUD、过滤 / 合并、方案接口（保存 / 载入 / 追加冲突 / 导入导出） |
| `test_compat.py` | 8 | 文件锁（含持有者被杀后释放）、PID 文件、结束进程树、Windows 分支模拟 |
| `test_web_app.py` | 25 | 配置接口、鉴权、CSRF / Host / Origin / 限流 / 安全头、开始·暂停·强制结束、进度统计、日志 |

### 8.3 写测试的配方

* **内存版 MinIO**：`tests/fake_minio.FakeMinio`，通过 `store.use_client(fake)` 注入。故障注入属性：`break_plan`（某对象每次 `get_object` 在多少字节后断线）、`corrupt_once`、`fail_list`、`on_get`（回调：下载中改对象 / 触发暂停）、`chunk_delay`（拉长下载窗口）。
* **不睡眠**：`mock.patch.object(store, "RETRY_BASE_DELAY", 0.0)`；缩小块与回退：`store.CHUNK_SIZE`、`store.RESUME_BACKOFF`；暂停轮询：`dl.STOP_POLL_SECONDS`。
* **跑完整 `main()`**：继承 `test_pause_resume.MainFlowBase`——它会补丁 `CONFIG_DB / STATE_DB / LOG_DIR / LOCK_FILE / PID_FILE`、`fetch_rows`、`build_disk_configs`，并在 `addCleanup` 里还原 `apply_config` 改过的全局变量、日志处理器、信号处理器、`store.STOP`。**自己写类似测试必须照做**，否则会污染同进程里的其它测试。
* **Web 测试**：`web_app.app.test_client()`；setUp 里清空 `_cs_cache`、`_cache`、`_token_ok`、`_failures`，并补丁 `dl.CONFIG_DB` 等路径。写接口测试要带 `json=`（否则会因 415 失败——这是有意的）。
* **psycopg2**：测试开头 `try: import psycopg2 except ImportError: sys.modules["psycopg2"] = types.ModuleType("psycopg2")`；需要查询行为时用 `test_selection_rules.FakePg`。
* **避免测试重复执行**：公共 `setUp` 放在没有 `test_*` 方法的基类里（见 `test_plans.StoreBase`），不要让子类继承带用例的父类。
* **不要在 `pkill -f` 里写会匹配自身命令行的模式**（开发时用 PID 文件管理后台服务）。

### 8.4 手动验证清单（真实 MinIO 与真实 Windows）

自动化测试无法覆盖这两类环境。改动相关代码或首次上线前，请按序验证：

1. `python minio_store.py check`：连接、凭据、bucket 权限。
2. `python minio_store.py get <某个 episode 前缀> <空目录>`：单条原子下载；比对文件数 / 大小；确认没有 `.minio-tmp` 残留。
3. 一个目标条数为 1 的小任务跑完整流程；检查目录结构、`completed` 记录、日志。
4. **暂停 / 继续**：下载大文件中途点“暂停”，确认临时文件保留、`paused`；再继续，确认日志里出现“续传 … 从 N / M 字节继续”。
5. **强制结束**（任务管理器 / `kill -9`）后重启：确认能续传，最终文件逐字节一致（`sha256sum`）。
6. 拔网线 / 防火墙拦截 30 秒：确认文件级重试与续传，不出现 `missing_*`。
7. **Windows 额外**：路径含空格 / 中文；盘符挂载点校验；长路径；杀毒软件开启时改名重试；关闭 `run_web.bat` 窗口后下载进程是否继续；`stop_download.bat` 与页面“暂停”。
8. 分片上传的大对象（ETag 带 `-`）与开启 SSE 的对象：确认行为符合预期（只校验字节数 / 需关闭 MD5 校验）。

## 9. 扩展指南（配方）

### 9.1 新增设置项

1. 在 `config_db.SETTINGS` 增加一项（键 `分组.名称`、默认值、类型、范围、`secret`）。
2. 在 `apply_config()`（运行类）或 `store.apply_tuning()`（传输类）里应用它。
3. **前端无需改动**：表单由 `schema()` 自动生成；只有要放进新分组时才加 `GROUPS` 与对应页面容器。
4. 如果它也该出现在状态页，在 `api_status` 的 `settings` 里补字段。
5. 更新 [DATA.md](DATA.md) 设置项表与 README 设置项表；加测试（`test_config_db` 校验范围，`test_pause_resume`/集成测试验证生效）。
6. 密钥类设置：确认 `public_settings` 屏蔽、不写日志。

### 9.2 新增 episode 状态码

1. `download_one` 里写入并返回该字符串；`process_rows_parallel` 的 `counts` 初值补一项。
2. 若它表示“确认缺数据，默认跳过”，加入 `MISSING_STATUSES`。
3. 前端 `index.html` 的 `STATUS` 映射补中文名与色调。
4. 更新 §5 状态机、[DATA.md](DATA.md)、README 状态表；补测试。

### 9.3 新增 HTTP 接口

1. 在 `web_app.py` 对应区段加路由；写操作**必须**能通过 `guard()`（前端用 `api()` 自动带 JSON 头）；返回统一的 `{"error": 中文原因}` 与合适的状态码。
2. 读取配置用 `snapshot()` / `get_cs()`；不要在 Web 进程里改下载脚本的全局变量。
3. 不要把密钥放进响应；不要拼接用户输入进命令行或 SQL。
4. 更新 [API.md](API.md)；在 `test_web_app.py`（或 `test_plans.py`）加测试，覆盖鉴权、非法输入、成功路径。

### 9.4 新增页面

1. `index.html`：`PAGES` 里登记（标题、副标题函数）；侧栏加 `<a data-page>`；加 `<section id="page-xxx" hidden>`。
2. 需要后端配置的页面：在 `route()` 里按需 `loadConfig()`；保存按钮走 `reloadPage()` + `setState(..., "ok")`；输入变化调用 `markDirty()`。
3. 新增会被轮询重绘的区域用 `setHtml`。

### 9.4b 新增筛选 / 选条规则

选条逻辑集中在 `fetch_rows`（SQL 与排序）、`select_rows_for_target_count`（计数与补齐）。**改动前先读 §4.2**：别名排序、质量过滤、`MISSING_STATUSES` 跳过、已下载先计数是相互依赖的。新增 SQL 条件请像 `build_sql` 一样参数化并做成设置项（默认保持旧行为），并在 `test_selection_rules.py` 加用例。

### 9.5 替换 / 扩展数据源（PostgreSQL → 其它）

下载器对数据源的唯一要求是 `fetch_rows()` 返回 `list[dict]`，每个 dict 至少含：`episode_id`、`task_name`、`parent_key`（桶内前缀）、`file_size`（字节，>0）、`content_time`、`create_time`，以及 `job_id / sn_code / parsed_status / quality_status / video_file_size`。别名并入与排序发生在 `fetch_rows` 内部，替换实现时保持这两步。连接参数继续放 `pg.*` 设置或新增一组设置项。

### 9.6 增强下载校验（例如校验和）

挂载点在 `minio_store._fetch_one`：下载完成后、`os.replace` 之前。注意：必须在**临时文件**上校验，通过后才改名；校验失败抛 `IntegrityError`（会丢弃临时文件并重试）。如果是对象级校验和（`x-amz-checksum-*`），读取方式在 `stat_object` 之后补充，并在 `RemoteObject` 增加字段。

## 10. 约定、版本与发布

* 代码风格、不可破坏的约定、提交前自查清单：**见 [AGENTS.md](../AGENTS.md)**（本文不重复）。
* 语言：面向用户的日志 / 错误 / 注释用中文，说清“发生了什么 + 怎么处理”；标识符用英文。
* **版本号**：`VERSION` 文件（语义化版本）；页面侧栏与 `/healthz` 显示它。对外行为变化（尤其是选条规则、默认值、数据格式）必须升版本并在 `CHANGELOG.md` 写明“会改变行为的修复”。
* **发布流程**：
  1. 全部测试通过；清理运行时产物（`__pycache__`、`*.sqlite3*`、`logs/`、`*.lock`、`*.pid`、`.env`、`.venv`）。
  2. 更新 `VERSION`、`CHANGELOG.md`、README / 本文档中的数字（测试数等）。
  3. `.bat` 保持纯 ASCII + CRLF；`.sh` 保持可执行位。
  4. 打 zip（顶层目录 `minio_downloader/`），在**干净目录解压**后重跑测试并冒烟：启动 Web、`/healthz`、鉴权、写接口 415。
* **配置库升级**：新增列 / 表时提升 `SCHEMA_VERSION`，并在 `ConfigStore.__init__` 里补迁移，加“从旧库升级后数据保留”的测试（见 `test_plans.py::test_upgrade_from_v1_database_keeps_data`）。

## 11. 调试手册

### 11.1 看日志

* 总日志 `logs/minio_task_downloader.log`（页面“日志”页实时看）；单条 `logs/episode_<id>.log`；Web 启动失败看 `logs/web_last_run.stderr`。
* 想看 SDK 请求细节：临时在 `setup_logging` 里把 `logging.getLogger("urllib3").setLevel(logging.DEBUG)`。

### 11.2 查状态库

```bash
sqlite3 minio_download_state.sqlite3
> select status, count(*) from downloads group by status;
> select episode_id, status, error, partial_path from downloads where status in ('failed','paused','running') limit 20;
> select * from task_disk_assignments;
> select key, value from control;                     -- stop_requested / last_run
> delete from control where key='stop_requested';     -- 手动清除卡住的暂停请求
```

* 让某条重新来过：`delete from downloads where episode_id='…'`；若它的正式目录还在，下次运行会按“本地已有”计入；想真的重下，先删目录。
* 一批 `missing_*` 想重新检查：命令行加 `--recheck-missing`，或页面“⋯”菜单里打开“重新检查缺文件”。

### 11.3 查配置库

```bash
sqlite3 minio_downloader_config.sqlite3
> select key, value from settings;                 -- 注意：密钥项是明文
> select s.name, t.name, t.target_count, t.tags from tasks t join scenes s on s.id=t.scene_id order by s.position, t.position;
> select id, name, updated_at from plans;
```

### 11.4 常见问题定位

| 现象 | 看哪里 |
|---|---|
| “开始下载”点了没反应 / 立刻回到空闲 | `logs/web_last_run.stderr`；概览顶部的“上次运行异常结束”横幅 |
| 暂停后很久没停 | 在等一次很慢的网络读取（读超时 `transfer.read_timeout`，默认 120 秒）；用“强制结束”，数据仍安全 |
| 概览显示“已中断” | 上次被强制结束 / 崩溃，这些条目下次续传 |
| 磁盘页提示未挂载 | `build_disk_configs` 的挂载点校验；路径必须在挂载点之下 |
| 反复 `MD5 与 ETag 不一致` | 对象启用了 SSE → 关闭“校验 MD5”；或确认是否被代理改写内容 |
| 某任务一直凑不满 | 日志里 `任务 X 已遍历完所有可检查的数据，仍缺 N 条` 一行会按原因分类（缺 mcap / 缺 task_info / 无效 / 检查失败） |
| Windows 上改名失败 | 杀毒 / 索引占用，已自动重试；仍失败则把数据目录加入排除项 |

### 11.5 本地复现一次暂停

```bash
python minio_task_downloader_by_count.py --max-rounds 1 &   # 下载中…
python minio_task_downloader_by_count.py --stop             # 请求暂停
# 日志出现：收到暂停请求 … 已按请求暂停 …；状态库里对应条目为 paused
python minio_task_downloader_by_count.py --max-rounds 1     # 继续：日志里 “续传 xxx：从 N / M 字节继续”
```

## 12. 已知限制与改进方向

**已知限制**

* 任务级空间预检是“整体放得下才开始”，放不下则该任务本轮跳过（沿用自原脚本）。
* 分片上传的对象 ETag 不是内容 MD5，只能校验字节数；SSE 对象需关闭 MD5 校验。
* 真实 MinIO / 真实 Windows 行为没有自动化验证（§8.4）。
* Web 只有“单口令”鉴权，没有用户 / 角色；口令经 HTTP 头传输，跨机访问请放在 HTTPS 反向代理之后。
* 任务元数据表名 `public.minio_galbot_scene_task_jobs` / `minio_galbot_scene_task` 写在脚本里。
* 同一时间只有一个下载器实例（设计如此）。

**可考虑的改进**

* 磁盘空间按“能放多少先下多少”的细粒度预检，替代整体预检。
* 方案版本历史（每次覆盖保留旧版本）与“按标签批量设置条数”的方案级模板。
* 下载速度 / 预计剩余时间：前端已有 `partial_bytes` 与速度，可结合剩余条数与平均大小估算 ETA。
* 对象级校验和（`x-amz-checksum-*`）支持，弥补分片对象无法校验内容的缺口。
* 把 `minio_task_downloader_by_count.py` 按 §3.4 的段落拆成多个模块（状态库、调度、选条数），降低单文件规模；拆分时保持 §2.3 的依赖方向，并先补足对应的集成测试。


## 13. Go 版本（初始移植）

Go 实现位于 `go/`，与 Python 版本并行维护，不替换 `py_sample/` 或 `py_shell/`。

| 文件 | 说明 |
|---|---|
| `go/main.go` | CLI、PostgreSQL 候选查询、任务调度、MinIO 下载、兼容 Python 历史完成台账的 SQLite 状态恢复 |
| `go/config.example.json` | 按时长任务模板，对应 `py_shell/minio_task_downloader.py` |
| `go/config.count.example.json` | 按条数任务模板，对应 `py_shell/minio_task_downloader_by_count.py`；43 个任务，默认每任务 40 条 |
| `.github/workflows/go-release.yml` | Windows amd64、Linux amd64、Linux arm64 交叉构建与打包 |

构建要求 Go 1.23+。本地命令：

```bash
cd go
go mod tidy
go test ./...
go build -o minio-download .
```

按时长运行使用时长模板；按条数运行使用条数模板。两个模板的任务名、历史别名和目标目录必须分开维护，不能简单把两份 Python 清单合并，因为部分任务在两种脚本中的当前名称 / 历史别名方向不同。

Go 首版已覆盖：质量过滤、任务别名归并、排序后去重、按时长 / 条数选取、条数模式并发预检必需文件并补足有效候选、时长模式将上次缺文件任务排到候选末尾、按实际任务目录和磁盘负载分配任务、任务级空间预检（扣除已有临时文件大小，并累计预留同一磁盘上排队任务的预计空间）、每盘 episode 并发、单条内文件并发、Range 续传、ETag 条件请求、标准 MD5 ETag 校验、暂存目录清理、文件锁、SQLite 状态记录和原子目录发布。

**尚未达到 Python 全功能等价**：配置 SQLite、Web 完整控制台 / API、任务方案与标签、配置管理与连接测试、磁盘空间原子预留 / 写满换盘、完整 Python 状态机和历史状态库兼容。Go 当前的状态表与 Python 主要表字段尽量对齐，但不应假设两种实现可无条件共享同一个状态库。Go 首版必须先通过本地 `go test`、三个目标平台交叉构建和真实 MinIO / PostgreSQL 小样本联调，才可用于正式数据。
