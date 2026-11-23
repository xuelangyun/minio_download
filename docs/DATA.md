# 数据与配置参考

> 适用版本：2.2.x。配套：[开发文档](DEVELOPMENT.md) · [HTTP 接口](API.md)。
> 本文给出**所有持久化数据的结构**：运行时文件、本地数据目录、配置库、状态库、设置项、JSON 格式，以及上游数据源（PostgreSQL / MinIO）的约定。

## 目录

1. [运行时文件一览](#1-运行时文件一览)
2. [本地数据目录布局](#2-本地数据目录布局)
3. [配置库（config.sqlite3）](#3-配置库)
4. [设置项参考](#4-设置项参考)
5. [JSON 格式](#5-json-格式)
6. [状态库（state.sqlite3）](#6-状态库)
7. [上游数据源约定](#7-上游数据源约定)
8. [备份、迁移与重置](#8-备份迁移与重置)

---

## 1. 运行时文件一览

| 文件 | 默认位置 | 覆盖方式 | 内容 / 说明 |
|---|---|---|---|
| `minio_downloader_config.sqlite3`（+ `-wal` / `-shm`） | 程序目录 | 环境变量 `MINIO_DOWNLOAD_CONFIG_DB` | 配置库。**含 Secret Key / PG 密码明文**；POSIX 权限 0600 |
| `minio_download_state.sqlite3`（+ `-wal` / `-shm`） | 程序目录 | `MINIO_DOWNLOAD_STATE_DB` | 状态库：下载状态、已下载台账、暂停请求、上次运行结果 |
| `logs/minio_task_downloader.log` | 程序目录 `logs/` | `MINIO_DOWNLOAD_LOG_DIR` | 总日志；50 MB 轮转，保留 5 份 |
| `logs/episode_<episode_id>.log` | 同上 | 同上 | 单条 episode 的下载日志（追加写） |
| `logs/web_last_run.stderr` | 同上 | 同上 | Web 最近一次启动下载器的 stderr，每次启动覆盖 |
| `minio_download.lock` | 程序目录 | — | 进程锁（内容为空；由操作系统持有锁） |
| `minio_download.pid` | 程序目录 | — | `{"pid": N, "created": 进程创建时间戳, "written": 写入时间}`，下载器退出时删除 |

> 环境变量只用于**定位文件**，以及在配置库**首次创建**时导入初始值（见 §3.3）。

## 2. 本地数据目录布局

```
<磁盘路径>/<版本目录>/<场景>/<目录名>/
    <episode 目录名>/                  ← 正式目录：只在完整、已校验后才出现（rename 而来）
        task_info.json
        <...>.SYNC.mcap
        <其它对象，保持与 MinIO 前缀下相同的相对路径>
    .<episode 目录名>.partial/         ← 暂存目录：下载中 / 暂停后 / 失败后保留，供续传
        task_info.json                    已完成并校验的文件（正式文件名）
        .<文件名前 60 字节>.<12 位标记>.minio-tmp    正在写的临时文件
```

* `<版本目录>/<场景>/<目录名>` 由 `task_dir_name()` 生成，每一级经 `safe_component()` 处理（替换非法字符、去掉首尾空格和点；Windows 保留名加 `_` 前缀）。**目录名**默认等于任务名，可在任务配置里单独指定（多个任务可共用一个目录名，此时它们必须在同一块磁盘）。
* `<episode 目录名>` 是 `parent_key` 的最后一级（全局唯一，不加前缀）。
* **临时文件的 12 位标记** = `sha1(etag | size | mtime)[:12]`：MinIO 上的对象被替换后标记会变，旧临时文件自动作废。文件名部分按 UTF-8 字节截断到 60，保证不超过文件名长度上限。
* 正式文件的修改时间被设置为对象在 MinIO 上的修改时间；“是否已完成”的判断 = 大小一致且修改时间相差 < 2 秒。
* 暂存目录内**不属于本次清单**的文件（旧临时文件、多余文件）会在下载前后被清理，因此它必须是专用目录。

## 3. 配置库

### 3.1 表结构（schema 版本 2）

```sql
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS scenes (
    id INTEGER PRIMARY KEY AUTOINCREMENT, position INTEGER NOT NULL, name TEXT NOT NULL UNIQUE);
CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scene_id INTEGER NOT NULL REFERENCES scenes(id) ON DELETE CASCADE,
    position INTEGER NOT NULL, name TEXT NOT NULL UNIQUE,
    target_count INTEGER NOT NULL CHECK (target_count >= 0),
    directory TEXT NOT NULL DEFAULT '',
    tags TEXT NOT NULL DEFAULT '[]');
CREATE TABLE IF NOT EXISTS task_aliases (
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    position INTEGER NOT NULL, alias TEXT NOT NULL, PRIMARY KEY (task_id, alias));
CREATE TABLE IF NOT EXISTS plans (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    note TEXT NOT NULL DEFAULT '',
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS disks (
    name TEXT PRIMARY KEY, position INTEGER NOT NULL, path TEXT NOT NULL, mountpoint TEXT NOT NULL);
```

| 表 | 说明 |
|---|---|
| `meta` | `schema_version`、`seeded_at`（有它表示已初始化过，不再导入默认值） |
| `settings` | 键值对；`value` 是 JSON 文本。含普通设置项、`tasks.data_version`（版本目录）、`web.token_hash`（PBKDF2 哈希，格式 `pbkdf2_sha256$迭代$盐$哈希`） |
| `scenes` / `tasks` / `task_aliases` | 场景 → 任务 → 历史别名；`position` 决定顺序（即下载顺序）；`tasks.tags` 是 JSON 数组（仅元数据，下载器不读取）；任务名、场景名全局唯一 |
| `plans` | 任务方案；`payload` 是任务 JSON 快照（§5） |
| `disks` | 目标磁盘；`position` 决定顺序（并列时的分配顺序） |

* 连接参数：WAL 模式、`busy_timeout=30000`、`foreign_keys=ON`；写用 `BEGIN IMMEDIATE`，读用只读事务。
* **升级**：旧库打开时自动补 `tasks.tags` 列并新建 `plans` 表，数据保留；`schema_version` 更新为 2。

### 3.2 访问约定

* 应用代码只通过 `config_db.ConfigStore` 访问，不要手写 SQL 改配置表（会绕过校验与事务）。
* 下载器与 Web 读取配置一律用 `snapshot()`（同一事务内读出全部，不会读到半新半旧）。
* 密钥项（`minio.secret_key`、`pg.password`）：`public_settings()` 返回空串 + `secrets_set`；写入时 `None` = 不修改、`""` = 清除。

### 3.3 首次初始化（seed）

配置库第一次创建（`meta.seeded_at` 不存在）时，`open_config_store()` 调用 `build_seed()` 并 `seed(...)` 一次。来源，**后者覆盖前者**：

1. 代码内置默认值（`SCENE_TASKS` 43 个任务、Linux 下三块默认磁盘；Windows 无默认磁盘）；
2. 旧版 `tasks_config.json`（路径可用 `MINIO_TASKS_CONFIG` 指定）；
3. MinIO 连接：`minio_config.json` → `~/.mc/config.json` 里的 `minio1` alias（只读文件，不执行 mc）→ 环境变量 `MINIO_ENDPOINT` / `MINIO_ACCESS_KEY` / `MINIO_SECRET_KEY` / `MINIO_SECURE` …；
4. 其它环境变量：`MINIO_BUCKET`、`MINIO_PG_HOST / PORT / DATABASE / USER / PASSWORD`、`MINIO_REQUIRE_MCAP`、`MINIO_FILE_WORKERS` / `MINIO_FILE_RETRIES` / `MINIO_RETRY_DELAY` / `MINIO_CONNECT_TIMEOUT` / `MINIO_READ_TIMEOUT` / `MINIO_FSYNC` / `MINIO_VERIFY_MD5`；
5. PG 密码：没有环境变量时沿用原脚本里的默认值（`LEGACY_PG_PASSWORD`），仅为升级后行为不变，请尽快修改；
6. Web 口令：`web_app.main` 在**没有口令时**用环境变量 `MINIO_WEB_TOKEN` 初始化一次。

同时把初始任务存成方案“初始配置”。**之后修改这些环境变量不再生效。**

## 4. 设置项参考

键格式 `分组.名称`，定义在 `config_db.SETTINGS`；类型：`str` / `int` / `float` / `bool`；🔒 = 密钥（不会返回浏览器）。
**所有设置在下一次启动下载时生效**（`web.*` 在重启 Web 后生效）。

| 键 | 类型 | 默认 | 范围 | 名称 | 作用于 |
|---|---|---|---|---|---|
| `minio.endpoint` | str | — | — | 服务地址 | store.config_from_settings → 客户端 |
| `minio.access_key` | str | — | — | Access Key | store.config_from_settings |
| `minio.secret_key` | str 🔒 | — | — | Secret Key | store.config_from_settings |
| `minio.secure` | bool | 是 | — | 使用 HTTPS | 仅当服务地址不带 http(s):// 时生效 |
| `minio.cert_check` | bool | 是 | — | 校验证书 | urllib3 `cert_reqs` |
| `minio.region` | str | — | — | 区域 | Minio(region=…) |
| `minio.bucket` | str | `rocp-data` | — | Bucket | `MINIO_BUCKET` |
| `pg.host` | str | `127.0.0.1` | — | 主机 | `PG_HOST`（fetch_rows） |
| `pg.port` | int | `5432` | 1 ~ 65535 | 端口 | `PG_PORT` |
| `pg.database` | str | `minio_event` | — | 数据库 | `PG_DATABASE` |
| `pg.user` | str | `postgres` | — | 用户名 | `PG_USER` |
| `pg.password` | str 🔒 | — | — | 密码 | `PG_PASSWORD` |
| `run.min_free_gb` | float | `3.0` | 0 ~ 1e+06 | 每块盘保留空间（GiB） | `MIN_FREE_GB`（DiskReservationManager） |
| `run.space_safety_factor` | float | `1.15` | 1 ~ 5 | 空间安全系数 | `SPACE_SAFETY_FACTOR` |
| `run.max_writes_per_disk` | int | `2` | 1 ~ 16 | 每块盘并发写入数 | `MAX_WRITES_PER_DISK`（任务内并发 episode 数） |
| `run.require_mcap` | bool | 是 | — | 要求每条数据含 mcap | `REQUIRE_MCAP`（check_required_files、cleanup） |
| `run.include_null_quality` | bool | 否 | — | 包含质量状态为空的数据 | `INCLUDE_NULL_QUALITY` → build_sql |
| `run.mcap_pattern` | str | `*.SYNC.mcap` | — | mcap 文件名规则 | `MCAP_FILE_PATTERN`（fnmatch，区分大小写） |
| `run.validate_workers` | int | `16` | 1 ~ 64 | 检查 MinIO 目录的并发数 | `VALIDATE_WORKERS`（pick_valid_rows 并发） |
| `run.default_rounds` | int | `1` | 1 ~ 50 | 默认补齐轮数 | `DEFAULT_FILL_ROUNDS`（--max-rounds 默认值；Web 开始下载的默认值） |
| `transfer.file_workers` | int | `4` | 1 ~ 16 | 单条数据内并行文件数 | store.FILE_WORKERS |
| `transfer.file_retries` | int | `5` | 1 ~ 20 | 单个文件最大尝试次数 | store.FILE_RETRIES |
| `transfer.retry_delay` | float | `1.0` | 0 ~ 60 | 重试间隔基数（秒） | store.RETRY_BASE_DELAY |
| `transfer.connect_timeout` | int | `10` | 1 ~ 300 | 连接超时（秒） | store.CONNECT_TIMEOUT（建客户端时） |
| `transfer.read_timeout` | int | `120` | 5 ~ 3600 | 读取超时（秒） | store.READ_TIMEOUT（建客户端时） |
| `transfer.fsync` | bool | 是 | — | 改名前 fsync | store.FSYNC |
| `transfer.verify_md5` | bool | 是 | — | 校验 MD5 | store.VERIFY_MD5 |
| `web.host` | str | `127.0.0.1` | — | 监听地址 | web_app.main（重启 Web 后生效） |
| `web.port` | int | `8765` | 1 ~ 65535 | 监听端口 | web_app.main（重启 Web 后生效） |

不在 `settings` 表里、但属于配置的两项：`tasks.data_version`（版本目录，随任务一起保存）、`web.token_hash`（访问口令哈希，只能通过口令接口写入）。

## 5. JSON 格式

### 5.1 任务 JSON（接口 / 方案共用）

```json
{
  "data_version": "银河2.2数据",
  "scenes": [
    {
      "scene": "Household(家居)",
      "tasks": [
        { "name": "0930_废物装箱", "count": 40,
           "aliases": ["0902_废物装箱", "0904_废物装箱"],
           "dir": "废物装箱",
           "tags": ["桌面", "分拣"] }
      ]
    }
  ]
}
```

| 字段 | 规则 |
|---|---|
| `data_version` | 版本目录；写入当前配置时不能为空；方案里可为空（载入时沿用当前） |
| `scene` | 非空；场景名全局唯一 |
| `name` | 非空；任务名全局唯一；不能与别名、其它任务名冲突 |
| `count` | 非负整数（0 = 不下载新数据） |
| `aliases` | 数据库里的历史任务名，会并入该任务；不能与其它任务名 / 别名冲突 |
| `dir` | 本地目录名；空 = 同任务名；不同任务共用同名目录时必须同盘 |
| `tags` | 字符串数组；每个 ≤ 24 字符，最多 12 个，去重保序；元数据，下载器不读取 |

校验入口：`web_app._check_tasks` → `config_db.tasks_from_dict` + `minio_task_downloader_by_count._build_task_config`。

### 5.2 方案导出文件

```json
{ "format": "minio-downloader-plan", "version": 1, "name": "桌面整理-第一批", "note": "说明", "payload": { /* 任务 JSON */ } }
```

导入也接受 `{"plan": <上面的对象>}`。方案名最长 60 字符；同名导入自动加 ` (2)`、` (3)`。

### 5.3 磁盘

```json
{ "name": "disk1", "path": "D:\\data", "mountpoint": "D:\\" }
```

`mountpoint` 缺省 = `path`。下载器启动时要求：挂载点真实存在且真是挂载点 / 盘符根目录；`path` 在挂载点之下；各条目名称不重复、不在同一文件系统。

## 6. 状态库

由下载器创建和写入（`StateDB`）；Web 只读，仅写 `control.stop_requested`。WAL 模式。**每个线程使用自己的连接。**

```sql
CREATE TABLE downloads (
    episode_id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    parent_key TEXT,
    disk_name TEXT,
    target_path TEXT,        -- 正式目录
    partial_path TEXT,       -- 暂存目录（续传用）
    expected_bytes INTEGER,  -- 数据库里的 file_size，用于预留空间
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    started_at TEXT, completed_at TEXT,
    updated_at TEXT NOT NULL
);
CREATE INDEX idx_download_task_status ON downloads(task_name, status);

CREATE TABLE task_disk_assignments (
    task_name TEXT NOT NULL, disk_name TEXT NOT NULL, last_used_at TEXT NOT NULL,
    PRIMARY KEY (task_name, disk_name)
);

CREATE TABLE downloaded_episodes (      -- 已下载台账：确认完成（或在磁盘上发现已有副本）后才写
    episode_id TEXT PRIMARY KEY, task_name TEXT NOT NULL, source_task_name TEXT,
    job_id INTEGER, sn_code TEXT, parent_key TEXT, disk_name TEXT, target_path TEXT,
    file_size INTEGER, content_time REAL, create_time TEXT, downloaded_at TEXT NOT NULL
);
CREATE INDEX idx_downloaded_task ON downloaded_episodes(task_name);

CREATE TABLE control (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL);
```

### 6.1 `downloads.status`

`pending`（默认，几乎不出现）· `running` · `paused` · `completed` · `failed` · `waiting_space` · `retrying_space` · `missing_task_info` · `missing_mcap` · `invalid_source` · `invalid_size`。含义与转换见 [开发文档 §5](DEVELOPMENT.md#5-episode-状态机)。

### 6.2 `control` 的键

| 键 | 值 | 写入者 | 生命周期 |
|---|---|---|---|
| `stop_requested` | ISO 时间（任意非空值即有效） | Web“暂停” / `--stop` | 下载器启动时清除；Web 开始下载前也会清除 |
| `last_run` | JSON：`{"result": "finished"｜"paused"｜"error", "at": ISO 时间, "counts": {…}, "mode": "cleanup_dry", "error": "…"}` | 下载器（`record_last_run`） | 每次运行结束覆盖 |

`counts` 的键是 `download_one` 的返回值：`completed / skipped / stopped / failed / waiting_space / missing_task_info / missing_mcap / invalid_source / invalid_size`。

### 6.3 台账 `downloaded_episodes` 与进度

* 概览的“已下载 N 条 / 时长 / 大小”= 台账（与 `downloads` 里 `completed` 且不在台账中的补充）**且正式目录仍然存在**；记录在但目录被删的计入 `stale_records`，不算进度（下次运行会重新下载）。
* `content_time` 是秒；概览以小时显示。

## 7. 上游数据源约定

### 7.1 PostgreSQL（`fetch_rows` 的查询）

```sql
SELECT j.episode_id, j.job_id, j.task_name, j.sn_code, j.parent_key, j.file_size,
       j.content_time, j.parsed_status, j.quality_status, j.create_time, j.video_file_size
FROM public.minio_galbot_scene_task_jobs AS j
WHERE j.del_flag = 0
  AND <质量过滤：由 build_sql(include_null_quality) 生成>
  AND j.job_id IN (SELECT t.job_id FROM public.minio_galbot_scene_task AS t WHERE t.task_name = ANY(%s))
ORDER BY j.task_name ASC, j.create_time ASC, j.episode_id ASC
```

* `%s` = 所有配置的任务名 + 全部历史别名。
* 返回后：别名并入当前任务名（保留 `source_task_name`）→ 在任务内按 `(create_time 为空排后, create_time, episode_id)` 重新排序 → 按 `episode_id` 去重 → 再按 `parent_key` 去重（不同 episode 指向同一路径时只保留最早的一条，避免重复计入时长、本地指向同一目录）。
* 质量过滤默认：`j.quality_status NOT IN ('不合格', '未解析', '未质检')`（同时排除 NULL）；开启 `run.include_null_quality` 变为 `(j.quality_status IS NULL OR … NOT IN …)`。

### 7.2 MinIO 对象

* 一条 episode 对应桶内一个前缀 `parent_key/`（规范化后，不带首尾 `/`）。
* 必需文件：`parent_key/task_info.json`（必需）；`run.require_mcap` 开启时还需要一个匹配 `run.mcap_pattern`（默认 `*.SYNC.mcap`）的直接子文件。检查用一次非递归列目录完成。
* 下载时递归列出前缀下所有对象，保持相对路径落盘；以 `/` 结尾的零字节对象视为目录标记。

## 8. 备份、迁移与重置

| 想做的事 | 做法 |
|---|---|
| 备份配置 | 停止 Web，拷贝 `minio_downloader_config.sqlite3`（`-wal` / `-shm` 若存在一并拷贝，或先用 `sqlite3 … ".backup 新文件"`）。**备份文件含密钥明文** |
| 迁移到新机器 | 拷贝配置库；在新机器“磁盘”页改成本机路径；“连接”页重新测试。仅想迁移任务：用任务方案的导出 / 导入 |
| 重置下载进度 | 停止下载器，删除状态库。磁盘上的正式目录会被重新计入（`find_local_copy` 扫描所有磁盘）；`paused / running` 信息丢失，但仍会被选中的暂存目录会续传 |
| 重置配置 | 停止 Web 与下载器，删除配置库。下次启动重新初始化（默认任务 + 环境变量 / 旧文件导入）；连接、磁盘、方案、口令需重新设置 |
| 忘记 Web 口令 | 在本机运行 `python web_app.py --set-token 新口令` |
| 清掉卡住的暂停请求 | `delete from control where key='stop_requested'`（状态库） |
