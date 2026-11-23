# AGENTS.md

给 AI 编程助手（以及新加入的开发者）的项目说明。改代码前请先读完“不可破坏的约定”。

> 详细的架构、流程、接口与数据结构见 [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)、[docs/API.md](docs/API.md)、[docs/DATA.md](docs/DATA.md)；本文只列**约定**。
> 改动影响到文档里描述的行为 / 接口 / 表结构 / 设置项时，请同步更新对应文档。

## 项目是什么

把 MinIO 里的机器人采集数据（episode）按“每个任务 N 条”下载到多块本地磁盘，并提供 Web 控制台。
任务元数据来自 PostgreSQL，下载状态存本地 SQLite，**所有配置也存 SQLite**，文件通过 **minio Python SDK** 下载（不使用 mc）。
要在 Windows 与 Linux / macOS 上都能运行。

## 目录与职责

| 文件 | 职责 |
|---|---|
| `minio_store.py` | **唯一**访问 MinIO 的地方：客户端、列目录、`download_prefix`（原子下载 + 暂停）、`publish_dir`（原子发布） |
| `config_db.py` | 配置库（SQLite）：`SETTINGS` 设置项定义（含默认值、范围、是否密钥）、任务 / 磁盘 / 口令哈希的读写与校验 |
| `compat.py` | 跨平台：`FileLock`、PID 文件、`process_alive`、`kill_tree`、`detached_popen_kwargs` |
| `minio_task_downloader_by_count.py` | 选条数、分盘调度、空间预留、状态库、暂停机制、命令行入口；`apply_config()` 把配置库快照应用到模块变量 |
| `web_app.py` | Flask 后端：以**子进程**启动下载脚本；暂停 = 往状态库写 `stop_requested`；读写配置库；只读读取状态库与日志 |
| `index.html` | 单页前端（原生 JS，无构建、无外部 CDN）；设置表单由 `/api/config` 返回的 schema 自动生成 |
| `tests/` | `fake_minio.py`（内存版 MinIO）+ 单元 / 集成 / 暂停续传 / 崩溃恢复 / Web 接口测试 |
| `*.bat` / `*.sh` | 启动脚本（见下“脚本约定”） |

## 常用命令

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v      # 全部测试，约 6 秒；不需要 MinIO / PostgreSQL / minio 包 / 网络
python web_app.py                            # 启动 Web 控制台
python minio_task_downloader_by_count.py --max-rounds 1   # 命令行下载；--stop 请求暂停
python minio_store.py check                  # 连接排查（读配置库）
```

改完代码务必跑全部测试。改动 `minio_store.py` 的下载 / 续传 / 暂停 / 发布逻辑时，必须同时新增或更新测试，
并确认 `tests/test_crash_recovery.py`（真实子进程 kill -9）和 `tests/test_pause_resume.py` 仍然通过。

## 不可破坏的约定

### 数据原子性（最重要）

1. **正式路径只能由原子操作产生。** 文件：写 `.minio-tmp` → 校验 → fsync → `os.replace`。目录：在 `.<名字>.partial` 里下载完 → 核对清单 → `store.publish_dir`（`rename`）。**永远不要直接往正式文件 / 正式目录里写。**
2. 暂存目录与正式目录必须在**同一父目录**（同一文件系统），否则 `rename` 不再原子。
3. `.partial` 与 `.minio-tmp` 命名被 `sweep_stale_partials`、`_sweep_foreign` 依赖，不要改格式；清理逻辑只允许删除“目标磁盘内、名字形如 `.xxx.partial`”的目录。
4. `download_prefix` 会删除暂存目录里不属于本次清单的文件，所以它的 `dest` 必须是专用暂存目录，**绝不能传正式目录或用户目录**。
5. 校验失败（字节数 / MD5 / 对象被改动）必须丢弃临时文件并重试或失败，**不得**为了“让任务通过”而放行。
6. 先落盘、后记账：SQLite 写 `completed` 必须发生在 `publish_dir` 成功之后。崩溃在两者之间由下次运行的“发现已有目录 → 修复状态”逻辑兜底。

### 暂停与续传

7. 暂停信号是 `store.STOP`（`threading.Event`）。下载循环必须在**数据块边界**调用 `_check_abort`，抛 `StoreStopped`；任何地方**不得吞掉** `StoreStopped`，也不得把它当成失败。
8. 暂停时必须尽量把已写入的数据 flush + fsync（`_stream_to_tmp` 的 `except BaseException` 分支），保留临时文件与暂存目录；`download_one` 把该条记为 `paused`（保留 `partial_path`），而不是 `failed`。
9. 暂停请求的来源有三个：状态库 `control.stop_requested`（Web / `--stop`，由 `start_stop_watcher` 轮询）、SIGINT / SIGTERM / Windows 的 SIGBREAK（`install_signal_handlers`，第一次安全暂停、第二次 `os._exit`）。新增停止来源时只做一件事：`store.STOP.set()`。
10. 启动时必须清掉遗留的 `stop_requested`；结束时必须调用 `record_last_run`（finished / paused / error），Web 横幅依赖它。

### MinIO 访问

11. **不得**引入 `mc`、对 MinIO 的 `subprocess` 调用，或其它绕开 `minio_store` 的访问方式（`SourceHygieneTests` 会检查）。
12. 列目录 / stat 失败（网络、5xx）抛 `StoreUnavailable`，**不等于**“文件不存在”。只有确认缺文件才能写 `missing_task_info` / `missing_mcap`（它们默认会被一直跳过）；暂时性故障写 `failed`。
13. 异常分类：`DiskFull`（换盘）、`StoreStopped`（暂停）、`StoreUnavailable`（可重试 / 续传）、`IntegrityError`（丢弃重来）、`StoreError`（不可重试，如权限）。新增错误处理时沿用。
14. 对象名进入本地路径前必须经过 `_safe_rel`（拒绝 `..`、绝对路径；Windows 上还拒绝非法字符 / 保留名）。列前缀必须带结尾 `/`。
15. 客户端每个**进程**一个（`get_client` 按 PID 缓存）；线程间共享安全。不要在 fork 前创建并复用连接池。

### 配置（全部在 SQLite）

16. **所有可调配置都在配置库里**，通过 Web 页面维护；不要新增靠环境变量 / 配置文件 / 硬编码的配置项。环境变量只用于“首次创建配置库时导入”（`build_seed`）和定位 DB 文件路径（`MINIO_DOWNLOAD_CONFIG_DB` 等）。
17. 新增设置项的步骤：① 在 `config_db.SETTINGS` 加一项（默认值、类型、范围、`secret`）；② 在 `apply_config()`（或 `store.apply_tuning`）里应用；③ 前端表单**自动生成**，通常无需改 `index.html`（需要放到新分组时才改 `GROUPS` 和页面）；④ 更新 README 设置项表；⑤ 加测试。
18. 密钥（Secret Key、PG 密码）：**永远不要**经 Web 接口返回给浏览器（`public_settings` 返回空串 + `secrets_set`）；写入时 `None` 表示不修改、`""` 表示清除；不要写进日志、测试断言输出或提交。口令只存 PBKDF2 哈希。
19. 任务配置校验统一走 `_build_task_config`（重名、别名冲突、目录冲突、条数非法）；`ConfigStore.set_tasks` 是一个事务，要么全成功要么不变。读取配置用 `snapshot()`（一个只读事务，一致），不要分别调用 `get_tasks` / `get_disks` / `get_settings` 再拼。
20. 配置修改在**下次启动下载时**生效（`main()` 开头 `apply_config(config_snapshot(...))`）。Web 进程里不要调用 `apply_config`（它会改模块全局变量），Web 直接用快照字典。

### 任务标签与方案

- **标签是纯元数据**：只存在于配置库 `tasks.tags`（JSON 列表）和方案 payload 里，**下载器不得读取或依赖标签**。`ConfigStore.get_tasks()` 仍返回原来的元组结构（`SCENE_TASKS` 格式），标签走 `get_task_tags()` / `get_tasks_dict()` / `snapshot()["tags"]`。
- `set_tasks(dv, scene_tasks, tags=None)`：`tags=None` 表示沿用同名任务原有标签（下载器侧、“恢复默认”等不关心标签的调用不得抹掉用户的分类）；传字典才按它设置。
- 任务 JSON 的互转用 `config_db.tasks_to_dict` / `tasks_from_dict`（含标签）；脚本里的 `tasks_to_json` / `tasks_from_json` 只服务旧版 `tasks_config.json` 导入，不要再给新代码用。
- 方案 = 任务 JSON 的**快照**（`plans.payload`）。写入前一律经 `canonical_payload` 规范化、经 `web_app._check_tasks` 校验（重名 / 别名冲突 / 目录冲突）；方案内允许 `data_version` 为空（载入时沿用当前）。
- 载入方案：`replace` 整体替换（版本目录取方案的，空则沿用当前）；`merge` 只追加当前没有的任务（按任务名），**已有任务和条数绝不改动**，冲突的任务跳过并返回原因（`merge_payloads` 的 `validate` 回调逐个任务试加）。按标签过滤用 `filter_by_tags`。
- 新增方案相关接口仍要走 `guard()`（JSON、同源、口令）；导入文件内容不可信，必须校验后再存；导入同名自动加序号，不覆盖。
- 前端：编辑器的“未保存”只由 `#cfg-scenes` / `#cfg-version` 的输入触发；会在鼠标按下和松开之间被重绘的区域，一律用 `setHtml()`（内容没变不动 DOM），否则点击会丢。

### 调度与状态

21. 每个任务固定在一块磁盘；每块磁盘同一时间只跑一个任务；写入前必须经 `DiskReservationManager.try_reserve` 预留空间。
22. SQLite：WAL 模式，**每个线程 / worker 自己的 `StateDB` 连接**，不跨线程共用。`download_one` 返回字符串状态码，`process_rows_parallel` 的 `counts` 按它统计；新增状态码要同时更新 `counts` 初值、`MISSING_STATUSES`（如适用）、`index.html` 的 `STATUS`、README 状态表。

### Web 控制台

23. 启动下载只能通过 `MODES` 白名单拼参数，不要拼接用户输入进命令行。
24. 鉴权与加固（`guard()`）：写接口必须 `application/json`（防 CSRF，**新增写接口也要走 JSON，前端 `api()` 已统一设置**）；`Origin` 必须同源；设了口令则必须带 `Authorization: Bearer`，错误 8 次锁定 60 秒；**没设口令只允许本机直连**，带 `X-Forwarded-For` 或 `Host` 不是 localhost 的一律拒绝（防 DNS 重绑定）。非本机监听必须已设口令（`main()` 里检查）。不要放宽这些检查；新增响应头在 `security_headers()`。
25. Web 对状态库只读（`mode=ro`），仅有两处写入：`control.stop_requested`（暂停请求）和启动前清理它。下载状态由下载脚本负责。
26. 前端输出一律 `esc()` 转义后再拼进 HTML；不引入构建工具、不引用外部 CDN（内网可用；CSP 也是 `default-src 'self'`）。
    有未保存修改的配置页用 `dirtyPages` 跟踪：离开前确认、后台刷新不得覆盖；保存 / 放弃修改后先 `reloadPage()` 再刷新。

### 跨平台（Windows + POSIX）

27. 进程锁 / 存活检查 / 结束进程树 / 脱离父进程，一律用 `compat`。**不要**在业务代码里直接用 `fcntl`、`os.getpgid`、`os.killpg`、`/proc`、`os.statvfs`、`du`、`start_new_session`、`signal.SIGKILL`。
28. 磁盘用量用 `shutil.disk_usage`；目录大小用 `directory_size_bytes`（纯 Python）；比较路径用 `os.path.normcase`。
29. Windows 上改名 / 替换文件可能被杀毒软件短暂占用：用 `_fs_retry`。目录 fsync 在 Windows 上不可用，已吞掉 `OSError`，保持这个行为。
30. 文件名：本地目录名经 `safe_component`（Windows 保留名加 `_`）；MinIO 对象名经 `_safe_rel`。
31. 文本文件一律显式 `encoding="utf-8"`；子进程设置 `PYTHONUTF8=1`；控制台输出 `reconfigure(errors="replace")`，避免 GBK 控制台崩溃。
32. 目前**没有 Windows 实机测试环境**：Windows 分支用 `mock.patch` 模拟测试。改动这些分支时请在 README 的“Windows 注意事项”里如实说明验证程度。

### 脚本约定

33. `.bat` 文件必须是**纯 ASCII + CRLF**（消息用英文），避免代码页问题；用 `.venv\Scripts\python.exe`；`.sh` 文件需可执行（`chmod +x`）。

### 机密

34. 不要把 Access Key / Secret Key / 数据库密码 / 口令写进代码、日志、测试输出或提交。日志里只允许出现 endpoint 与配置来源。

## 代码风格

- Python 3.10+，`from __future__ import annotations`，尽量写类型标注。
- 面向用户的日志、错误信息、注释使用**中文**，说清“发生了什么 + 怎么处理”；标识符用英文。
- `minio_store.py` 除 `minio` / `urllib3`（在 `_build_client` 内惰性导入）与 `compat` 外只用标准库，保证没有 minio 包也能跑测试。

## 测试指南

- 用 `tests/fake_minio.py` 的 `FakeMinio` 注入：`store.use_client(fake)`；故障注入：`break_plan`（中途断线）、`corrupt_once`（内容损坏）、`fail_list`（列目录失败）、`on_get`（下载中改对象 / 触发暂停）、`chunk_delay`（拉长下载窗口）。
- 不睡眠：`mock.patch.object(store, "RETRY_BASE_DELAY", 0.0)`；缩小块大小 / 回退字节：`CHUNK_SIZE`、`RESUME_BACKOFF`；暂停轮询：`dl.STOP_POLL_SECONDS`。
- `tests/test_pause_resume.py` 的 `MainFlowBase` 演示了如何在进程内跑完整的 `main()`：补丁 `CONFIG_DB / STATE_DB / LOG_DIR / LOCK_FILE / PID_FILE`、`fetch_rows`、`build_disk_configs`，并在 `addCleanup` 里还原 `apply_config` 改过的全局变量、日志处理器和信号处理器。写类似测试时照此做。
- Web 测试用 `web_app.app.test_client()`，并清空 `_cs_cache`、`_cache`、`_token_ok`。
- 提交前检查：全部测试通过；没有残留的 `__pycache__`、`*.sqlite3*`、`logs/`、`*.lock`、`*.pid`、`.env`、`.venv`。

## 选条规则（改动前先读）

- `fetch_rows()` 把历史别名并入当前任务后，**必须**在任务内按 `create_time` 全局排序（`select_rows_for_target_count` 依赖“最早的 N 条”）。`tests/test_selection_rules.py` 覆盖。
- 质量状态过滤由 `build_sql(include_null_quality)` 生成：默认排除 不合格 / 未解析 / 未质检，**以及 NULL**（沿用原行为）；开启设置 `run.include_null_quality` 才包含 NULL。不要直接改 SQL 文本。
- `download_one` 先查本地（已有完整目录 → 记 `completed` 并跳过），**之后**才访问 MinIO 校验必需文件；本地已有的数据不得依赖网络。

## 已知问题（有意未改，改之前先与维护者确认）

- 任务级空间预检是“整体放得下才开始”。
- 真实 MinIO / 真实 Windows 的行为（`get_object` 的 Range、`list_objects` 分页、文件锁、进程组）在本仓库测试中由模拟替代，未自动化验证；改动相关逻辑请在真实环境手动验证（`minio_store.py get <prefix> <目录>` 可单独验证下载）。

## 提交前自查清单

- [ ] `python -m unittest discover -s tests` 全绿
- [ ] 没有新增对 `mc` 的依赖，没有绕过 `minio_store`
- [ ] 没有直接写正式路径；`publish_dir` 之后才写 `completed`；`StoreStopped` 没被吞掉
- [ ] 暂时性故障没有被记成 `missing_*`
- [ ] 新配置项加在 `config_db.SETTINGS`，没有新增环境变量 / 硬编码配置；README 设置项表已更新
- [ ] 没有在业务代码里直接使用 POSIX 专有调用（见约定 27）
- [ ] 新增 Web 写接口走 JSON、不绕过 `guard()`；新增页面输出已转义
- [ ] 更新了 `CHANGELOG.md`；改了对外行为时同步 `VERSION`
- [ ] 行为 / 接口 / 表结构 / 设置项变了：同步更新 `docs/` 下对应文档（接口 → API.md，表与设置项 → DATA.md，流程 → DEVELOPMENT.md）
- [ ] 没有提交密钥或运行时产物
