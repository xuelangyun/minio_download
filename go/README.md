# MinIO Download - Go 版本

Go 实现与 Python 版本并行维护，提供 Windows / Linux 命令行下载器。条数模式可以读取 Python Web 控制台使用的 SQLite 配置库和下载状态库；时长模式使用独立 JSON 配置。

## 功能

- PostgreSQL 查询 episode，按当前任务名和历史别名归并。
- 按累计视频时长（小时）或 episode 条数选择下载。
- 质量状态过滤、创建时间优先、episode_id / parent_key 去重。
- 使用 MinIO Go SDK，不调用 mc。
- 检查 task_info.json 与可选的 *.SYNC.mcap。
- 多磁盘任务固定分配；每盘任务顺序调度，每盘并发 episode 数可配；每条 episode 内文件并行下载（默认 4）。
- SQLite WAL 状态库与下载台账；按配置兼容 Python 的历史完成台账信任策略，即使旧磁盘归档也能正确计入目标。
- 暂存目录下载完成后原子 rename 发布；启动时清理不属于当前对象清单的残留文件，保留有效 Range 临时文件。
- 跨平台单实例文件锁、原子 PID 文件与 SQLite `--stop` 暂停请求；文件与发布目录执行 fsync（Windows 不支持时安全忽略）。
- 任务级磁盘空间预检，默认预留 3 GiB，按 1.15 安全系数估算；同一磁盘上排队的多个任务会累计预留预计空间，避免分别预检均通过但总量超出可用空间。文件并发参数 `file_workers` 默认 4，与 `max_writes_per_disk` 分开控制；条数模式会先并发检查必需文件并补足有效候选，`validate_workers` 默认 16；单文件重试次数 `file_retries` 默认 5，退避基数 `retry_delay_seconds` 默认 1 秒。`verify_md5` 默认开启；若 MinIO 使用 SSE 且 ETag 并非对象内容 MD5，可按实际情况关闭。

## 配置和运行

Go 版本按 Python 的两个 py_shell 脚本拆分了任务配置模板：
- `config.example.json`：按时长任务清单，对应 `minio_task_downloader.py`。
- `config.count.example.json`：按条数任务清单，对应 `minio_task_downloader_by_count.py`（43 个任务，默认每个任务 40 条）。

将所需模板复制为 `config.json`。不使用 Python 配置库时，填写 MinIO、PostgreSQL、磁盘路径和任务目标；不要提交真实密钥。时长模式的 `target_hours` 单位为小时，数据库 `content_time` 按秒累计；条数模式使用 `target_count`。两种模式必须使用各自对应的任务配置，避免历史别名和任务目录发生冲突。时长模板默认启用 `trust_completed_records`，与 `py_shell/minio_task_downloader.py` 一致：更换 / 归档磁盘后仍按历史完成台账计入目标。条数模板默认关闭该项，与 `py_shell/minio_task_downloader_by_count.py` 一致：只有配置磁盘上的正式目录真实存在才计入目标。

需要 Go 1.23+：

    cd go
    go mod tidy
    go build -o minio-download .
    cp config.example.json config.json
    ./minio-download --config config.json --mode duration

    # 另一个终端可请求安全暂停；运行中的进程会轮询 SQLite 控制位
    ./minio-download --config config.json --stop

    # 条数模式：使用独立的 43 任务配置
    cp config.count.example.json config.json
    ./minio-download --config config.json --mode count

    # 启动只读 Web API（默认 127.0.0.1:8765）
    ./minio-download --config config.json --web

    # 若要复用 py_sample 的配置库，在 config.json 中设置 config_db 路径；
    # state_db 指向 py_sample/minio_download_state.sqlite3 可共享暂停控制与状态。

Windows PowerShell：

    Copy-Item config.count.example.json config.json
    .\minio-download.exe --config config.json --mode count

## Python SQLite 配置库

条数模式可直接读取 `py_sample` 的配置库，不需要再复制 MinIO / PostgreSQL / 磁盘 / 任务参数：

1. 先在 Python Web 控制台完成配置并停止正在运行的下载器。
2. 复制 `config.count.example.json` 为 `config.json`。
3. 将 `config_db` 设置为 Python 的 `minio_downloader_config.sqlite3` 文件路径；`state_db` 设置为 Python 的 `minio_download_state.sqlite3` 路径；若希望 Python Web 识别 Go 进程和读取日志，还要把 `pid_file`、`log_dir` 分别指向 Python 的 `minio_download.pid` 和 `logs` 目录。
4. 运行 `./minio-download --config config.json`，未指定 `--mode` 时会自动识别为 `count`。

Go 对配置库只读，不会改写 Python Web 保存的配置。当前导入 MinIO、PostgreSQL、TLS 证书校验策略、磁盘、任务顺序、别名、条数目标、校验/传输参数及 fsync 策略。`config_db` 目前只支持 `count` 模式；时长模式仍使用 `config.example.json` 中的 `target_hours`，避免把 Python 条数目标误解释为时长目标。

## Go Web API 当前范围

`--web` 可启动独立 HTTP 服务，提供配置快照 `GET /api/config`、设置项批量更新 `PUT /api/config/settings`、MinIO / PostgreSQL 连接测试 `POST /api/config/test`、任务配置更新 / 默认任务重置 `PUT /api/config/tasks`、`POST /api/config/tasks/reset`、磁盘配置更新 / 检查 `PUT /api/config/disks`、`POST /api/config/disks/check`、任务方案管理 `GET|POST /api/plans`、`GET|PUT|DELETE /api/plans/{id}`、`POST /api/plans/{id}/apply`、`GET /api/plans/{id}/export`、`POST /api/plans/import`，以及状态 `GET /api/status`、`GET /api/episodes?task=任务名`、`GET /api/logs?offset=N`、`POST /api/run` 启动下载、安全暂停 `POST /api/stop` 和无需口令的 `GET /healthz`。`GET /api/config` 返回设置 schema、分组、任务 / 标签、磁盘、配置路径和运行状态；Secret Key / PostgreSQL 密码始终返回空串，并通过 `secrets_set` 标记是否已保存。`/api/run` 当前只接受 `mode=download`，支持 `max_rounds`（1–50）和 `recheck_missing`；命令行 `--max-rounds` 未指定时使用配置库 `run.default_rounds`，`--web-host` / `--web-port` 可覆盖配置库 `web.host` / `web.port`；Web 会以独立进程组启动下载器，关闭 Web 不会连带终止下载。暂停请求必须使用 `Content-Type: application/json`，请求体 `{}`；Web 支持安全暂停与强制结束：安全暂停保留临时文件供续传；强制结束会终止下载器进程树，当前文件的临时数据保留，下次运行按 Range 续传。状态接口返回任务标签、历史完成进度、磁盘空间/挂载状态、磁盘任务分配、运行中的 episode 和上次运行结果。日志接口按 offset 增量读取完整行。Web API 默认只监听 `127.0.0.1:8765`；未配置 Python Web 访问口令时拒绝非本机监听。配置库已设置 PBKDF2 口令时，API 请求必须使用 `Authorization: Bearer <口令>`；5 分钟内连续 8 次错误口令会锁定该来源 60 秒。写请求必须使用 `Content-Type: application/json`，并校验同源 `Origin`。

## 当前限制

目前 Go Web 已实现基础状态页、配置查询与批量设置、任务和磁盘配置更新、MinIO / PostgreSQL 连接测试、任务方案 API（CRUD、应用、JSON 导入 / 导出）；访问口令管理与 Python Web 完整页面尚未实现。磁盘检查目前覆盖挂载状态、路径包含关系、可写性与可用空间；未挂载时不会向其上级系统目录写探测文件。

- Web 访问口令修改 / 取消。
- 磁盘间同一文件系统检测。
- 任务方案的高级差异预览和冲突逐项交互处理。
- 完整 Python 状态库的所有历史兼容、精确实时速度 / ETA 汇总。
- 动态磁盘预留锁、完整崩溃恢复与真实 MinIO / PostgreSQL 集成测试。

Go 可读取 Python 运行控制表中的 `stop_requested`（兼容 ISO 时间戳），`--stop` 和 Web `POST /api/stop` 都会写入同一控制表。要让 Python Web 控制台观察 Go 下载状态、进程和日志，请将两者的 `state_db`、`pid_file`、`log_dir` 指向同一位置。Go 使用 Python Web 预期的 `minio_task_downloader.log` 文件名，PID 文件包含兼容的 `pid` / `created` 字段。生产使用前应先在测试 bucket 验证。
