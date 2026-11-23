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
- 暂存目录下载完成后原子 rename 发布；启动时清理不属于当前对象清单的残留文件，保留有效 Range 临时文件（仅接受普通文件，符号链接暂存文件会被拒绝并清理）；Episode 中已完整落盘且大小 / 修改时间与 MinIO 对象一致的文件会直接跳过，避免重试时重复下载，校验口径与 Python 版一致。 续传前、GetObject 元数据读取后及下载完成后都会核对对象大小 / ETag / LastModified；发现远端版本变化立即丢弃旧暂存文件并停止该对象重试，避免把不同版本对象拼接到同一文件。
- 跨平台单实例文件锁、原子 PID 文件与 SQLite `--stop` 暂停请求；启用 fsync 时，流式下载每写入 32 MiB 同步一次临时文件，单次请求结束 / 中断时再次同步；新进程首次续传会回退最近 64 MiB 后重下，降低崩溃后尾部未落盘风险；文件与发布目录也执行 fsync（Windows 不支持时安全忽略）。
- 任务级磁盘空间预检，默认预留 3 GiB，按 1.15 安全系数估算；每个对象开始前再次检查实际剩余空间，并以进程内预留账本保护并发对象；流式写入时逐字节扣减预留，断点续传只预留尚未下载的部分；新进程首次接续时会回退并重下最近 64 MiB，降低异常断电导致尾部数据未可靠落盘的风险，并把回退释放的空间重新计入预留；同一进程内网络重试不重复回退；若重试流程丢弃本次运行刚写入的暂存数据，会把对应字节重新计入预留，避免预留账本与实际磁盘占用脱节。对象级空间不足首次发生后，会停止该 episode 及其所属任务继续准入尚未开始的对象 / episode，已获得预留并开始的并发下载允许收尾；episode 标记为 `waiting_space`，有效暂存数据保留。同一磁盘上排队的多个任务会累计预留预计空间。启动磁盘调度前，Go 还会按文件系统设备标识获取跨进程 `flock`，同一主机上不同下载器实例即使配置了同一文件系统的不同目录，也不能并发占用；锁在进程退出或崩溃时由操作系统释放。该机制仅协调同一主机，不等同于跨主机共享存储锁。文件并发参数 `file_workers` 默认 4，与 `max_writes_per_disk` 分开控制；条数模式会先并发检查必需文件并补足有效候选，`validate_workers` 默认 16；单文件重试次数 `file_retries` 默认 5，退避基数 `retry_delay_seconds` 默认 1 秒。`verify_md5` 默认开启；若 MinIO 使用 SSE 且 ETag 并非对象内容 MD5，可按实际情况关闭。

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

    # 启动 Web API（默认 127.0.0.1:8765）
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
3. 将 `config_db` 设置为 Python 的 `minio_downloader_config.sqlite3` 文件路径；`state_db` 设置为 Go 独立状态库路径（例如 `go/minio_download_state.sqlite3`）；若希望 Python Web 识别 Go 进程和读取日志，还要把 `pid_file`、`log_dir` 分别指向 Python 的 `minio_download.pid` 和 `logs` 目录。Go 状态库与 Python 状态库并非完全兼容，生产环境应使用独立的 `state_db`。如确实需要共享 `control.stop_requested` 暂停信号，必须先备份并验证，不建议直接共用整个状态库。
4. 运行 `./minio-download --config config.json`，未指定 `--mode` 时会自动识别为 `count`。

Go 对配置库只读，不会改写 Python Web 保存的配置。当前导入 MinIO、PostgreSQL、TLS 证书校验策略、磁盘、任务顺序、别名、条数目标、校验/传输参数及 fsync 策略。`config_db` 目前只支持 `count` 模式；时长模式仍使用 `config.example.json` 中的 `target_hours`，避免把 Python 条数目标误解释为时长目标。

## Go Web API 当前范围

`--web` 可启动独立 HTTP 服务，提供配置快照 `GET /api/config`、设置项批量更新 `PUT /api/config/settings`、MinIO / PostgreSQL 连接测试 `POST /api/config/test`、任务配置更新 / 默认任务重置 `PUT /api/config/tasks`、`POST /api/config/tasks/reset`、磁盘配置更新 / 检查 `PUT /api/config/disks`、`POST /api/config/disks/check`、任务方案管理 `GET|POST /api/plans`、`GET|PUT|DELETE /api/plans/{id}`、`POST /api/plans/{id}/apply`、`GET /api/plans/{id}/export`、`POST /api/plans/import`，访问口令设置 / 轮换 / 清除 `PUT /api/auth/token`，以及状态 `GET /api/status`、`GET /api/episodes?task=任务名`、`GET /api/logs?offset=N`、`POST /api/run` 启动下载、安全暂停 `POST /api/stop` 和无需口令的 `GET /healthz`。`GET /api/config` 返回设置 schema、分组、任务 / 标签、磁盘、配置路径和运行状态；Secret Key / PostgreSQL 密码始终返回空串，并通过 `secrets_set` 标记是否已保存。`/api/run` 当前只接受 `mode=download`，支持 `max_rounds`（1–50）和 `recheck_missing`；命令行 `--max-rounds` 未指定时使用配置库 `run.default_rounds`，`--web-host` / `--web-port` 可覆盖配置库 `web.host` / `web.port`；Web 会以独立进程组启动下载器，关闭 Web 不会连带终止下载。暂停请求必须使用 `Content-Type: application/json`，请求体 `{}`；Web 支持安全暂停与强制结束：安全暂停保留临时文件供续传；强制结束会终止下载器进程树，当前文件的临时数据保留，下次运行按 Range 续传。状态接口返回任务标签、历史完成进度、磁盘空间/挂载状态、磁盘任务分配、运行中的 episode、汇总下载字节、近似速度、ETA、进度百分比和上次运行结果。速度基于两次轮询间暂存目录字节增量估算，并非网卡级精确吞吐。日志接口按 offset 增量读取完整行。Web API 默认只监听 `127.0.0.1:8765`；未配置 Python Web 访问口令时拒绝非本机监听。配置库已设置 PBKDF2 口令时，API 请求必须使用 `Authorization: Bearer <口令>`；5 分钟内连续 8 次错误口令会锁定该来源 60 秒。写请求必须使用 `Content-Type: application/json`，并校验同源 `Origin`。

## 当前限制

目前 Go Web 已实现基础状态页、配置查询与批量设置、任务和磁盘配置更新、MinIO / PostgreSQL 连接测试、任务方案 API（CRUD、应用、JSON 导入 / 导出）及访问口令设置 / 轮换 / 清除。`PUT /api/auth/token` 使用 `{ "action":"set", "token":"至少 12 字节的新口令" }` 设置或轮换口令，使用 `{ "action":"clear" }` 清除；口令只以 PBKDF2-SHA256 摘要保存，不返回明文。该接口仍受当前 API 鉴权保护：已设置口令时必须先携带旧口令完成认证；未设置口令时仅允许本机访问。清除后，非本机监听的既有服务会拒绝远程 API 请求；重启时若仍无口令则禁止绑定非本机地址。磁盘检查目前覆盖挂载状态、路径包含关系、可写性与可用空间；未挂载时不会向其上级系统目录写探测文件。

- 磁盘间同一文件系统检测：Linux 通过内核设备号识别，能够识别 bind mount / 设备别名；Windows 使用系统分区表中的卷设备标识，拒绝重复挂载同一卷。
- 任务方案的高级差异预览和冲突逐项交互处理。
- 完整 Python 状态库的所有历史兼容、精确实时速度 / ETA 汇总。
- 跨主机共享文件系统的磁盘协调、完整崩溃恢复与真实 MinIO / PostgreSQL 集成测试。

Go 可读取 Python 运行控制表中的 `stop_requested`（兼容 ISO 时间戳），`--stop` 和 Web `POST /api/stop` 都会写入同一控制表。要让 Python Web 控制台观察 Go 下载状态、进程和日志，请将两者的 `state_db`、`pid_file`、`log_dir` 指向同一位置。Go 使用 Python Web 预期的 `minio_task_downloader.log` 文件名，PID 文件包含兼容的 `pid` / `created` 字段。生产使用前应先在测试 bucket 验证。


Windows 文件发布：替换已有目标文件时先将旧文件改名为同目录备份，再发布新文件；发布失败时回滚旧文件，并对 Windows sharing violation / lock violation 最多重试 6 次。


Windows 文件发布与恢复：单文件和 PID 文件替换使用同目录备份、发布、失败回滚，并对Windows sharing violation / lock violation 最多重试 6 次；Episode 正式目录更新也先备份旧目录，发布失败时恢复旧目录，避免先删除旧数据再发布新数据造成数据丢失。

Episode 目录发布具备启动时崩溃恢复：备份目录按 episode 正式目录名隔离；若进程在旧目录移走后异常退出，下次处理该 episode 时会恢复最近的普通目录备份并保留 `.partial` 下载目录；若新正式目录已发布，则清理遗留备份。符号链接备份不会被自动跟随。

Episode 备份路径使用随机且尚不存在的同级路径，不创建临时空目录占位，避免进程在占位目录创建后崩溃时被误恢复为空 Episode；恢复扫描会清理遗留空备份目录，只恢复含有内容的普通目录。

跨进程磁盘锁文件位于配置的 `lock_file` 同级 `disk-locks` 目录。不同系统用户需要对该共享目录具有读写权限，并使用相同的 `lock_file` 路径，才能实现同机实例间协调。

SQLite 完成状态校验：默认不再仅凭正式目录存在就认定 Episode 已完成；必须是非符号链接的普通目录，且包含普通文件 `task_info.json`。启用 `require_mcap` 时，还必须存在匹配 `mcap_pattern` 的普通文件。`trust_completed_records=true` 仍保留历史归档记录信任行为。

SQLite 异常退出恢复：每次新一轮下载启动时，在选取任务前扫描遗留 `running` 记录。正式目录已满足完成文件要求的，恢复为 `completed`；否则重置为 `pending`，记录中断原因并保留 `partial_path`，供后续续传。该操作使用 SQLite 事务，重复执行安全，不删除临时文件。

缺失 MCAP 清理具备异常中断恢复：先在 SQLite 事务中将记录标记为 `cleanup_pending` 并移除完成台账，再删除 Episode 目录，最后删除清理状态记录。正常启动时会在验证所有配置磁盘挂载后恢复遗留清理项；清理目标必须位于配置磁盘目录内。`--cleanup-missing-mcap --dry-run` 只预览待清理和待恢复记录，不修改文件或数据库。
