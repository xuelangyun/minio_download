# MinIO Download - Go 版本

Go 版本采用 **Web 首次配置 + 单文件 SQLite 存储**。无需准备 JSON 配置文件，下载程序启动后直接打开网页，完成 MinIO、PostgreSQL、磁盘和任务配置，再从网页启动下载。

- Windows amd64、Linux amd64、Linux arm64。
- 首次启动自动创建 `minio_download.sqlite3`，并写入内置任务清单。
- MinIO / PostgreSQL 参数、任务、磁盘、任务方案、运行状态、下载台账、暂停控制和日志统一保存在同一个 SQLite 文件。
- 不另外生成 `config.json`、日志文件、PID 文件或锁文件。
- 下载的视频和必要的临时续传数据仍写入网页设置的目标磁盘目录。

## 启动与首次配置

直接执行程序：

Windows PowerShell：

    .\minio-download.exe

Linux：

    chmod +x ./minio-download
    ./minio-download

默认打开 `http://127.0.0.1:8765`。第一次启动会自动创建 SQLite 数据库；没有配置 MinIO / PostgreSQL 密钥和目标磁盘时，网页会提示需要补齐的配置，不会启动下载。

网页操作顺序：

1. 在“连接配置”填写 MinIO Endpoint、Access Key、Secret Key、Bucket，以及 PostgreSQL 连接信息；保存并分别测试连接。
2. 在“磁盘配置”通过图形界面添加磁盘，选择系统检测到的挂载点并设置下载目录；先检查磁盘再保存。
3. 在“任务配置”检查任务清单、数据版本目录、目标条数和目标时长（小时）；可将当前任务保存为方案，之后替换载入或追加载入重复使用。
4. 在“运行参数”调整磁盘预留空间、并发、校验和重试参数。
5. 返回“运行状态”，点击“开始下载”；日志和运行状态直接从 SQLite 读取。

默认只监听本机 `127.0.0.1:8765`。需要局域网访问时，先在本机“运行参数”页设置访问口令，再将 `web.host` 改成 `0.0.0.0` 并重启程序。不要在没有访问口令的情况下对公网开放控制台。

## SQLite 数据库

默认数据库文件：`minio_download.sqlite3`，位于程序启动时的当前工作目录。可用 `--db <路径>` 指定其他位置：

    ./minio-download --db /data/minio_download.sqlite3

数据表包括设置、任务 / 别名、磁盘、任务方案、运行控制、下载状态、下载完成台账、进程状态和运行日志。SQLite 使用 DELETE journal 模式，避免长期保留 WAL / SHM 辅助文件。数据库是唯一的程序配置与运行记录文件；不要手动删除运行中的数据库。

安全暂停通过 SQLite 控制表写入，不需要 PID 文件：

    ./minio-download --stop --db /data/minio_download.sqlite3

Web 页面也提供开始、暂停和强制停止操作。强制停止后保留有效临时数据，下次运行按 Range 续传。


## 功能

- PostgreSQL 查询 episode，按当前任务名和历史别名归并。
- 按累计视频时长（小时）或 episode 条数选择下载；目标时长与目标条数均保存在 SQLite 任务配置中。
- 质量状态过滤、创建时间优先、episode_id / parent_key 去重。
- 使用 MinIO Go SDK，不调用 mc。
- 检查 task_info.json 与可选的 *.SYNC.mcap。
- 多磁盘任务固定分配；每盘任务顺序调度，每盘并发 episode 数可配；每条 episode 内文件并行下载（默认 4）。
- 单文件 SQLite 配置与下载台账；使用 DELETE journal 模式，避免长期保留 WAL / SHM 辅助文件；按兼容策略处理历史完成记录。
- 暂存目录下载完成后原子 rename 发布；启动时清理不属于当前对象清单的残留文件，保留有效 Range 临时文件（仅接受普通文件，符号链接暂存文件会被拒绝并清理）；Episode 中已完整落盘且大小 / 修改时间与 MinIO 对象一致的文件会直接跳过，避免重试时重复下载，校验口径与 Python 版一致。 续传前、GetObject 元数据读取后及下载完成后都会核对对象大小 / ETag / LastModified；发现远端版本变化立即丢弃旧暂存文件并停止该对象重试，避免把不同版本对象拼接到同一文件。
- 下载进程状态、单实例租约和安全暂停标记保存在 SQLite；不创建 PID / 锁文件。启用 fsync 时，流式下载每写入 32 MiB 同步一次临时文件，单次请求结束 / 中断时再次同步；新进程首次续传会回退最近 64 MiB 后重下，降低崩溃后尾部未落盘风险；文件与发布目录也执行 fsync（Windows 不支持时安全忽略）。
- 任务级磁盘空间预检，默认预留 3 GiB，按 1.15 安全系数估算；每个对象开始前再次检查实际剩余空间，并以进程内预留账本保护并发对象；流式写入时逐字节扣减预留，断点续传只预留尚未下载的部分；新进程首次接续时会回退并重下最近 64 MiB，降低异常断电导致尾部数据未可靠落盘的风险，并把回退释放的空间重新计入预留；同一进程内网络重试不重复回退；若重试流程丢弃本次运行刚写入的暂存数据，会把对应字节重新计入预留，避免预留账本与实际磁盘占用脱节。对象级空间不足首次发生后，会停止该 episode 及其所属任务继续准入尚未开始的对象 / episode，已获得预留并开始的并发下载允许收尾；episode 标记为 `waiting_space`，有效暂存数据保留。同一磁盘上排队的多个任务会累计预留预计空间。启动磁盘调度前，Go 通过 SQLite `process_state` 租约保证同一数据库同一时刻只有一个下载进程；租约使用 PID 与进程启动时间识别过期记录，不创建锁文件。该机制用于协调同一主机共享同一 SQLite 文件的实例，不等同于跨主机共享存储锁。文件并发参数 `file_workers` 默认 4，与 `max_writes_per_disk` 分开控制；条数模式会先并发检查必需文件并补足有效候选，`validate_workers` 默认 16；单文件重试次数 `file_retries` 默认 5，退避基数 `retry_delay_seconds` 默认 1 秒。`verify_md5` 默认开启；若 MinIO 使用 SSE 且 ETag 并非对象内容 MD5，可按实际情况关闭。

## 命令行维护参数

正常使用只需直接启动程序并在网页完成配置。维护场景可使用：

    # 指定单文件 SQLite 数据库位置
    ./minio-download --db /data/minio_download.sqlite3

    # 命令行请求安全暂停
    ./minio-download --stop --db /data/minio_download.sqlite3

旧版 JSON 配置入口 `--config` 仅用于兼容已有部署；新安装不需要准备或维护该文件。

## 旧版 Python 配置库兼容

当前新 Go 版本默认使用自己的单文件 SQLite 数据库，不需要依赖或复制 Python 版本的配置库。原有 `--config <JSON>` 入口仅保留作旧版兼容，不作为新安装方式；新部署请直接启动程序并在网页配置。


## Go Web API 当前范围

`--web` 可启动独立 HTTP 服务，提供配置快照 `GET /api/config`、设置项批量更新 `PUT /api/config/settings`、MinIO / PostgreSQL 连接测试 `POST /api/config/test`、任务配置更新 / 默认任务重置 `PUT /api/config/tasks`、`POST /api/config/tasks/reset`、磁盘挂载点检测 `GET /api/config/disks/detect`、磁盘配置更新 / 检查 `PUT /api/config/disks`、`POST /api/config/disks/check`、任务方案管理 `GET|POST /api/plans`、`GET|PUT|DELETE /api/plans/{id}`、`POST /api/plans/{id}/apply`、`GET /api/plans/{id}/export`、`POST /api/plans/import`，访问口令设置 / 轮换 / 清除 `PUT /api/auth/token`，以及状态 `GET /api/status`、`GET /api/episodes?task=任务名`、`GET /api/logs?offset=N`、`POST /api/run` 启动下载、安全暂停 `POST /api/stop` 和无需口令的 `GET /healthz`。`GET /api/config` 返回设置 schema、分组、任务 / 标签、磁盘、配置路径和运行状态；Secret Key / PostgreSQL 密码始终返回空串，并通过 `secrets_set` 标记是否已保存。`/api/run` 接受 `mode=count` 或 `mode=duration`，支持 `max_rounds`（1–50）和 `recheck_missing`；命令行 `--max-rounds` 未指定时使用配置库 `run.default_rounds`，`--web-host` / `--web-port` 可覆盖配置库 `web.host` / `web.port`；Web 会以独立进程组启动下载器，关闭 Web 不会连带终止下载。暂停请求必须使用 `Content-Type: application/json`，请求体 `{}`；Web 支持安全暂停与强制结束：安全暂停保留临时文件供续传；强制结束会终止下载器进程树，当前文件的临时数据保留，下次运行按 Range 续传。状态接口返回任务标签、历史完成进度、磁盘空间/挂载状态、磁盘任务分配、运行中的 episode、汇总下载字节、近似速度、ETA、进度百分比和上次运行结果。速度基于两次轮询间暂存目录字节增量估算，并非网卡级精确吞吐。日志接口按 offset 增量读取完整行。Web API 默认只监听 `127.0.0.1:8765`；未设置本 Go 控制台访问口令时拒绝非本机监听。SQLite 中配置了 PBKDF2 口令后，API 请求必须使用 `Authorization: Bearer <口令>`；5 分钟内连续 8 次错误口令会锁定该来源 60 秒。写请求必须使用 `Content-Type: application/json`，并校验同源 `Origin`。

## 当前限制

目前 Go Web 提供完整的首次配置页面、状态与日志页面、配置查询与保存、图形化磁盘配置及挂载点检测、MinIO / PostgreSQL 连接测试、任务目标时长设置、任务方案页面（保存、覆盖、替换 / 追加载入、导出）及访问口令设置 / 轮换 / 清除。`PUT /api/auth/token` 使用 `{ "action":"set", "token":"至少 12 字节的新口令" }` 设置或轮换口令，使用 `{ "action":"clear" }` 清除；口令只以 PBKDF2-SHA256 摘要保存，不返回明文。该接口仍受当前 API 鉴权保护：已设置口令时必须先携带旧口令完成认证；未设置口令时仅允许本机访问。清除后，非本机监听的既有服务会拒绝远程 API 请求；重启时若仍无口令则禁止绑定非本机地址。磁盘检查目前覆盖挂载状态、路径包含关系、可写性与可用空间；未挂载时不会向其上级系统目录写探测文件。

- 磁盘间同一文件系统检测：Linux 通过内核设备号识别，能够识别 bind mount / 设备别名；Windows 使用系统分区表中的卷设备标识，拒绝重复挂载同一卷。
- 任务方案的高级差异预览和冲突逐项交互处理。
- 完整 Python 状态库的所有历史兼容、精确实时速度 / ETA 汇总。
- 跨主机共享文件系统的磁盘协调、完整崩溃恢复与真实 MinIO / PostgreSQL 集成测试。

新部署的 Go 版本将配置、下载状态、运行控制、进程状态和日志统一保存在自身 SQLite 文件中，不生成 `pid_file` 或 `log_dir`。旧版 Python 配置库导入只通过显式的旧版兼容配置入口使用，不是默认运行链路。生产使用前应先在测试 bucket 验证。


Windows 文件发布：替换已有目标文件时先将旧文件改名为同目录备份，再发布新文件；发布失败时回滚旧文件，并对 Windows sharing violation / lock violation 最多重试 6 次。


Windows 文件发布与恢复：单文件和 Episode 目录替换使用同目录备份、发布、失败回滚，并对 Windows sharing violation / lock violation 最多重试 6 次；Episode 正式目录更新也先备份旧目录，发布失败时恢复旧目录，避免先删除旧数据再发布新数据造成数据丢失。

Episode 目录发布具备启动时崩溃恢复：备份目录按 episode 正式目录名隔离；若进程在旧目录移走后异常退出，下次处理该 episode 时会恢复最近的普通目录备份并保留 `.partial` 下载目录；若新正式目录已发布，则清理遗留备份。符号链接备份不会被自动跟随。

Episode 备份路径使用随机且尚不存在的同级路径，不创建临时空目录占位，避免进程在占位目录创建后崩溃时被误恢复为空 Episode；恢复扫描会清理遗留空备份目录，只恢复含有内容的普通目录。

同一主机上的下载进程通过 SQLite `process_state` 租约做单实例协调；租约记录异常残留时，会通过进程 PID 和启动时间识别并清除过期记录，不需要磁盘锁文件。

SQLite 完成状态校验：默认不再仅凭正式目录存在就认定 Episode 已完成；必须是非符号链接的普通目录，且包含普通文件 `task_info.json`。启用 `require_mcap` 时，还必须存在匹配 `mcap_pattern` 的普通文件。`trust_completed_records=true` 仍保留历史归档记录信任行为。

SQLite 异常退出恢复：每次新一轮下载启动时，在选取任务前扫描遗留 `running` 记录。正式目录已满足完成文件要求的，恢复为 `completed`；否则重置为 `pending`，记录中断原因并保留 `partial_path`，供后续续传。该操作使用 SQLite 事务，重复执行安全，不删除临时文件。

缺失 MCAP 清理具备异常中断恢复：先在 SQLite 事务中将记录标记为 `cleanup_pending` 并移除完成台账，再删除 Episode 目录，最后删除清理状态记录。正常启动时会在验证所有配置磁盘挂载后恢复遗留清理项；清理目标必须位于配置磁盘目录内。`--cleanup-missing-mcap --dry-run` 只预览待清理和待恢复记录，不修改文件或数据库。
