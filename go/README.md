# MinIO Download - Go 版本

Go 实现与 Python 版本并行维护。当前首版提供 Windows / Linux 命令行下载核心。

## 功能

- PostgreSQL 查询 episode，按当前任务名和历史别名归并。
- 按累计视频时长（小时）或 episode 条数选择下载。
- 质量状态过滤、创建时间优先、episode_id / parent_key 去重。
- 使用 MinIO Go SDK，不调用 mc。
- 检查 task_info.json 与可选的 *.SYNC.mcap。
- 多磁盘任务固定分配；每盘任务顺序调度，每盘并发 episode 数可配；每条 episode 内文件并行下载（默认 4）。
- SQLite WAL 状态库与下载台账。
- 暂存目录下载完成后原子 rename 发布；启动时清理不属于当前对象清单的残留文件，保留有效 Range 临时文件。
- 跨平台单实例文件锁；文件与发布目录执行 fsync（Windows 不支持时安全忽略）。
- 任务级磁盘空间预检，默认预留 3 GiB，按 1.15 安全系数估算。文件并发参数 `file_workers` 默认 4，与 `max_writes_per_disk` 分开控制。

## 配置和运行

Go 版本按 Python 的两个 py_shell 脚本拆分了任务配置模板：
- `config.example.json`：按时长任务清单，对应 `minio_task_downloader.py`。
- `config.count.example.json`：按条数任务清单，对应 `minio_task_downloader_by_count.py`（43 个任务，默认每个任务 40 条）。

将所需模板复制为 `config.json`，填写 MinIO、PostgreSQL、磁盘路径。不要提交真实密钥。时长模式的 `target_hours` 单位为小时，数据库 `content_time` 按秒累计；条数模式使用 `target_count`。两种模式必须使用各自对应的任务配置，避免历史别名和任务目录发生冲突。按条数模板默认启用 `trust_completed_records`，兼容 py_shell 更换/归档磁盘后仍按历史台账计数；按时长模板默认要求正式目录仍存在，避免把本地已删除的数据计入目标。

需要 Go 1.23+：

    cd go
    go mod tidy
    go build -o minio-download .
    cp config.example.json config.json
    ./minio-download --config config.json --mode duration

    # 条数模式：使用独立的 43 任务配置
    cp config.count.example.json config.json
    ./minio-download --config config.json --mode count

Windows PowerShell：

    Copy-Item config.example.json config.json
    .\minio-download.exe --config config.json --mode count

## 当前首版限制

已实现文件级 Range 续传、ETag 条件请求、标准 MD5 ETag 校验、下载后远端元数据复核和 Windows 对象文件名检查。仍未移植 Python Web 控制台、配置 SQLite、任务方案/标签、Web 发起的跨进程暂停控制、PID/单实例锁、磁盘预留锁、完整崩溃恢复与 Python 配置库兼容层。命令行 Ctrl+C 可取消当前 context，并将正在下载的 episode 标记为 `paused`，保留 `.minio-tmp` 供下次 Range 续传。Go 使用自己的 SQLite 状态表，不会直接复用 Python 状态库。生产使用前应先在测试 bucket 验证。
