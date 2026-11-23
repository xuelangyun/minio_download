# MinIO Download - Go 版本

Go 实现与 Python 版本并行维护。当前首版提供 Windows / Linux 命令行下载核心。

## 功能

- PostgreSQL 查询 episode，按当前任务名和历史别名归并。
- 按累计视频时长（小时）或 episode 条数选择下载。
- 质量状态过滤、创建时间优先、episode_id / parent_key 去重。
- 使用 MinIO Go SDK，不调用 mc。
- 检查 task_info.json 与可选的 *.SYNC.mcap。
- 多磁盘任务固定分配；每盘任务顺序调度，任务内并发下载。
- SQLite WAL 状态库与下载台账。
- 暂存目录下载完成后原子 rename 发布。
- 任务级磁盘空间预检，默认预留 3 GiB，按 1.15 安全系数估算。

## 配置和运行

复制 config.example.json 为 config.json，填写 MinIO、PostgreSQL、磁盘路径和任务目标。不要提交真实密钥。target_hours 按小时配置，content_time 按秒累计；target_count 按条数配置。

需要 Go 1.23+：

    cd go
    go mod tidy
    go build -o minio-download .
    cp config.example.json config.json
    ./minio-download --config config.json --mode duration
    ./minio-download --config config.json --mode count

Windows PowerShell：

    Copy-Item config.example.json config.json
    .\minio-download.exe --config config.json --mode count

## 当前首版限制

暂存目录失败时保留，但文件级 Range 断点续传、ETag/MD5 校验、Python Web 控制台、配置 SQLite、任务方案/标签、Web 暂停控制尚未移植。生产使用前应先在测试 bucket 验证。
