# HTTP 接口参考

> 适用版本：2.2.x。配套：[开发文档](DEVELOPMENT.md)（§4.9 解释鉴权与进程控制的实现）· [数据与配置参考](DATA.md)（JSON 格式、设置项）。
> 示例响应由真实接口生成并裁剪（列表只保留第一项）。

## 目录

* [通用约定](#通用约定)
* [运行状态](#运行状态) — `GET /api/status` · `GET /api/episodes` · `GET /api/logs` · `GET /healthz`
* [运行控制](#运行控制) — `POST /api/run` · `POST /api/stop`
* [配置](#配置) — `GET /api/config` · `PUT /api/config/tasks` · `POST /api/config/tasks/reset` · `PUT /api/config/disks` · `POST /api/config/disks/check` · `PUT /api/config/settings` · `POST /api/config/test` · `PUT|DELETE /api/config/token`
* [任务方案](#任务方案) — `GET|POST /api/plans` · `GET|PUT|DELETE /api/plans/{id}` · `POST /api/plans/{id}/apply` · `GET /api/plans/{id}/export` · `POST /api/plans/import`

---

## 通用约定

### 地址与格式

* 默认 `http://127.0.0.1:8765`。请求与响应均为 JSON（UTF-8，不转义中文）。页面 `GET /` 返回 `index.html`。
* 时间均为 ISO-8601（UTC，如 `2026-11-21T16:14:19+00:00`）。字节数为整数。

### 鉴权

| 情形 | 规则 |
|---|---|
| **没设访问口令** | 只接受本机直连：`remote_addr` 为 `127.0.0.1` / `::1`，**没有** `X-Forwarded-For` 头，且 `Host` 为 `localhost` / `127.0.0.1` / `::1`；否则 `403` |
| **设了口令** | 所有 `/api/*` 必须带 `Authorization: Bearer <口令>`；缺失或错误 → `401` |
| 失败限流 | 同一远端地址 5 分钟内带了**错误口令** 8 次 → `429`（`Retry-After` 秒），锁定 60 秒，期间连正确口令也拒绝。**没带口令不计入失败** |

`/` 与 `/healthz` 不需要口令。

### 跨站防护

* **写方法（POST / PUT / DELETE）必须 `Content-Type: application/json`**，否则 `415`（即使没有请求体也要带，发 `{}`）。
* 请求带 `Origin` 头时，必须与当前站点同源，否则 `403`。

### 错误格式

所有错误统一为：

```json
{ "error": "中文原因" }
```

可能附带 `code`（如 `"exists"`）或 `problems`（开始前缺少的配置列表）。

| 状态码 | 含义 |
|---|---|
| 400 | 参数 / 内容不合法（`error` 里有具体原因） |
| 401 | 口令缺失或错误 |
| 403 | 未设口令时的非本机访问 / 跨站请求 / 主机名不允许 / 无权限结束进程 |
| 404 | 方案不存在 |
| 409 | 冲突：下载器已在运行 / 没有运行中的下载器 / 同名方案 |
| 415 | 写操作缺少 JSON `Content-Type` |
| 429 | 口令错误次数过多 |

### 响应头

所有响应带 `X-Content-Type-Options: nosniff`、`X-Frame-Options: DENY`、`Referrer-Policy: no-referrer`；`/api/*` 带 `Cache-Control: no-store`；页面带 CSP。

---

## 运行状态

### `GET /api/status`

概览页的全部数据。参数：`?fresh=1` 绕过缓存（进度统计缓存：下载中 4 秒 / 空闲 15 秒；`running_episodes` 与暂停状态永远实时）。

```json
{
  "version": "2.2.0",
  "data_version": "银河2.2数据",
  "downloader": { "running": false, "pid": null, "by_web": false },
  "stop_requested": false,
  "setup_issues": [ { "page": "connection", "text": "尚未配置 MinIO 连接（服务地址 / Access Key / Secret Key）" } ],
  "minio": { "endpoint": "", "bucket": "rocp-data" },
  "settings": { "default_rounds": 1, "mcap_pattern": "*.SYNC.mcap", "min_free_gb": 3.0, "require_mcap": true, "writes_per_disk": 2 },
  "last_run": {},
  "stale_records": 0,
  "running_episodes": [],
  "disks": [ { "name": "disk1", "path": "/mnt/temdata1", "exists": false, "mounted": false, "total": 0, "free": 0, "used": 0, "tasks": [] } ],
  "scenes": [ { "scene": "Education and Entertainment(教育娱乐)",
    "tasks": [ { "name": "积木搭建", "dir": "积木搭建", "target": 40, "aliases": [], "tags": [],
                 "downloaded": 0, "hours": 0.0, "bytes": 0, "issues": {}, "disk": null } ] } ]
}
```

| 字段 | 说明 |
|---|---|
| `downloader` | `running`；`pid`；`by_web` = 是否由本 Web 启动（否则是命令行启动，靠 PID 文件识别） |
| `stop_requested` | 下载器在运行且已收到暂停请求（界面显示“正在暂停…”） |
| `setup_issues` | 开始下载前还缺的配置：`page` ∈ `connection / disks / tasks`。非空时 `POST /api/run` 返回 400 |
| `last_run` | 上次运行结果：`result` ∈ `finished / paused / error`，另有 `at`、`counts`、`mode`、`error`（以及本 Web 启动且异常退出时的 `exit_code`） |
| `stale_records` | 记录标记已完成但本地目录已被删除的条数（不计入进度） |
| `running_episodes` | 下载器运行时才有内容：`episode_id, task_name, disk_name, started_at, expected_bytes, partial_bytes`（暂存目录当前字节数，前端据此算速度）；最多 24 条 |
| `disks[]` | `total / free / used` 字节（读不到为 0）；`mounted` = 挂载点是否真挂载；`tasks` = 已分配到该盘的任务 |
| `scenes[].tasks[]` | `downloaded` 已下载条数（正式目录仍存在才算）；`hours` 已下载时长；`bytes` 已下载大小；`issues` = 未完成原因计数 `{状态: 条数}`（状态见 [DATA §6.1](DATA.md#61-downloadsstatus)）；`disk` 分配的磁盘；`tags` 标签 |

### `GET /api/episodes?task=<任务名>`

某任务下**尚未完成**的 episode 及原因，最多 200 条，按更新时间倒序。

```json
{ "episodes": [ { "episode_id": "e-123", "status": "missing_mcap", "error": "缺少 mcap 文件（*.SYNC.mcap）：s3://…", "parent_key": "p/e-123", "updated_at": "2026-11-21T10:00:00+00:00" } ] }
```

### `GET /api/logs?offset=<N>`

增量读取总日志。省略 `offset`（或 `offset` 超出文件大小 / 为负，例如日志被轮转）= 首次加载，返回最后约 96 KiB，并丢弃被截断的首行。只返回**完整的行**。

```json
{ "text": "…\n", "offset": 51234, "reset": true }
```

下次请求带上返回的 `offset`；`reset=true` 表示前端应清屏重画。

### `GET /healthz`

无需口令，不含配置信息：`{ "ok": true, "version": "2.2.0" }`。

---

## 运行控制

### `POST /api/run`

以子进程启动下载器（独立会话 / 进程组，关闭网页不影响）。

请求：

```json
{ "mode": "download", "max_rounds": 1, "recheck_missing": false }
```

| 字段 | 取值 |
|---|---|
| `mode` | `download`（默认）· `cleanup_dry`（`--cleanup-missing-mcap --dry-run`）· `cleanup`（`--cleanup-missing-mcap`，**会删除本地缺 mcap 的目录**） |
| `max_rounds` | 1–50；缺省取设置 `run.default_rounds` |
| `recheck_missing` | `true` → `--recheck-missing` |

* 成功：`{ "ok": true, "pid": 12345 }`。启动前会清掉遗留的暂停请求。
* `400`：模式未知 / 轮数不合法 / **配置未补全**（响应含 `problems: [{page, text}]`）。
* `409`：`{"error": "下载器已在运行（PID 12345）"}`。

### `POST /api/stop`

请求：`{}`（暂停）或 `{ "force": true }`（强制结束）。

| 请求 | 行为 | 响应 |
|---|---|---|
| `{}` | **暂停**：往状态库写 `stop_requested`；下载器在当前数据块写完后安全停下、保存断点；再次 `POST /api/run` 即续传 | `{ "ok": true, "mode": "pause" }` |
| `{ "force": true }` | 立即结束下载器进程树（数据仍安全，续传时最后一小段需重下） | `{ "ok": true, "mode": "force" }` |

错误：`409` 当前没有运行中的下载器；`403` 没有权限结束该进程（由其它用户启动）。命令行启动的下载器同样适用。

---

## 配置

### `GET /api/config`

页面“任务 / 磁盘 / 连接 / 参数”一次性取回所需的全部配置。

| 字段 | 说明 |
|---|---|
| `schema[]` | 设置项定义（用于自动生成表单）：`key, kind, group, label, help, secret, min, max, placeholder, default`；密钥项的 `default` 为 `null` |
| `groups` | 分组键 → 中文名 |
| `values` | 当前值；**密钥项一律为 `""`** |
| `secrets_set` | 已设置的密钥项键列表（如 `["pg.password"]`） |
| `tasks` | 任务 JSON（[DATA §5.1](DATA.md#51-任务-json接口--方案共用)），含标签 |
| `disks` | `[{name, path, mountpoint}]` |
| `token_set` | 是否已设置访问口令 |
| `paths` | `config_db / state_db / log_dir` 的实际路径 |
| `platform` | `"windows"` 或 `"posix"` |
| `running` | 下载器是否在运行（用于提示“下次启动生效”） |

### `PUT /api/config/tasks`

整体替换任务配置（一个事务，失败则不变）。请求体 = 任务 JSON，`data_version` **不能为空**。

* 成功：`{ "ok": true, "running": false }`。
* `400` 示例：`{"error": "任务名重复: '新任务'（任务名必须全局唯一）"}`、`{"error": "任务 'x' 的目标条数必须是非负整数，当前为 -1"}`、`{"error": "标签太长（最多 24 个字符）：…"}`、`{"error": "版本目录不能为空"}`。

### `POST /api/config/tasks/reset`

恢复为 Go 内置 count 模式默认任务列表（同名任务的标签保留）。`{ "ok": true }`。此接口当前只适用于 Go count 模式。

### `PUT /api/config/disks`

请求：`{ "disks": [ { "name": "disk1", "path": "D:\\data", "mountpoint": "D:\\" } ] }`。`mountpoint` 缺省 = `path`。
`400`：名称 / 路径为空，或名称重复。成功 `{ "ok": true, "running": false }`。

### `POST /api/config/disks/check`

只检查，不修改任何东西。请求 `{ "disks": [...] }`（缺省则检查已保存的）。

```json
{ "results": [ { "name": "d", "ok": false, "free": 10409414656, "total": 270553174016,
                 "notes": [ { "level": "bad", "text": "挂载点不存在：/nope" } ] } ] }
```

检查项：挂载点存在且是真挂载点；路径在挂载点之下（同一盘符）；路径存在（或其最近的已存在上级目录可写）；剩余空间；是否与其它条目在同一文件系统（`level` 为 `ok` / `bad`）。

### `PUT /api/config/settings`

请求：`{ "values": { "run.min_free_gb": 5, "minio.secret_key": null, "pg.password": "新密码" } }`。

* 只写传入的键；**密钥项 `null` = 不修改，`""` = 清除**。未知键 / 类型或范围不对 → `400`（多个错误用 `；` 连接），**整批都不保存**。
* 成功：`{ "ok": true, "saved": ["run.min_free_gb", "pg.password"], "running": false }`。
* 400 示例：`{"error": "端口：不能大于 65535"}`。

### `POST /api/config/test`

测试连接。使用请求里**尚未保存**的表单值覆盖已保存的值；密钥为 `null` 则沿用已保存的。

请求：`{ "target": "minio" | "pg", "values": { …可选 } }`。

* 连接类失败也返回 `200`：`{ "ok": false, "message": "StoreError: 尚未配置 MinIO 服务地址（Web 控制台 →“连接”页）" }`；成功：`{ "ok": true, "message": "连接成功：http://…，bucket=…" }`。
* `pg` 还会检查表 `public.minio_galbot_scene_task_jobs` 是否存在，不存在时 `ok=false`。
* `400`：`target` 不是 `minio` / `pg`。

### `PUT /api/config/token` · `DELETE /api/config/token`

设置 / 取消访问口令。`PUT` 请求 `{ "token": "至少6个字符" }`（`400` 过短）；成功后**旧口令立即失效**，调用方要换用新口令。`DELETE` 后恢复“只允许本机访问”。

---

## 任务方案

方案 = 任务 JSON 的快照，详见 [开发文档 §4.10](DEVELOPMENT.md#410-任务方案)。

### `GET /api/plans`

```json
{
  "current": { "scene_count": 3, "task_count": 43, "total_target": 1720, "tags": [] },
  "plans": [ { "id": 1, "name": "初始配置", "note": "首次启动时导入的任务配置",
               "scene_count": 3, "task_count": 43, "total_target": 1720, "tags": [],
               "created_at": "2026-11-21T16:14:19+00:00", "updated_at": "2026-11-21T16:14:19+00:00" } ]
}
```

按更新时间倒序。`tags` 是方案里出现过的全部标签（保序去重）。`current` 是当前已保存配置的同样摘要。

### `GET /api/plans/{id}`

方案摘要字段 + `payload`（完整任务 JSON）。`404` 方案不存在。

### `POST /api/plans`

把任务配置存成方案。

```json
{ "name": "桌面整理-第一批", "note": "只含桌面类", "payload": { /* 任务 JSON，可省略 */ }, "overwrite": false }
```

* `payload` 缺省 = 当前**已保存**的配置；传入则保存传入的内容（前端传编辑器里未保存的内容）。内容先经完整校验（重名 / 别名冲突等）。
* 成功：`{ "ok": true, "id": 2 }`。
* `409` 同名且 `overwrite` 不为 `true`：`{ "error": "已有同名方案：x", "code": "exists" }`；`overwrite: true` 覆盖内容（不给 `note` 则保留原备注）。
* `400`：名称为空 / 超过 60 字符 / 内容不合法。

### `PUT /api/plans/{id}`

改名 / 改备注 / 覆盖内容。字段均可选：`name`、`note`、`payload`（用它覆盖方案内容）、`from_current: true`（用当前已保存的配置覆盖）。
`409` 新名称与其它方案同名；`404` 不存在；成功 `{ "ok": true }`。

### `DELETE /api/plans/{id}`

只删除方案本身，不影响当前任务配置与已下载数据。`{ "ok": true }` / `404`。

### `POST /api/plans/{id}/apply`

载入方案到**当前任务配置**（立即写入配置库，下次启动下载时生效）。

```json
{ "mode": "merge", "tags": ["桌面"] }
```

| 字段 | 说明 |
|---|---|
| `mode` | `replace`（默认）：整体替换，版本目录取方案的（方案里为空则沿用当前），标签取方案的 · `merge`：只追加当前**没有**的任务（按任务名），已有任务与条数不变，新场景自动创建 |
| `tags` | 只取带有任一这些标签的任务；缺省 / 空 = 全部 |

响应：

```json
{ "ok": true, "mode": "merge", "added": 0, "total_tasks": 43, "running": false,
  "skipped": [ { "name": "积木搭建", "reason": "当前配置里已有同名任务" } ] }
```

`skipped[].reason` 可能是“已有同名任务”，或与现有配置冲突的具体原因（别名 / 目录名冲突等）——冲突的任务只是被跳过，不影响其它任务。`replace` 时 `skipped` 为空。
`400`：`mode` 不合法 / 过滤后没有任务（“方案里没有符合所选标签的任务”）/ 校验失败；`404` 方案不存在。

### `GET /api/plans/{id}/export`

返回 JSON 文件（`Content-Disposition: attachment; filename*=UTF-8''…`）：

```json
{ "format": "minio-downloader-plan", "version": 1, "name": "桌面整理-第一批", "note": "…", "payload": { /* 任务 JSON */ } }
```

### `POST /api/plans/import`

请求体 = 导出的文件内容（或 `{ "plan": <文件内容> }`）；只含 `payload` 的 JSON 也可以。

* 内容不可信，先完整校验；同名**不覆盖**，自动命名为 `名称 (2)`、`名称 (3)`…
* 成功：`{ "ok": true, "id": 5, "name": "桌面整理-第一批 (2)" }`；`400` 缺少 `payload` / 内容不合法。

---

## 调用示例

```bash
# 设置了口令时；没设口令时本机直连可省略 Authorization
H='Authorization: Bearer 你的口令'; J='Content-Type: application/json'

curl -s -H "$H" localhost:8765/api/status | jq '.downloader, .stop_requested'
curl -s -H "$H" -H "$J" -X POST -d '{"max_rounds":3}'  localhost:8765/api/run
curl -s -H "$H" -H "$J" -X POST -d '{}'               localhost:8765/api/stop        # 暂停
curl -s -H "$H" -H "$J" -X POST -d '{"mode":"merge","tags":["桌面"]}' localhost:8765/api/plans/1/apply
curl -s -H "$H" -OJ localhost:8765/api/plans/1/export                                # 下载方案文件
```
