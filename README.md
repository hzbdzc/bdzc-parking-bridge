# 博达智创停车桥接程序

接收博达智创停车系统中海康威视终端上报的过车消息，解析后转换为大园区停车系统 API 请求并发送。业务方向按现场要求反转：我方进场发送为对方出场，我方出场发送为对方进场。

## 功能简介

- HTTP server 固定监听所有网卡，端口和接收路径由 `config.json` 控制。
- 支持海康 multipart/JSON 过车消息解析，保存原始请求、原始 JSON、图片、大园区请求和发送结果。
- 只自动发送符合条件的停车场出入口事件：`active`、方向为 `enter/exit`、`passingType` 为 `plateRecognition/stop/manual`、车牌有效、过车时间未超过配置的过旧阈值。
- 发送失败后在当前 service worker 内按 `1 / 5 / 10` 秒等待重试；第 4 次仍失败则写入 `dead_letter`。
- 当前事件状态为 `pending / sending / sent / dead_letter / skipped / parse_error`。
- 不需要自动发送的记录会保存为 `skipped`，并保留跳过原因；有 payload 的记录可在 GUI 里手动重发。
- HTTP server 由父进程托管 Uvicorn 子进程，提供 `/livez`、`/status` 和图片访问；父进程退出后 HTTP 子进程会自动退出。
- service 层使用 3 个统一 worker 处理入站消息、发送、手动重发和清理任务；旧数据每小时自动清理，也可在 GUI 配置窗口手动触发。
- SQLite 使用 WAL 和 busy timeout，数据库写入由 storage 层串行化。
- GUI 支持开始/停止 HTTP server、查看记录详情、配置导入导出、模拟发送、手动重发、查看日志和清理旧数据。

## 运行方法

安装依赖：

```powershell
uv sync
```

启动 GUI：

```powershell
uv run bdzc_parking
```

也可以使用模块入口：

```powershell
uv run python -m bdzc_parking
```

Windows 现场可双击项目根目录的 [start_bdzc_parking.bat](start_bdzc_parking.bat)。该脚本会使用项目内 `config.json`，同步运行依赖，并启动无控制台窗口的 GUI。

启动后在 GUI 顶部点击 `HTTP server` 开始监听；也可以把 `auto_start_server` 配置为 `true`，让程序启动时自动开启。

## 配置

默认配置文件是项目根目录的 `config.json`，也可通过环境变量 `HKPARKING_CONFIG` 指定。

常用配置项：

- `listen_port`、`listen_path`、`auto_start_server`：海康终端上报地址。
- `partner_api_url`、`park_id`：大园区 API 地址和停车场 ID。
- `local_entry_hobby/cid/cname`、`local_exit_hobby/cid/cname`：我方入口、出口映射到大园区 API 的通道信息。
- `default_phone`：大园区 payload 默认手机号。
- `external_url_base`：图片外部访问 URL 前缀；为空时 payload 中只保存图片文件名。
- `request_timeout_seconds`：发送大园区 API 的单次请求超时。
- `max_event_age_seconds`：过车时间相对接收时间超过该秒数时跳过自动发送。
- `db_path`、`log_path`：SQLite 数据库和日志文件路径。

日志默认写入 `logs/bdzc_parking.log`，超过 10MB 自动轮转，保留最近 5 个历史文件。

## 运维接口

- `GET /livez`：只检查 HTTP 子进程本身是否存活。
- `GET /status`：返回 HTTP 生命周期、service worker、队列、数据库健康、最近成功发送时间、死信积压和数据库大小。
- 图片访问路径由 `external_url_base` 的 path 部分决定，例如 `https://example.com/parking-images` 对应 `/parking-images/<文件名>`。

## 开发和测试

安装开发依赖：

```powershell
uv sync --dev
```

运行测试：

```powershell
uv run pytest
```

语法编译检查：

```powershell
uv run python -m compileall -q src tests
```

历史 raw request 文件按日期整理：

```powershell
uv run python -m bdzc_parking.maintenance migrate-raw-requests
```

## 海康终端配置

在海康出入口终端 Web 管理界面中配置远程 HTTP 主机：

- 平台接入方式：HTTP 主机
- 地址：运行本程序的主机 IP
- 端口：`listen_port`，默认 `1888`
- URL：`listen_path`，默认 `/park`

如果大园区需要访问过车图片，需要配置 `external_url_base`，并确保该地址能访问到本程序的 HTTP server 或前置反向代理。

## 源码说明

| 文件 | 说明 |
| --- | --- |
| [src/bdzc_parking/app.py](src/bdzc_parking/app.py) | 程序组装入口：加载配置、初始化日志、创建数据库、service、HTTP server 和 GUI。 |
| [src/bdzc_parking/config.py](src/bdzc_parking/config.py) | 配置模型、默认值、JSON 读写、类型转换和校验。 |
| [src/bdzc_parking/common.py](src/bdzc_parking/common.py) | 跨模块复用的纯工具函数。 |
| [src/bdzc_parking/gui.py](src/bdzc_parking/gui.py) | PySide6 图形界面，负责展示、配置、模拟发送、手动重发和操作入口。 |
| [src/bdzc_parking/http_server.py](src/bdzc_parking/http_server.py) | 入站 HTTP 层，托管 Uvicorn 子进程、接收海康消息、提供探针和图片访问。 |
| [src/bdzc_parking/service.py](src/bdzc_parking/service.py) | 核心业务编排和大园区 API 客户端：消费入站队列、解析、过滤、入库、发送、重试、死信和清理。 |
| [src/bdzc_parking/storage.py](src/bdzc_parking/storage.py) | SQLite 持久化边界，负责事件、附件、状态、查询、健康探针和数据清理。 |
| [src/bdzc_parking/parser.py](src/bdzc_parking/parser.py) | 海康 multipart/JSON 请求解析和标准事件提取。 |
| [src/bdzc_parking/models.py](src/bdzc_parking/models.py) | 共享数据结构、过滤规则和大园区 payload 映射逻辑。 |
| [src/bdzc_parking/maintenance.py](src/bdzc_parking/maintenance.py) | 一次性维护命令入口。 |
| [src/bdzc_parking/__main__.py](src/bdzc_parking/__main__.py) | `python -m bdzc_parking` 入口。 |

参考资料在 `references/`，海康样本消息在 `references/hik_events/`。
