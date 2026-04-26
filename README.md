# 博达智创停车桥接程序

本项目用于接收海康威视停车终端上报的过车消息，转换为大园区停车系统 API 所需格式后发送。业务方向会反转：我方入口进场发送为大园区 `out`，我方出口出场发送为大园区 `in`。

## 功能简介

- 内置 HTTP server，接收海康停车终端上报的过车消息。
- 解析海康 multipart/JSON 报文，提取过车字段和图片。
- 按规则过滤自动发送事件，支持车牌识别和停车触发两类有效事件，跳过无车牌、无效事件类型、过车时间过旧等不需要自动发送的记录。
- 对于车牌有效且方向可映射的记录，预生成大园区 API payload；即使默认跳过自动发送，仍可在详情里手动发送。
- 自动发送采用持久化状态机 `pending / sending / failed_retryable / dead_letter / sent / skipped / parse_error`。
- 自动发送失败后固定在 `1s / 5s / 10s` 后重试；第 4 次实际发送仍失败时转为 `dead_letter`，不再自动补发。
- 支持配置 `external_url_base`，把 `img` 拼成外部可访问的图片 URL，并由内置 HTTP server 提供图片下载。
- 图片下载按 IP 做令牌桶限流，避免外部反复刷图拖垮程序。
- 发送链路使用固定后台 worker 和有界队列，避免下游 API 异常时线程数无限增长。
- 提供最小运维接口：`GET /healthz` 返回进程和 SQLite 健康状态，`GET /status` 返回失败堆积、队列长度、最近成功发送时间和数据库大小。
- SQLite 启用 WAL / busy timeout，并按保留期清理过期事件、图片和原始报文。
- 使用 Qt GUI 控制 HTTP server、查看过车列表和详情、载入/导出配置、查看日志、执行模拟发送测试。

## 运行方法

先安装依赖：

```powershell
uv sync
```

启动 GUI：

```powershell
uv run bdzc_parking
```

也可以用模块方式启动：

```powershell
uv run python -m bdzc_parking
```

Windows 下也可以直接双击项目根目录的 [start_bdzc_parking.bat](start_bdzc_parking.bat) 启动。

如果需要把历史 `data/raw_requests` 根目录文件整理到按日期划分的子目录，可执行：

```powershell
.\migrate_raw_requests_by_date.bat
```

默认启动后在 GUI 中点击顶部 `HTTP server` 按钮开始监听，再让海康终端向本机监听地址上报消息；也可以在配置中启用 `auto_start_server`，让程序启动时自动开启 HTTP server。

GUI 关闭时会弹确认框。如果此时 HTTP server 正在运行，提示会明确说明退出将停止海康上报接收和外部图片访问。

## 运维接口

- `GET /`：纯文本存活响应，保持兼容旧探针和人工检查方式。
- `GET /healthz`：JSON 健康检查；只按进程可响应且 SQLite 可用返回成功。
- `GET /status`：JSON 状态接口；返回失败堆积数、队列长度、最近一次成功发送时间和数据库大小。

## 配置说明

配置文件 `config.json` 支持以下关键项：

- `listen_port`、`listen_path`、`auto_start_server`
- `partner_api_url`、`park_id`
- `local_exit_hobby`、`local_exit_cid`、`local_exit_cname`：博达出口通道；车辆出博达园区、进入上园路时默认发送 `hobby: "in"`
- `local_entry_hobby`、`local_entry_cid`、`local_entry_cname`：博达入口通道；车辆出上园路、进入博达园区时默认发送 `hobby: "out"`
- `default_phone`、`external_url_base`
- `request_timeout_seconds`、`max_event_age_seconds`
- `max_request_bytes`、`request_read_timeout_seconds`
- `sender_worker_count`、`sender_queue_size`、`stale_sending_seconds`
- `event_retention_days`、`artifact_retention_days`
- `image_rate_limit_per_minute`、`image_rate_limit_burst`
- `db_path`、`log_path`、`event_table_column_widths`

其中：

- `external_url_base` 为空时，`img` 保持图片文件名；非空时会变成完整 URL，例如 `https://host/parking-images/<文件名>`。
- `external_url_base` 必须带明确 path 前缀，程序会把这个 path 前缀作为图片下载路由，例如 `https://host/parking-images` 对应 `/parking-images/<文件名>`。
- 自动发送重试策略固定为 `1s / 5s / 10s`，不再由配置文件控制。
- `image_rate_limit_per_minute` 和 `image_rate_limit_burst` 只作用于图片 GET，不影响海康 `POST/PUT` 上报。

GUI 配置窗口支持直接编辑当前活动配置、载入外部配置文件并立即应用、把当前配置导出到指定文件。鼠标悬浮每个配置项时会显示用途和生效说明。

## 开发方法

本项目使用 Python 3.14 和 uv 管理依赖。常用开发命令：

```powershell
uv sync --dev
uv run python -m bdzc_parking
uv run python -m bdzc_parking.maintenance migrate-raw-requests
```

核心业务入口在 [src/bdzc_parking/service.py](src/bdzc_parking/service.py)：HTTP server 收到消息后，服务层负责解析、过滤、入库、通知 GUI，并按持久化状态机调度待发送记录、重试记录和死信记录。

## 测试方法

运行全部测试：

```powershell
uv run python -m pytest
```

语法编译检查：

```powershell
uv run python -m compileall -q src tests
```

## 海康威视停车，出入口终端主机配置

程序的 HTTP server 接收从海康威视出入口终端发送的 HTTP 消息。需要配置主机：

- 登录出入口终端 web 界面
- 参数配置 - 平台接入 - 远程主机，再点击“参数配置”按钮
  - 平台接入方式：http 主机
  - URL：`/park`
  - 填入程序运行主机的 IP 地址和程序端口（默认 `1888`）
  - 按需填入其他选项
  - 点击保存

如果要让外部系统访问保存的过车图片，需要再把 `external_url_base` 指向本程序可访问的地址；如果前面有反向代理，需要把相同 path 前缀转发到本程序。

## 主程序源码

| 文件 | 说明 |
| --- | --- |
| [src/bdzc_parking/__init__.py](src/bdzc_parking/__init__.py) | 包版本和导出信息 |
| [src/bdzc_parking/__main__.py](src/bdzc_parking/__main__.py) | `python -m bdzc_parking` 的模块入口 |
| [src/bdzc_parking/app.py](src/bdzc_parking/app.py) | 组装配置、日志配置、数据库、发送客户端、业务服务、HTTP server 和 GUI |
| [src/bdzc_parking/config.py](src/bdzc_parking/config.py) | 应用配置默认值、JSON 配置文件读取、字段类型转换和保存 |
| [src/bdzc_parking/gui.py](src/bdzc_parking/gui.py) | PySide6 GUI，负责 HTTP server 控制、过车列表、右侧详情面板、配置弹窗、模拟发送测试和手动发送 |
| [src/bdzc_parking/http_server.py](src/bdzc_parking/http_server.py) | 海康消息接收 HTTP server、`/healthz` / `/status` 运维接口、图片访问路由和请求限流 |
| [src/bdzc_parking/models.py](src/bdzc_parking/models.py) | `HikEvent`、`HikEventImage`、`SendResult` 等跨模块数据模型，以及事件过滤和海康到大园区 payload 的方向反转映射 |
| [src/bdzc_parking/maintenance.py](src/bdzc_parking/maintenance.py) | 一次性维护命令入口，负责执行历史 `raw_requests` 文件按日期整理等工具动作 |
| [src/bdzc_parking/parser.py](src/bdzc_parking/parser.py) | 海康 multipart/JSON 解析、过车字段提取和图片 part 提取 |
| [src/bdzc_parking/sender.py](src/bdzc_parking/sender.py) | 大园区 API HTTP 客户端和响应解释；生产发送使用单次请求，重试由服务层状态机调度 |
| [src/bdzc_parking/service.py](src/bdzc_parking/service.py) | 核心桥接流程：解析、过滤、入库、状态机发送、固定 1/5/10 秒补捞重试、手动发送和清理维护 |
| [src/bdzc_parking/storage.py](src/bdzc_parking/storage.py) | SQLite 事件表、去重、图片保存、发送状态机元数据、保留期清理、运维统计和 payload 读取 |

## 测试源码

| 文件 | 说明 |
| --- | --- |
| [tests/test_config.py](tests/test_config.py) | 配置文件加载、导入导出、URL 校验和字段类型转换测试 |
| [tests/test_http_server.py](tests/test_http_server.py) | 图片下载路由、限流、请求体大小保护和 `/healthz`、`/status` 接口测试 |
| [tests/test_maintenance.py](tests/test_maintenance.py) | 维护命令入口和历史 `raw_requests` 文件整理输出测试 |
| [tests/test_mapper.py](tests/test_mapper.py) | 过车事件过滤和方向反转映射测试 |
| [tests/test_parser.py](tests/test_parser.py) | 海康消息样本解析、关键字段提取和图片提取测试 |
| [tests/test_sender.py](tests/test_sender.py) | 大园区 API 发送、响应解释和重试逻辑测试 |
| [tests/test_service.py](tests/test_service.py) | skipped 记录生成 payload、固定 1/5/10 秒状态机重试、死信和动态图片 URL 测试 |
| [tests/test_storage.py](tests/test_storage.py) | 事件入库、图片保存、payload 图片引用、旧库迁移和列表轻量读取测试 |
