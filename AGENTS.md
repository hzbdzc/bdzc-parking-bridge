# 博达智创-管委会停车桥接程序
- 项目名称：博达智创-管委会停车桥接程序
- 项目缩写：bdzc_parking
- 参考所有的参考文档，API 文档，网络搜索结果，完成这个项目
- 从我方“博达智创”停车系统（海康威视终端）获取过车数据，转换格式，发送给“大园区”（钱塘智慧城，数字能源港）的停车系统。
    - 我方的进场车辆，转换为对方的出场车辆。反之亦然。
- README.md 包含项目简介，简单的运行/开发/测试方法，源文件的介绍。


# 参考文档
- 参考文档在目录 references
    - 目录  hkapi: 含有海康威视停车终端 ISAPI 的文档信息。
    - 上城博达apiv0.1.csv: 定义了大园区的停车系统api。
- 目录 hik_events: 之前采集到的样本 http 接收消息。


# 代码
- 用 Python 3.14 编写
- 用 uv 管理依赖和环境
- GUI 使用 PySide6，HTTP 发送使用标准库 urllib，数据持久化使用 SQLite
- 记录 log，log 文件超过 10M 自动轮转，保留最近 5 个历史文件
- 每个源文件、每个函数都要有注释。程序的主要或者关键步骤也要写明注释
- 当前通过 `uv run bdzc_parking` 或 `uv run python -m bdzc_parking` 运行；如需单独可拷贝运行包，需要后续增加并维护专门的打包配置

# 需求描述
- HTTP server 固定监听所有网卡地址，接收海康威视停车终端发送来的 multipart/JSON 过车消息，监听端口和路径由配置控制
- 接收到消息后解析过车字段和图片，写入 SQLite 数据库，保留原始 HTTP body、原始 JSON、大园区请求 JSON、发送状态、发送次数、下次重试时间、返回信息和错误信息
- 只自动发送符合条件的过车信息：停车场出入口事件、active 状态、方向为 enter/exit、passingType 为 plateRecognition / stop / manual, 车牌有效，且过车时间相对接收时间没有超过配置的过旧跳过秒数
- 对需要发送的记录转换为大园区停车系统 API payload，并按业务要求反转方向：我方进场转换为对方出场，我方出场转换为对方进场
- 自动发送采用持久化状态机：`pending`、`sending`、`failed_retryable`、`dead_letter`、`sent`、`skipped`、`parse_error`
- 发送失败后固定在 1、5、10 秒后重试；第 4 次实际发送仍失败时，记录错误并转为 `dead_letter`，不再自动补发
- 对不需要发送的记录标记为“已跳过”，并保存跳过原因；列表中跳过记录优先显示跳过原因
- 提供最小运维 HTTP JSON 接口：`GET /status`；同时保留现有 `GET /` 纯文本存活响应
- 配置文件 `config.json` 可定义监听端口、接收路径、是否自动开启 HTTP server、大园区 API 地址、停车场 ID、出入口 hobby/cid/cname、默认手机号、请求超时、过旧跳过秒数、数据库路径和日志路径
- Qt GUI 提供主要功能：
    - 开始、停止 HTTP server，打开配置页、模拟发送页、帮助页
    - 过车列表显示数据库 ID、过车时间、车牌、方向、通道、类型、状态/次数/原因、返回信息；隐藏表格控件自身的行号栏
    - 选中列表记录后，在右侧详情面板显示完整字段、跳过原因、接收 HTTP、发送给大园区 API 的请求原文、返回内容、过车图片、首次发送时间、最后尝试时间、下次重试时间和死信时间
    - 详情页可查看大图，并对有大园区 payload 的记录执行手动重发
    - 配置页可修改基本信息和大园区 API 地址等配置，保存到 `config.json`
    - 模拟发送页可手动填写过车信息和 API 地址，发送给大园区 API 并查看返回结果或错误


# 架构描述
- `app.py` 是程序组装入口，负责加载配置、初始化日志、创建 `EventStore`、`PartnerClient`、`ParkingBridgeService`、`BridgeHTTPServer`，并把这些对象交给 GUI。业务逻辑不应写在 `app.py`。
- `common.py` 只放跨模块复用的纯工具函数，例如时间字符串、JSON 展示、文件名、路径和图片 part 判断。`common.py` 不应依赖 GUI、HTTP server、service、storage 或配置对象。
- `config.py` 只负责配置模型、默认值、加载、保存和类型转换。其他模块读取 `AppConfig`，不要在业务模块里直接解析 `config.json`。
- GUI 层在 `gui.py`，是独立的用户界面层。GUI 负责展示数据、响应用户操作、调用 HTTP server 的启动/停止、调用 service 的手动重发和查询 store 数据；GUI 不负责解析海康消息、不直接执行自动发送状态机、不直接拼装大园区 payload。
- HTTP server 层在 `http_server.py`，是独立的入站 HTTP 层。它基于 Uvicorn/Starlette 接收海康终端请求、提供 `/`、`/status` 和图片访问，做请求大小/路径/header 限制和生命周期状态记录。HTTP server 不解析业务字段、不写发送状态机；收到合法上报后只把原始请求交给 `ParkingBridgeService.enqueue_http_request()`。
- service 层在 `service.py`，是核心业务编排层。它负责消费 HTTP 入站队列、调用 parser 解析海康消息、调用 models 判断是否发送和生成大园区 payload、调用 storage 入库、调度后台发送 worker、执行 retry/dead-letter 状态流转，并通知 GUI 刷新。
- parser 层在 `parser.py`，只负责把海康 multipart/JSON 原始请求解析为标准化事件输入。parser 不访问数据库、不发送 HTTP、不依赖 GUI。
- models 层在 `models.py`，保存跨模块共享的数据结构、筛选规则和大园区 payload 映射逻辑。models 应保持纯业务规则，不做 I/O。
- storage 层在 `storage.py`，是 SQLite 持久化边界。它负责建表/迁移、事件和附件保存、状态更新、查询列表/详情、数据库健康探针和数据清理。其他层不要直接操作 SQLite。
- sender 层在 `sender.py`，是出站 HTTP 客户端边界。它只负责用标准库 `urllib` 向大园区 API 发送一次请求并解释响应为 `SendResult`；重试调度、状态写库和业务日志由 service 层负责。
- maintenance 层在 `maintenance.py`，是独立维护命令入口，复用配置、日志和 storage 的清理/维护能力，不依赖 GUI 和 HTTP server。
- 主要依赖方向是：`app -> gui/http_server/service/storage/sender/config`；`gui -> http_server/service/storage/sender/config/common`；`http_server -> service/config/common`；`service -> parser/models/storage/sender/config/common`；`storage -> models/config/common`；`sender -> models/config`。下层模块不应反向依赖 GUI、HTTP server 或 app。
- 运行时 GUI 和 HTTP server 是并列组件，共享同一个 `ParkingBridgeService` 和 `EventStore`。GUI 可以停止/启动 HTTP server，也可以替换运行中的 store/service；替换时必须让 HTTP server 指向新的 service，避免 HTTP 入站请求写入旧数据库。
- 自动发送是持久化状态机，不依赖 GUI 是否打开。GUI 只是观察和人工操作入口；HTTP server 只是入站入口；真正的状态推进发生在 service 后台 worker 和 storage 状态更新中。
