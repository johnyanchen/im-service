# IM Service

仿微信风格的即时通讯系统，基于 Go 微服务架构，支持单聊、群聊、好友管理，使用 Kafka 做消息扇出、WebSocket 实时推送。

## 架构

```
        ┌─────────────────────────────────────────────────────────────┐
        │                      Browser (React)                        │
        └─────────────────────┬───────────────────────┬──────────────┘
                         HTTP/REST                 WebSocket
                              │                        │
                              ▼                        ▼
               ┌──────────────────────┐   ┌────────────────────────┐   ┌──────────────┐
               │      API Gateway     │   │        Gateway         │──►│    Redis     │
               │  解析 token          │   │  上线: SetRoute         │   │  用户路由表  │
               │  转发 gRPC 请求      │   │  下线: DelRoute         │◄──│              │
               └──────────┬───────────┘   │  心跳: 刷新路由 TTL     │   └──────────────┘
                     gRPC │               └────────────────────────┘          ▲
                          ▼                        ▲                          │ 查路由
┌──────────────┐  ┌──────────────────────┐         │ gRPC Push               │
│  PostgreSQL  │  │        Logic         │  ┌──────────────────────┐         │
│  messages    │◄─│  鉴权/消息/好友/会话  │  │        Fanout        │─────────┘
│  convs ...   │  │  发布消息事件        │  │  消费 Kafka 事件      │
└──────────────┘  └──────────┬───────────┘  │  写 user_conversations│
                       发布  │              └──────────▲────────────┘
                             ▼                     消费│
                    ┌──────────────────────────────────┐
                    │             Kafka                │
                    │         (message_fanout)         │
                    └──────────────────────────────────┘
```

### 服务说明

| 服务 | 端口 | 职责 |
|------|------|------|
| **apigateway** | :8080 | 无状态 HTTP API 网关，解析 token、转发 gRPC 请求，服务前端静态资源 |
| **gateway** | :8081 (WS) / :9001 (gRPC) | 维护 WebSocket 长连接，接收 fanout 推送并下发给客户端 |
| **logic** | :9002 | 核心业务逻辑（消息、会话、好友），gRPC 服务 |
| **fanout** | — | Kafka 消费者，将消息事件扇出推送给所有在线成员 |

### 技术栈

- **后端**：Go 1.25，gRPC，PostgreSQL 16，Redis 7，Kafka 3.7
- **前端**：React 19，Vite，Tailwind CSS
- **消息传递**：Kafka topic `message_fanout`
- **路由**：用户在线路由存 Redis，gateway 地址作为 key

## 功能

- 注册 / 登录（JWT）
- 好友管理：邀请码添加好友、处理申请、删除好友
- 单聊：好友间才能发消息；删好友后旧历史不可见，重加好友后从新消息开始
- 群聊：创建群、邀请好友加入
- 实时推送：WebSocket 推送新消息、好友事件
- 未读红点：per-conversation 消息水位去重计数
- 多端互踢：同账号新设备登录，旧连接收到 4001 下线

## 快速开始

### 依赖

- Docker & Docker Compose
- Go 1.21+
- Node.js 18+

### 启动

```bash
# 1. 启动基础设施（PostgreSQL、Redis、Kafka）
docker compose up -d

# 2. 一键启动所有服务 + 前端
bash dev.sh
```

前端默认跑在 http://localhost:3000

停止所有服务：

```bash
bash dev.sh stop
```

### 手动启动（分开跑）

```bash
# 基础设施
docker compose up -d

# 各服务（分别开终端）
go run ./logic
go run ./gateway
go run ./fanout
go run ./apigateway

# 前端
cd frontend && npm install && npm run dev
```

### Migration

migration 文件在 `migrations/` 目录，`dev.sh` 启动时会自动幂等执行。手动跑：

```bash
psql "postgres://im:im@localhost:5432/im?sslmode=disable" -f migrations/005_min_visible_msg.sql
psql "postgres://im:im@localhost:5432/im?sslmode=disable" -f migrations/006_is_deleted.sql
```

## 项目结构

```
├── apigateway/        # HTTP API 网关
├── gateway/           # WebSocket 网关
├── logic/             # 业务逻辑服务
├── fanout/            # Kafka 消费 & 消息扇出
├── frontend/          # React 前端
├── pkg/
│   ├── model/         # 数据库操作
│   ├── kafka/         # Kafka producer/consumer
│   ├── config/        # 配置读取
│   └── ...
├── proto/             # Protobuf 定义 & 生成代码
├── migrations/        # SQL 迁移文件
├── docker-compose.yml
└── dev.sh             # 一键启动脚本
```

## 设计要点

**单聊会话惰性创建**：点开好友不会预建会话，发第一条消息时才在服务端创建，避免空会话污染对方列表。

**删好友语义**：单向删除（仿微信）。删除方的 `user_conversations` 记录 `is_deleted=true`，`min_visible_msg_id` 推到当前最大消息 id；对方无感，历史保留。重加好友再聊天后，水位之前的历史对删除方不可见。

**未读计数去重**：前端维护 per-conversation 的 `message_id` 水位（`unreadHiRef`），sync 和 WS 推送都通过水位判断是否累加，防止重复计数。

**连接一致性**：用户上线换绑时用分布式锁串行化，防止并发上线导致的竞态与 goroutine 泄漏。
