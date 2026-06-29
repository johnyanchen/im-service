# IM 服务技术设计方案

## 1. 项目概述

基于 Go 实现的生产级即时通讯服务，支持单聊和群聊，采用接入层/业务层分离架构，具备水平扩展能力。

**技术栈：** Go 1.22、gRPC、WebSocket、PostgreSQL、Redis、Kafka
**前端：** 原生 HTML/JS（左侧会话列表 + 右侧聊天记录）

---

## 2. 整体架构

```
浏览器
  │ WebSocket
  ▼
Gateway Service（多实例）
  │ 上行：gRPC → Logic Service（负载均衡）
  │ 下行：被 Logic/Fanout gRPC 调用后推给客户端

Logic Service（多实例，无状态）
  │ 写消息 → PostgreSQL
  │ 发事件 → Kafka（1条）
  │ 路由查询 → Redis

Fanout Worker（多实例）
  │ 消费 Kafka
  │ 批量更新 user_conversations → PostgreSQL
  │ 查路由 → Redis
  │ 推送在线成员 → gRPC → Gateway

共享存储：
  PostgreSQL：持久化
  Redis：路由表、群成员缓存
  Kafka：消息扩散队列
```

---

## 3. 服务职责

### Gateway
- 维护 WebSocket 长连接，不碰任何业务逻辑
- **上行**：收到客户端消息帧 → gRPC 转发给任意 Logic 实例
- **下行**：暴露 `Push(user_id, payload)` gRPC 接口，收到后找本地连接推给客户端
- **连接管理**：用户上线写 Redis 路由 `user:{id}:gateway = "gateway-1:9001"`，下线删除

### Logic
- 无状态，处理所有业务逻辑
- 收到消息 → 鉴权 → 写 `messages` 表 → 发1条 Kafka 事件
- 提供上线同步接口：按 `last_sync_at` 返回增量的 `user_conversations` 和消息内容

### Fanout Worker
- 消费 Kafka `message_fanout` topic
- 查 `conversation_members`（Redis 缓存，miss 则查 DB）得到成员列表
- 用 worker pool（并发上限50）并行处理每个成员：
  - 批量 UPDATE `user_conversations`（不管在不在线，都更新；用户上线时依赖它来同步状态）
  - 查 Redis 路由，在线则 gRPC 调对应 Gateway 推送完整消息，离线则跳过推送

---

## 4. 数据存储

### 数据库表

```sql
-- 用户
users(id, username, password_hash, created_at)

-- 会话（单聊/群聊统一）
conversations(id, type, created_at)               -- type: "dm" | "group"
conversation_members(conversation_id, user_id)    -- 单聊2条，群聊N条

-- 群组附加信息
groups(id, conversation_id, name, owner_id)

-- 消息源（不扩散，1条消息1条记录）
messages(id, conversation_id, from_id, content, created_at)

-- 用户维度会话状态（写扩散，每人1条）
user_conversations(
  user_id,
  conversation_id,
  last_msg_id,     -- 最新消息id，用于列表预览和上线同步
  unread_count,    -- 未读红点
  updated_at       -- 会话列表排序依据
)

-- 用户同步状态
user_sessions(user_id, last_sync_at)
```

### Redis

```
user:{id}:gateway       → "gateway-1:9001"     # 路由表，用户在哪台Gateway
conv:{id}:members       → [uid1, uid2, ...]    # 群成员缓存
```

---

## 5. 消息流转

### 单聊（A → B）

```
A 发消息
→ Gateway 收到，gRPC → 任意 Logic 实例
→ Logic：写 messages，发1条 Kafka 事件 {msg_id, conversation_id, type:"dm"}
→ Fanout 消费：
    查 conversation_members 得到 [A, B]
    批量更新 A、B 的 user_conversations
    查 B 的路由 → B 在线 → gRPC → Gateway-N → 推给 B
               → B 离线 → 跳过
```

### 群聊（A → 群G，成员 B/C/D...）

```
A 发群消息
→ Gateway → Logic
→ Logic：写 messages，发1条 Kafka 事件 {msg_id, conversation_id, type:"group"}
→ Fanout 消费：
    查群成员列表（Redis缓存优先）
    worker pool 并行（上限50）：
      批量 UPDATE user_conversations（所有成员）
      查每个在线成员路由 → gRPC → Gateway 推送
```

### 用户上线同步

```
B 建立 WebSocket
→ Gateway 写 Redis 路由
→ 客户端携带 last_sync_at 请求 Logic
→ Logic 返回：
    SELECT * FROM user_conversations WHERE user_id=B AND updated_at > last_sync_at
    + 对应的 messages 内容
→ 更新 user_sessions.last_sync_at
```

---

## 6. 群成员变化处理

退群：从 `conversation_members` 删除记录，Fanout 后续扩散时不再包含该用户，自然收不到新消息。

历史消息权限：查询 `messages` 时校验请求方是否在 `conversation_members` 中，不在则拒绝。

---

## 7. 服务间通信

| 链路 | 协议 |
|---|---|
| 客户端 ↔ Gateway | WebSocket |
| Gateway → Logic（上行） | gRPC |
| Logic/Fanout → Gateway（下行推送） | gRPC |
| Logic → Kafka | Producer |
| Fanout ← Kafka | Consumer |

---

## 8. 目录结构

```
im-service/
├── gateway/        # Gateway 服务
├── logic/          # Logic 服务
├── fanout/         # Fan-out Worker
├── proto/          # gRPC 接口定义（共用）
└── pkg/
    ├── model/      # DB 模型
    └── config/     # 配置
```

---

## 9. 扩展性说明

- **Gateway 扩容**：直接加实例，Redis 路由表自动分散连接
- **Logic 扩容**：无状态，直接加实例，Gateway 负载均衡轮询
- **Fanout 扩容**：Kafka 分区数决定最大并行消费数，加实例即可
- **单聊和群聊统一处理**：Fanout 按 `conversation.type` 区分，逻辑收敛
