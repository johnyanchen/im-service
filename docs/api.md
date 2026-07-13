# IM 服务接口文档

本系统由三个后端服务组成：**Gateway**（HTTP/WS 网关）、**Logic**（业务逻辑，gRPC）、**Fanout**（消息扩散 worker）。前端只与 Gateway 交互；Gateway 通过 gRPC 调用 Logic；Logic 通过 Kafka 投递事件给 Fanout；Fanout 回调 Gateway 的 gRPC `Push` 完成实时推送。
```
Frontend ──HTTP/WS──▶ Gateway ──gRPC──▶ Logic ──Kafka──▶ Fanout ──gRPC(Push)──▶ Gateway ──WS──▶ Frontend
```

---

## 1. Gateway 服务

### 1.1 HTTP REST 接口

- 监听地址：`WS_ADDR`（默认 `localhost:8080`）
- 除 `/api/login`、`/api/register` 外，所有接口都要求在请求头带 `Authorization: Bearer <token>`。
- 响应统一为 JSON，错误时返回 `{"error": "..."}`。

| 方法 | 路径 | 说明 | 鉴权 |
|------|------|------|------|
| POST | `/api/login` | 登录 | 否 |
| POST | `/api/register` | 注册 | 否 |
| GET | `/api/users` | 列出所有用户 | 是 |
| POST | `/api/messages` | 发送消息 | 是 |
| GET | `/api/sync` | 增量同步会话列表 | 是 |
| GET | `/api/conversations/{id}/messages` | 拉取会话历史消息 | 是 |
| POST | `/api/conversations/{id}/read` | 标记已读 | 是 |
| POST | `/api/conversations/dm` | 创建/获取单聊会话 | 是 |
| POST | `/api/conversations/group` | 创建群聊 | 是 |
| GET | `/api/friends` | 好友列表 | 是 |
| GET | `/api/friends/requests` | 好友申请列表 | 是 |
| POST | `/api/friends/request` | 发送好友申请 | 是 |
| POST | `/api/friends/handle` | 处理好友申请 | 是 |
| GET | `/api/invite-code` | 获取自己的邀请码 | 是 |
| POST | `/api/invite-code/refresh` | 刷新邀请码 | 是 |
| POST | `/api/friends/add-by-code` | 通过邀请码加好友 | 是 |

#### 详细定义

**POST `/api/login`**
```json
// 请求
{ "username": "alice", "password": "secret" }
// 响应
{ "token": "<jwt>", "user_id": 1 }
```

**POST `/api/register`**
```json
// 请求
{ "username": "alice", "password": "secret" }
// 响应
{ "user_id": 1, "token": "<jwt>" }
```

**GET `/api/users`**
```json
// 响应
{ "users": [ { "id": 1, "username": "alice" } ] }
```

**POST `/api/messages`**
```json
// 请求
{ "conversation_id": 10, "content": "hello" }
// 响应
{ "message_id": 100, "created_at": 1719800000000 }  // created_at 为毫秒时间戳
```

**GET `/api/sync?last_sync_at=<ms>`**
增量返回自 `last_sync_at`（毫秒）之后有更新的会话。
```json
// 响应
{
  "conversations": [
    {
      "conversation_id": 10,
      "type": "dm",              // dm | group
      "last_msg_id": 100,
      "unread_count": 3,
      "updated_at": 1719800000000,
      "name": "bob",
      "last_msg_content": "hello",
      "last_msg_from": "bob"
    }
  ]
}
```

**GET `/api/conversations/{id}/messages?before_id=<id>&limit=<n>`**
按 `before_id` 向前翻页拉历史，`limit` 默认 30。
```json
// 响应
{
  "messages": [
    {
      "id": 100,
      "conversation_id": 10,
      "from_id": 2,
      "from_username": "bob",
      "content": "hello",
      "created_at": 1719800000000
    }
  ]
}
```

**POST `/api/conversations/{id}/read`**
```json
// 请求
{ "msg_id": 100 }
// 响应
{ "status": "ok" }
```

**POST `/api/conversations/dm`**
若单聊会话已存在则直接返回，否则新建。
```json
// 请求
{ "peer_id": 2 }
// 响应
{ "conversation_id": 10 }
```

**POST `/api/conversations/group`**
```json
// 请求
{ "name": "开发组", "member_ids": [2, 3, 4] }
// 响应
{ "group_id": 5, "conversation_id": 10 }
```

**GET `/api/friends`**
```json
// 响应
{ "friends": [ { "id": 2, "username": "bob" } ] }
```

**GET `/api/friends/requests`**
```json
// 响应
{
  "requests": [
    {
      "id": 1,
      "from_id": 2,
      "from_username": "bob",
      "status": "pending",   // pending | accepted | rejected
      "created_at": 1719800000000
    }
  ]
}
```

**POST `/api/friends/request`**
```json
// 请求
{ "to_id": 2 }
// 响应
{}
```

**POST `/api/friends/handle`**
```json
// 请求
{ "request_id": 1, "accept": true }
// 响应
{}
```

**GET `/api/invite-code`**
```json
// 响应
{ "code": "AB12CD" }
```

**POST `/api/invite-code/refresh`**
```json
// 响应
{ "code": "XY99ZW" }
```

**POST `/api/friends/add-by-code`**
```json
// 请求
{ "code": "AB12CD" }
// 响应
{ "username": "bob" }   // 目标用户名；实际生成一条好友申请
```

### 1.2 WebSocket 接口

- 路径：`GET /ws`（HTTP 升级为 WebSocket）
- 连接后客户端须**首帧**发送鉴权消息：
  ```json
  { "token": "<jwt>" }
  ```
  token 无效则服务端立即关闭连接。同一用户重复登录会踢掉旧连接（跨网关通过 Redis 路由 + gRPC `Kick` 实现）。
- 心跳：服务端每 30s 发 Ping，读超时 60s；客户端需正常回 Pong。
- 服务端下行推送消息类型（由 Fanout 产生）：

  **新消息 `new_message`**
  ```json
  {
    "type": "new_message",
    "message_id": 100,
    "conversation_id": 10,
    "conversation_type": "dm",
    "conversation_name": "bob",
    "from_id": 2,
    "from_username": "bob",
    "content": "hello",
    "created_at": 1719800000000
  }
  ```

  **新会话 `conv_created`**
  ```json
  {
    "type": "conv_created",
    "conversation_id": 10,
    "conversation_type": "group",
    "conversation_name": "开发组",
    "created_at": 1719800000000
  }
  ```

### 1.3 Gateway gRPC 接口（内部）

`service GatewayService`，供 Fanout worker 及其它网关实例调用。

| RPC | 请求 | 响应 | 说明 |
|-----|------|------|------|
| `Push` | `PushRequest{ user_id, payload }` | `PushResponse{}` | 向本机在线用户下发 WS 消息；用户不在线则静默忽略 |
| `Kick` | `KickRequest{ user_id }` | `KickResponse{}` | 断开该用户在本机的连接（用于单点登录踢人）|

---

## 2. Logic 服务（gRPC，内部）

- 监听地址：`LOGIC_GRPC`（默认 `localhost:9002`）
- `service LogicService`，由 Gateway 调用。绝大多数 RPC 请求都带 `token` 字段做鉴权。

| RPC | 请求 | 响应 |
|-----|------|------|
| `Login` | `LoginRequest{ username, password }` | `LoginResponse{ token, user_id }` |
| `Register` | `RegisterRequest{ username, password }` | `RegisterResponse{ user_id, token }` |
| `SendMessage` | `SendMessageRequest{ token, conversation_id, content }` | `SendMessageResponse{ message_id, created_at }` |
| `Sync` | `SyncRequest{ token, last_sync_at, limit }` | `SyncResponse{ conversations[], messages[] }` |
| `CreateGroup` | `CreateGroupRequest{ token, name, member_ids[] }` | `CreateGroupResponse{ group_id, conversation_id }` |
| `CreateDM` | `CreateDMRequest{ token, peer_id }` | `CreateDMResponse{ conversation_id }` |
| `MarkRead` | `MarkReadRequest{ token, conversation_id, msg_id }` | `MarkReadResponse{}` |
| `ListUsers` | `ListUsersRequest{ token }` | `ListUsersResponse{ users[] }` |
| `GetMessages` | `GetMessagesRequest{ token, conversation_id, before_id, limit }` | `GetMessagesResponse{ messages[] }` |
| `SendFriendRequest` | `SendFriendRequestReq{ token, to_id }` | `SendFriendRequestResp{}` |
| `HandleFriendRequest` | `HandleFriendRequestReq{ token, request_id, accept }` | `HandleFriendRequestResp{}` |
| `ListFriends` | `ListFriendsRequest{ token }` | `ListFriendsResponse{ friends[] }` |
| `ListFriendRequests` | `ListFriendRequestsRequest{ token }` | `ListFriendRequestsResponse{ requests[] }` |
| `GetInviteCode` | `GetInviteCodeRequest{ token }` | `GetInviteCodeResponse{ code }` |
| `RefreshInviteCode` | `GetInviteCodeRequest{ token }` | `GetInviteCodeResponse{ code }` |
| `AddFriendByCode` | `AddFriendByCodeRequest{ token, code }` | `AddFriendByCodeResponse{ username }` |

> proto 定义见 `proto/im.proto`。`created_at`、`updated_at`、`last_sync_at` 等时间字段统一为**毫秒时间戳**。

---

## 3. Fanout 服务（Kafka Consumer，内部）

Fanout 不对外暴露接口，作为 Kafka 消费者运行，消费者组 `fanout-group`。

- **消费 Topic**：`message_fanout`
- **事件结构** `FanoutEvent`：

  ```json
  {
    "event_type": "new_message",       // new_message | conv_created（空串按 new_message 兼容）
    "message_id": 100,
    "conversation_id": 10,
    "conversation_type": "dm",
    "conversation_name": "bob",
    "from_id": 2,
    "from_username": "bob",
    "content": "hello",
    "created_at": 1719800000000,
    "members": [1, 2]                  // 仅 conv_created 使用
  }
  ```

- **处理逻辑**：
  - `new_message`：查会话成员（Redis 缓存，miss 回落 DB），为每个成员 upsert 会话视图行，并对在线成员通过 Gateway gRPC `Push` 下发 `new_message`。
  - `conv_created`：刷新成员缓存，对在线成员下发 `conv_created`，使新会话立即出现在会话列表。
  - 单聊场景下，会话名对每个成员显示为「对方用户名」。

---

## 附：环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `POSTGRES_DSN` | `postgres://im:im@localhost:5432/im?sslmode=disable` | PostgreSQL 连接串 |
| `REDIS_ADDR` | `localhost:6379` | Redis 地址 |
| `KAFKA_BROKER` | `localhost:9092` | Kafka broker |
| `JWT_SECRET` | `dev-secret-key` | JWT 签名密钥 |
| `GATEWAY_GRPC` | `localhost:9001` | Gateway gRPC 监听地址 |
| `LOGIC_GRPC` | `localhost:9002` | Logic gRPC 监听地址 |
| `WS_ADDR` | `localhost:8080` | Gateway HTTP/WS 监听地址 |
