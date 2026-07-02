# IM 服务技术设计方案

## 1. 项目概述

基于 Go 实现的生产级即时通讯服务，支持单聊和群聊，采用**接入层/业务层分离 + 有状态/无状态分离**的架构，具备水平扩展能力。

**技术栈：** Go 1.22、gRPC、WebSocket、PostgreSQL、Redis、Kafka
**前端：** React + Vite（左侧会话列表 + 右侧聊天记录）

---

## 2. 整体架构

```
浏览器
  │ HTTP /api/*            │ WebSocket /ws
  ▼                        ▼
API Gateway（无状态,多实例）  Gateway（有状态,多实例）
  │ gRPC → Logic           │ 维护 WS 长连接 + 在线路由
  │                        │ 下行：被 Logic/Fanout gRPC 调用后推给客户端
  ▼
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
  Redis：在线路由表、群成员缓存、上线分布式锁
  Kafka：消息扩散队列
```

### 2.1 核心设计原则：把"有状态"和"无状态"彻底分开

这套架构最关键的一条主线是：**让每个进程要么完全无状态、要么只承载一种状态，绝不混搭。** 原因是两类进程的运维诉求完全相反：

- **无状态进程**（API Gateway、Logic、Fanout）：不持有任何客户端会话，可随意扩缩容、频繁发版、崩溃重启，代价只是几个正在处理的请求重试。
- **有状态进程**（Gateway）：持有海量 WebSocket 长连接，内存吃紧，**一重启就断开全部在线连接**，应尽量少动。

如果把这两类职责塞进同一个进程，就会出现"改一个交友接口的小 bug，也要重启、把所有人的长连接一起震断"的荒唐局面。因此本设计的两次关键拆分（见 §3.1、§3.2）都服务于这条原则。

---

## 3. 服务职责

### 3.1 API Gateway（无状态 HTTP 接入层）

**职责**：接管全部 `/api/*` HTTP 请求和前端静态资源，监听 `:8080`（前端唯一入口）。每个 handler 都是同一个样板：解出 `Authorization: Bearer <token>` → gRPC 转发给 Logic → 回写 JSON。不持有任何连接、不碰 Redis、不注册 gRPC server。

**为什么单独拆出来**：HTTP API 是纯无状态的转发层，发版最频繁（加接口、改字段天天有）。把它从 Gateway 剥离后，**发布 API 层不会影响任何一条已建立的 WebSocket 长连接**——这正是拆分最大的收益。同时 API 层可以按 HTTP QPS 独立扩缩，与长连接数解耦。

### 3.2 Gateway（有状态长连接层）

拆分后 Gateway 回归到"只管连接"的**极薄一层**，监听 `:8081`，只保留三件事：

- **WebSocket 接入**：`/ws` 升级 + 首帧 token 鉴权（本地 `parseUIDFromToken` 校验 JWT，**不再调用 Logic**，进一步去掉了对业务层的依赖）。
- **连接管理（Hub）**：进程内维护 `userID → *Conn` 映射；用户上线写 Redis 在线路由，下线删除；30s 一次 Ping 心跳、2min 一次路由续期。
- **下行推送**：暴露 `Push(user_id, payload)` / `Kick(user_id)` gRPC 接口，供 Logic/Fanout 反向调用，找到本地连接推给客户端。

**为什么要薄**：长连接层是整个系统里最不该抖动的部分。它越薄、依赖越少（拆分后连 Logic gRPC 客户端都去掉了），需要重启发版的理由就越少，在线用户的连接就越稳。

### 3.3 Logic（无状态业务层）

- 处理所有业务逻辑：登录/注册、发消息、会话、好友、邀请码等。
- 收到消息 → 鉴权 → 写 `messages` 表 → 发 1 条 Kafka 事件。
- 提供上线同步接口：按 `last_sync_at` 返回增量的 `user_conversations` 和消息内容。

### 3.4 Fanout Worker（无状态扩散层）

- 消费 Kafka `message_fanout` topic。
- 查 `conversation_members`（Redis 缓存，miss 则查 DB）得到成员列表。
- worker pool（并发上限 50）并行处理每个成员：
  - 批量 UPDATE `user_conversations`（不管在不在线都更新，用户上线靠它同步状态）。
  - 查 Redis 路由，在线则 gRPC 调对应 Gateway 推送完整消息，离线则跳过。

---

## 4. 单点登录与连接一致性（核心难点）

系统要求"单点登录"：同一账号后登录的连接挤掉先登录的。在**多 Gateway 实例 + 客户端并发重连**的场景下，这个看似简单的需求隐藏着几个竞态，本设计逐一做了防护。

### 4.1 全局唯一的路由值：`gatewayAddr#connID`

Redis 里的在线路由不再只存网关地址，而是 `"127.0.0.1:9001#42"` 形式：

- `gatewayAddr` 跨实例唯一，供推送方定位到哪台 Gateway；
- `connID`（进程内自增 `connSeq`）区分**同一用户在同一台网关上的新旧连接**。

**为什么需要 connID**：没有它，就无法区分"当前 Redis 里登记的到底是不是我这条连接"。有了它，下线清理才能做到精准的 compare-and-delete（见 §4.3）。

### 4.2 用分布式锁串行化"上线换绑"

两条连接（可能落在不同网关）几乎同时上线时，"读旧路由 → 写新路由 → 登记本地 Hub → 踢旧连接"这一串操作若交错执行，会产生 Redis 路由与本地 Hub 不一致、幽灵连接、踢错人等一系列难缠的竞态。

**方案**：上线时先对 `user:lock:{uid}` 加分布式锁（`SET NX PX` + 随机 token，释放时用 Lua 脚本 compare-then-DEL），把换绑串行化：

```
AcquireLock(uid)                     // 串行化同一 uid 的上线
  oldRoute = GetRoute(uid)           // 读旧路由      ┐
  SetRoute(uid, myRoute)             // 写新路由      ├ 临界区：只做快操作
  hub.Add(uid, conn)                 // 登记本地 Hub  ┘
ReleaseLock(uid, token)
if oldRoute != "" && oldRoute != myRoute:
  kickByRoute(uid, oldRoute)         // 慢 IO（gRPC 踢人）放在锁外
```

**为什么这样设计**：

- **临界区只放快操作**（3 次 Redis/内存操作，亚毫秒级），把唯一的慢 IO——跨网关 gRPC 踢人——放到锁外。因此 `lockTTL=3s` 纯粹是**进程崩溃时的兜底自动释放**，正常业务永远不会顶穿它。
- **为什么选锁而不是无锁 CAS**：早期尝试过 `SwapRoute`（Lua 原子换路由）+ 各种自检 / epoch 版本号的无锁方案，但要同时保证 "Redis 路由" 与 "本地 map 里那条 conn" 指向同一条连接，逻辑越堆越复杂、出错面越大。鉴于同一账号并发上线本就是极低频事件（锁冲突概率极小），一把简单的分布式锁反而**更薄、更好懂、错误面更少**——这是一次刻意的"用小成本换确定性"的权衡。

### 4.3 下线清理防误删：compare-and-delete

连接断开时的清理走在锁外，必须防止"旧连接的延迟清理，删掉了新连接刚写好的路由/Hub 项"：

- **Redis**：`DelRouteIf(uid, myRoute)` —— Lua 脚本 `GET == myRoute` 才 `DEL`，路由已被新连接覆盖时自动跳过。
- **本地 Hub**：`Remove(uid, conn)` —— 仅当 `conns[uid] == conn` 本人时才删。
- **本机新旧连接换绑**：`kickByRoute` 发现旧路由就在本机时**直接返回不踢**，因为本机换绑已由锁内 `hub.Add`（踢旧装新）完成，再踢会误伤刚上线的新连接。

### 4.4 goroutine 泄漏修复

心跳/续期 goroutine 早期只 `select` 在 `ticker.C` 上。`ticker.Stop()` **不会关闭 channel**，一个只阻塞在 `<-ticker.C` 的 goroutine 会永久阻塞、随连接累积而泄漏。

**修复**：引入 `done chan struct{}`，`handleWS` 退出时 `close(done)`，goroutine `select` 增加 `case <-done: return` 作为**唯一可靠的退出路径**。

---

## 5. 数据存储

### 数据库表

```sql
-- 用户
users(id, username, password_hash, invite_code, created_at)

-- 会话（单聊/群聊统一）
conversations(id, type, created_at)               -- type: "dm" | "group"
conversation_members(conversation_id, user_id)    -- 单聊2条，群聊N条

-- 群组附加信息
groups(id, conversation_id, name, owner_id)

-- 好友关系与申请
friends(user_id, friend_id, created_at)
friend_requests(id, from_id, to_id, status, created_at)

-- 消息源（不扩散，1条消息1条记录）
messages(id, conversation_id, from_id, content, created_at)

-- 用户维度会话状态（写扩散，每人1条）
user_conversations(
  user_id, conversation_id,
  last_msg_id,     -- 最新消息id，用于列表预览和上线同步
  unread_count,    -- 未读红点
  updated_at       -- 会话列表排序依据
)

-- 用户同步状态
user_sessions(user_id, last_sync_at)
```

### Redis

```
user:{id}:gateway   → "gateway-1:9001#42"   # 在线路由：gatewayAddr#connID，TTL 5min 靠续期保活
user:lock:{id}      → <random-token>         # 上线换绑分布式锁，PX 3s 崩溃兜底
conv:{id}:members   → [uid1, uid2, ...]      # 群成员缓存
```

**为什么路由带 TTL + 续期**：进程崩溃来不及删路由时，TTL 5min 会自动过期兜底；正常在线则每 2min 续期一次，保证不误判离线。

---

## 6. 消息流转

### 单聊（A → B）

```
A 发消息（HTTP /api/messages → API Gateway → gRPC → Logic）
→ Logic：写 messages，发1条 Kafka 事件 {msg_id, conversation_id, type:"dm"}
→ Fanout 消费：
    查 conversation_members 得到 [A, B]
    批量更新 A、B 的 user_conversations
    查 B 的路由 → 在线 → gRPC → 对应 Gateway → 推给 B（WS 下行）
               → 离线 → 跳过
```

### 群聊（A → 群G，成员 B/C/D...）

```
A 发群消息 → API Gateway → Logic
→ Logic：写 messages，发1条 Kafka 事件 {msg_id, conversation_id, type:"group"}
→ Fanout 消费：
    查群成员列表（Redis缓存优先）
    worker pool 并行（上限50）：
      批量 UPDATE user_conversations（所有成员）
      查每个在线成员路由 → gRPC → Gateway 推送
```

**为什么写扩散到 `user_conversations`**：只写 1 条 `messages` 源记录 + 每人 1 条会话状态，既避免了消息内容 N 份冗余，又让"未读数/会话排序/上线增量同步"这些用户维度的读操作变成单表点查，读写都轻。

### 用户上线同步

```
B 建立 WebSocket（/ws → Gateway，写 Redis 路由）
→ 客户端携带 last_sync_at 请求 Logic（HTTP /api/sync → API Gateway → Logic）
→ Logic 返回 user_conversations WHERE updated_at > last_sync_at + 对应 messages 内容
→ 更新 user_sessions.last_sync_at
```

---

## 7. 好友与群成员变化

- **加好友**：支持"申请-同意"（`friend_requests`）和"邀请码直加"（`users.invite_code`）两条路径。
- **退群**：从 `conversation_members` 删除记录，Fanout 后续扩散不再包含该用户，自然收不到新消息。
- **历史消息权限**：查询 `messages` 时校验请求方是否在 `conversation_members` 中，不在则拒绝。

---

## 8. 服务间通信

| 链路 | 协议 | 端口 |
|---|---|---|
| 客户端 ↔ API Gateway（HTTP API + 静态资源） | HTTP | :8080 |
| 客户端 ↔ Gateway（长连接） | WebSocket | :8081 (`/ws`) |
| API Gateway → Logic | gRPC | :9002 |
| Logic/Fanout → Gateway（下行推送 Push/Kick） | gRPC | :9001 |
| Logic → Kafka | Producer | — |
| Fanout ← Kafka | Consumer | — |

> 前端通过 Vite dev proxy 路由：`/api` → `:8080`，`/ws` → `:8081`；生产环境由反代按同样规则分流。

---

## 9. 目录结构

```
im-service/
├── apigateway/    # 无状态 HTTP API 接入层（:8080）
├── gateway/       # 有状态 WebSocket 长连接层（:8081）
│   ├── server.go  #   WS 升级 + 上线换绑 + Push/Kick
│   ├── hub.go     #   userID→Conn 映射，防误删的 Add/Remove
│   └── handler.go #   心跳/续期/读循环（done channel 防泄漏）
├── logic/         # Logic 服务
├── fanout/        # Fan-out Worker
├── proto/         # gRPC 接口定义（共用）
├── frontend/      # React + Vite 前端
└── pkg/
    ├── model/      # DB 模型
    ├── redisstore/ # 路由表 + 分布式锁
    ├── kafka/      # producer/consumer
    └── config/     # 配置
```

---

## 10. 扩展性与设计优点小结

| 维度 | 设计 | 优点 |
|---|---|---|
| **发版稳定性** | API 层与长连接层分进程 | 发布 API 不断开任何在线 WS 连接 |
| **独立扩缩** | 有状态/无状态分离 | API 按 QPS 扩，Gateway 按连接数扩，互不牵制 |
| **Gateway 扩容** | Redis 路由表 | 直接加实例，连接自动分散 |
| **Logic 扩容** | 无状态 | 直接加实例，客户端负载均衡 |
| **Fanout 扩容** | Kafka 分区 | 分区数决定最大并行度，加实例即可 |
| **单点登录正确性** | 分布式锁 + connID + CAS 清理 | 并发上线不产生幽灵连接/错删/踢错人 |
| **锁开销可控** | 临界区只放快操作，慢 IO 在锁外 | 锁 TTL 纯崩溃兜底，正常永不顶穿 |
| **无 goroutine 泄漏** | ticker + done channel | 长连接层可长期稳定运行 |
| **单群聊统一** | 按 `conversation.type` 区分 | 逻辑收敛，一套扩散代码覆盖两种场景 |
