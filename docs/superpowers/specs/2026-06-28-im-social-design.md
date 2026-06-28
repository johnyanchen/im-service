# IM 社交聊天软件 · 技术设计方案

## 1. 项目概述

从零设计一款面向社交场景的即时通讯软件，支持单聊、群聊、好友系统。先做 Web 端，架构预留移动端扩展能力。

**技术栈：** Go 1.22 后端、React + Vite + Tailwind 前端、PostgreSQL、Redis、Kafka  
**目标规模：** 中型（万~十万级用户），支持水平扩展  
**消息存储策略：** 服务端永久存储，浏览器不做本地持久化

---

## 2. 整体架构

```
HTTP 链路（查询/操作，同步请求-响应）：

  浏览器 ──→ Gateway ──→ Logic ──→ PostgreSQL / Redis
         HTTP           gRPC        ←── 响应原路返回


WebSocket 链路（发消息，分两步）：

  步骤1 - 写入：
  浏览器 ──→ Gateway ──→ Logic ──→ ① 写 messages 表
         WS           gRPC        ② 发 Kafka 事件

  步骤2 - 扩散推送（异步）：
  Kafka ──→ Fanout ──→ ① 写 user_conversations（PostgreSQL）
                       ② 查路由（Redis）
                       ③ gRPC Push ──→ Gateway ──→ 接收方浏览器
```

### 服务职责

| 服务 | 职责 |
|------|------|
| **Gateway** | HTTP REST 接口 + WebSocket 长连接管理，透传请求给 Logic，接收 Fanout 推送指令推给客户端 |
| **Logic** | 所有业务逻辑（auth / user / relation / conversation / message），内部按模块组织 |
| **Fanout** | Kafka 消费，写扩散 user_conversations + 查路由推送在线成员 |

### 存储

| 组件 | 用途 |
|------|------|
| PostgreSQL（主从） | 持久化所有业务数据 |
| Redis | 在线路由表、群成员缓存、加好友 token、refresh_token 黑名单 |
| Kafka | 消息扩散队列（message_fanout topic） |

---

## 3. 前端设计

### 技术方案
- React + Vite + Tailwind CSS
- 状态管理：React Context + useReducer
- 通信：HTTP REST（查询/操作）+ 一条 WebSocket（实时推送）
- Token 存 localStorage，请求自动带 Authorization: Bearer header

### 布局
三栏布局 + 微信配色：
- **左栏**（60px）：深灰 #2e2e2e 导航条，放头像、会话💬、联系人👥、收藏⭐、设置⚙️，当前选中项绿色高亮
- **中栏**（240px）：浅灰 #f7f7f7 会话列表/联系人列表，选中项绿色底 #c9e7c8
- **右栏**（flex）：灰色背景 #ebebeb 聊天区，对方白色气泡，自己绿色气泡 #95ec69

### 主色
- 微信绿 #07c160
- 头像方圆角 border-radius: 4px

---

## 4. 数据模型

```sql
-- 用户
users (
  id BIGSERIAL PRIMARY KEY,
  username VARCHAR(32) UNIQUE NOT NULL,
  password_hash VARCHAR(128) NOT NULL,
  nickname VARCHAR(64),
  avatar_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE EXTENSION pg_trgm;
CREATE INDEX idx_users_username_trgm ON users USING gin (username gin_trgm_ops);

-- 好友关系（双向存储，A加B存两条）
friendships (
  user_id BIGINT NOT NULL,
  friend_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'accepted',  -- accepted / blocked
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, friend_id)
);

-- 好友申请
friend_requests (
  id BIGSERIAL PRIMARY KEY,
  from_id BIGINT NOT NULL,
  to_id BIGINT NOT NULL,
  message VARCHAR(128),
  status VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending / accepted / rejected
  created_at TIMESTAMPTZ DEFAULT NOW(),
  expired_at TIMESTAMPTZ
);

-- 会话（单聊/群聊统一）
conversations (
  id BIGSERIAL PRIMARY KEY,
  type VARCHAR(8) NOT NULL,           -- dm / group
  name VARCHAR(64),                    -- 群名，单聊为 NULL
  owner_id BIGINT,                     -- 群主，单聊为 NULL
  avatar_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 会话成员
conversation_members (
  conversation_id BIGINT NOT NULL,
  user_id BIGINT NOT NULL,
  role VARCHAR(8) NOT NULL DEFAULT 'member',  -- owner / admin / member
  joined_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (conversation_id, user_id)
);

-- 消息
messages (
  id BIGSERIAL PRIMARY KEY,
  conversation_id BIGINT NOT NULL,
  from_id BIGINT NOT NULL,
  msg_type VARCHAR(16) NOT NULL DEFAULT 'text',  -- text / image / system（预留）
  content TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_messages_conv_id ON messages (conversation_id, id DESC);

-- 用户维度会话状态（写扩散）
user_conversations (
  user_id BIGINT NOT NULL,
  conversation_id BIGINT NOT NULL,
  last_msg_id BIGINT DEFAULT 0,
  last_read_msg_id BIGINT DEFAULT 0,
  muted BOOLEAN DEFAULT FALSE,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, conversation_id)
);
```

**未读数计算：** 不存储 unread_count 字段，通过 `last_msg_id - last_read_msg_id` 或 `SELECT count(*) FROM messages WHERE conversation_id = ? AND id > last_read_msg_id` 实时计算。

### Redis 数据

```
user:{id}:gateway       → "gateway-1:9001"       # 在线路由
conv:{id}:members       → [uid1, uid2, ...]      # 群成员缓存
add_friend:{code}       → user_id (TTL 5min)     # 加好友一次性 token
refresh_blacklist:{jti} → 1 (TTL = token剩余有效期)  # token 黑名单
```

---

## 5. 接口设计

### HTTP REST（Bearer Token 鉴权）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/auth/register` | 注册 |
| POST | `/api/auth/login` | 登录，返回 access_token(15min) + refresh_token(7d) |
| POST | `/api/auth/refresh` | 刷新 token |
| GET | `/api/users/me` | 获取自己的资料 |
| PUT | `/api/users/me` | 修改昵称/头像 |
| GET | `/api/friends` | 好友列表 |
| POST | `/api/friends/qrcode` | 生成加好友 token（存 Redis TTL 5min），返回链接 |
| POST | `/api/friends/add` | 通过 token 发起好友申请 |
| GET | `/api/friends/requests` | 收到的好友申请列表 |
| POST | `/api/friends/requests/:id/accept` | 同意（写双向 friendships + 创建 DM 会话） |
| POST | `/api/friends/requests/:id/reject` | 拒绝 |
| DELETE | `/api/friends/:id` | 删除好友 |
| POST | `/api/friends/:id/block` | 拉黑 |
| GET | `/api/conversations` | 会话列表（带实时未读数计算） |
| POST | `/api/conversations` | 创建群聊 |
| GET | `/api/conversations/:id/messages` | 历史消息（cursor-based 分页） |
| GET | `/api/conversations/:id/members` | 群成员列表 |
| POST | `/api/conversations/:id/members` | 邀请入群 |
| DELETE | `/api/conversations/:id/members/:uid` | 踢人/退群 |

### WebSocket（实时通信）

连接建立后通过首条消息传递 access_token 认证。

| 消息类型 | 方向 | 说明 |
|------|------|------|
| `send_message` | 客户端→服务端 | 发消息 |
| `new_message` | 服务端→客户端 | 新消息推送 |
| `mark_read` | 客户端→服务端 | 标记已读到某条 msg_id |
| `typing` | 双向 | 正在输入状态 |
| `friend_request` | 服务端→客户端 | 收到好友申请通知 |
| `friend_accepted` | 服务端→客户端 | 好友申请被同意通知 |

---

## 6. 消息流转

### 发消息

```
客户端 WS send_message {conversation_id, content}
→ Gateway 转发 gRPC → Logic
→ Logic：鉴权 → 写 messages 表 → 发 1 条 Kafka 事件 {msg_id, conversation_id, from_id, content}
→ Fanout 消费：
    查 conversation_members（Redis 缓存优先）
    批量更新所有成员的 user_conversations.last_msg_id / updated_at
    查每个成员 Redis 路由，在线则 gRPC → Gateway 推送 new_message
```

### 加好友（二维码 + 一次性 token）

```
A 点击"我的二维码"
→ POST /api/friends/qrcode
→ Logic 生成随机 code，Redis SET add_friend:{code} = A的user_id (TTL 5min)
→ 返回链接 https://im.example.com/add?code=xxx
→ 前端用 qrcode.js 编码为二维码展示

B 扫码/打开链接
→ POST /api/friends/add {code}
→ Logic 查 Redis 拿到 A 的 user_id → 写 friend_requests(from=B, to=A)
→ A 在线 → WS 推送 friend_request 通知

A 同意
→ POST /api/friends/requests/:id/accept
→ Logic：写双向 friendships + 自动创建 DM 会话
→ B 在线 → WS 推送 friend_accepted 通知
```

### 用户上线

```
用户打开页面 → WebSocket 连接建立 → 首条消息发 access_token
→ Gateway 验证 token → 写 Redis 路由
→ 返回会话列表（全量 user_conversations + 实时计算未读数）
用户点进某会话 → GET /api/conversations/:id/messages 分页加载
```

### 已读标记

```
客户端 WS mark_read {conversation_id, msg_id}
→ Logic 更新 user_conversations SET last_read_msg_id = msg_id
```

---

## 7. 认证方案

- **access_token**：JWT，有效期 15 分钟，签名含 uid + jti
- **refresh_token**：JWT，有效期 7 天，用于刷新 access_token
- **登出**：将 refresh_token 的 jti 写入 Redis 黑名单（TTL = 剩余有效期）
- **WebSocket 认证**：连接建立后首条消息传 access_token，验证通过才注册路由

---

## 8. 目录结构

```
im-social/
├── gateway/           # Gateway 服务
│   ├── main.go
│   ├── server.go      # HTTP 路由 + WS 升级
│   ├── handler.go     # WebSocket 消息处理
│   └── hub.go         # 连接管理
├── logic/             # Logic 服务
│   ├── main.go
│   ├── server.go      # gRPC server
│   ├── auth.go        # 注册/登录/token 刷新
│   ├── user.go        # 用户资料
│   ├── relation.go    # 好友关系（申请/同意/拉黑）
│   ├── conversation.go # 会话管理
│   ├── message.go     # 发消息/历史查询
│   └── db/            # 数据库访问层
├── fanout/            # Fanout Worker
│   ├── main.go
│   ├── worker.go
│   └── fanout.go
├── proto/             # gRPC 接口定义
├── pkg/
│   ├── config/        # 配置
│   ├── model/         # DB 模型
│   └── redisstore/    # Redis 操作封装
├── migrations/        # SQL 迁移文件
├── frontend/          # React SPA
│   ├── src/
│   │   ├── components/
│   │   ├── contexts/
│   │   ├── hooks/
│   │   └── pages/
│   └── vite.config.js
└── docker-compose.yml
```

---

## 9. 扩展性

- **Gateway 扩容**：直接加实例，Redis 路由表自动分散连接
- **Logic 扩容**：无状态，直接加实例负载均衡
- **Fanout 扩容**：增加 Kafka 分区 + 消费者实例
- **数据库读写分离**：写走主库，会话列表/历史消息查询走从库
- **移动端扩展**：API 设计已是标准 REST + WS，移动端接入无需改后端
- **消息类型扩展**：messages.msg_type 预留，后续加图片/语音只需扩展类型和存储
