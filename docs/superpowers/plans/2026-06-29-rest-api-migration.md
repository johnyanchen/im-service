# 将 WebSocket 操作迁移为 HTTP REST API

## Context

当前项目中，除了 login/register/list_users 是 HTTP 接口外，其余操作（send、sync、mark_read、create_dm、create_group）全部走 WebSocket 消息。这不是标准做法——业界规范是 **WebSocket 只做服务端推送（下行通道）**，所有客户端发起的操作走 HTTP REST。这样做的好处：

- REST 有天然的请求/响应语义、状态码、错误处理
- 更容易加中间件（限流、鉴权、日志）
- 前端不需要维护复杂的 WebSocket 消息路由
- WebSocket 连接断开不影响操作（REST 是独立请求）

## 改动范围

### Gateway 后端（`gateway/server.go` + `gateway/handler.go`）

新增 REST 路由：

| Method | Path | 原 WS type | 请求体 |
|--------|------|-----------|--------|
| POST | `/api/messages` | send | `{conversation_id, content}` |
| GET | `/api/sync?last_sync_at=xxx` | sync | query param |
| POST | `/api/conversations/:id/read` | mark_read | `{msg_id}` |
| POST | `/api/conversations/dm` | create_dm | `{peer_id}` |
| POST | `/api/conversations/group` | create_group | `{name, member_ids}` |

所有接口通过 `Authorization: Bearer <token>` 鉴权。

提取一个公共的 `extractToken(r) (int64, error)` 方法从 header 解析 token 并验证得到 uid。

### Gateway handler.go

- 删除 `handleWS` 中的 switch/case 业务逻辑
- `handleWS` 只保留一个空的读循环（保活连接、检测断开）
- 删除 `WSMessage` 结构体（不再需要）

### 前端（`frontend/src/components/Chat.jsx`）

- `sendMsg` → `fetch POST /api/messages`
- `mark_read` → `fetch POST /api/conversations/:id/read`
- `create_dm` → `fetch POST /api/conversations/dm`
- `create_group` → `fetch POST /api/conversations/group`
- `sync`（初始化时和 dm_created/group_created 后）→ `fetch GET /api/sync`
- WebSocket 只监听 `onmessage`，不再发送业务消息（只发认证 token）

## 关键文件

- `gateway/server.go` — 添加路由和 handler
- `gateway/handler.go` — 精简为只保持连接
- `frontend/src/components/Chat.jsx` — 改用 fetch

## 验证方式

1. `go build ./...` 编译通过
2. 启动 gateway + logic + 基础设施
3. 前端登录 → 发消息 → 收到实时推送 → 切换会话 → 已读标记正常
4. 开两个浏览器 tab 互发消息验证实时推送仍然工作
