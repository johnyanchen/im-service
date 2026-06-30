# IM 好友系统技术方案

## 1. 背景与目标

IM 系统初期，所有注册用户互相可见、可以直接发消息，不适合线上使用。需要引入好友机制：

- 用户之间必须先成为好友，才能发起聊天
- 添加好友通过**邀请码**（替代搜索），更贴近扫码加好友的体验
- 好友申请需要对方同意才能生效

## 2. 整体架构

```
┌─────────────┐     HTTP/JSON      ┌──────────────┐     gRPC      ┌─────────────┐
│   Frontend  │ ◄──────────────► │   Gateway    │ ◄───────────► │    Logic    │
│  (React)    │   Bearer Token     │  (路由转发)   │               │  (业务逻辑)  │
└─────────────┘                    └──────────────┘               └──────┬──────┘
                                                                         │
                                                                         │ SQL
                                                                         ▼
                                                                  ┌─────────────┐
                                                                  │  PostgreSQL │
                                                                  └─────────────┘
```

**职责分离：**
- **Gateway**：纯 HTTP ↔ gRPC 适配器，不含业务逻辑
- **Logic**：所有业务规则（鉴权、校验、事务编排）
- **Model**：SQL 操作封装

## 3. 数据模型

### 3.1 好友申请表 `friend_requests`

```sql
CREATE TABLE friend_requests (
    id BIGSERIAL PRIMARY KEY,
    from_id BIGINT NOT NULL REFERENCES users(id),  -- 申请人
    to_id BIGINT NOT NULL REFERENCES users(id),    -- 被申请人
    status VARCHAR(16) NOT NULL DEFAULT 'pending', -- pending/accepted/rejected
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_friend_requests_to ON friend_requests(to_id, status);
```

### 3.2 好友关系表 `friendships`

```sql
CREATE TABLE friendships (
    user_id BIGINT NOT NULL REFERENCES users(id),
    friend_id BIGINT NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, friend_id)
);
```

**设计决策：双向存储。** A 和 B 成为好友时，同时写入 `(A,B)` 和 `(B,A)` 两行。

好处：查询"我的好友列表"只需 `WHERE user_id = ?`，不需要 `OR` 条件，索引友好、查询简单。

代价：存储翻倍（但好友关系数据量小，可以接受）。

### 3.3 邀请码（users 表扩展）

```sql
ALTER TABLE users ADD COLUMN invite_code VARCHAR(8) UNIQUE;
```

每个用户一个 6 位大写字母数字码，UNIQUE 约束保证全局唯一。

## 4. 邀请码机制

### 4.1 生成策略

```go
func generateInviteCode() string {
    b := make([]byte, 4)
    rand.Read(b)  // crypto/rand，安全随机
    return strings.ToUpper(hex.EncodeToString(b))[:6]
}
```

- 使用 `crypto/rand` 而非 `math/rand`，避免可预测
- 6 位 hex 大写 → 16^6 ≈ 1600 万种组合
- 对于中小规模应用足够，大规模可扩到 8 位

### 4.2 碰撞处理

数据库有 UNIQUE 约束兜底，代码层面最多重试 5 次：

```go
func (db *DB) RefreshInviteCode(ctx context.Context, userID int64) (string, error) {
    for i := 0; i < 5; i++ {
        code := generateInviteCode()
        _, err := db.Pool.Exec(ctx, "UPDATE users SET invite_code=$1 WHERE id=$2", code, userID)
        if err == nil {
            return code, nil
        }
        if !strings.Contains(err.Error(), "invite_code") {
            return "", err  // 非碰撞错误，直接返回
        }
        // UNIQUE 冲突，重试
    }
    return "", fmt.Errorf("生成邀请码失败，请重试")
}
```

### 4.3 生命周期

- **注册时**自动生成，随用户永久存在
- **可刷新**：用户主动点击刷新按钮，旧码立即失效
- **不消耗**：同一个码可被多人使用来发送好友申请

## 5. 核心流程

### 5.1 添加好友（通过邀请码）

```
B 输入 A 的邀请码
       │
       ▼
POST /api/friends/add-by-code {code: "ABC123"}
       │
       ▼ Gateway 转发
Logic.AddFriendByCode()
       │
       ├─ 解析 token → 得到 B 的 uid
       ├─ GetUserByInviteCode("ABC123") → 找到 A
       ├─ 校验：不能加自己
       ├─ 校验：是否已经是好友
       └─ CreateFriendRequest(B, A) → 写入 pending 状态
       │
       ▼
返回 {username: "A"} → 前端提示"已向 A 发送好友申请"
```

### 5.2 处理好友申请

```
A 点击"接受"
       │
       ▼
POST /api/friends/handle {request_id: 1, accept: true}
       │
       ▼
Logic.HandleFriendRequest()
       │
       ├─ 解析 token → 得到 A 的 uid
       ├─ 查询 friend_request → 校验是否是发给 A 的
       ├─ 校验 status == 'pending'（防止重复处理）
       └─ AcceptFriendRequest() → 事务内：
              ├─ UPDATE friend_requests SET status='accepted'
              └─ INSERT INTO friendships VALUES (A,B),(B,A)
```

### 5.3 关键事务

`AcceptFriendRequest` 是唯一使用显式事务的地方：

```go
func (db *DB) AcceptFriendRequest(ctx context.Context, id, fromID, toID int64) error {
    tx, err := db.Pool.Begin(ctx)
    if err != nil { return err }
    defer tx.Rollback(ctx)

    // 更新申请状态
    _, err = tx.Exec(ctx, `UPDATE friend_requests SET status='accepted' WHERE id=$1`, id)
    if err != nil { return err }

    // 双向写入好友关系
    _, err = tx.Exec(ctx,
        `INSERT INTO friendships (user_id, friend_id) VALUES ($1,$2),($2,$1) ON CONFLICT DO NOTHING`,
        fromID, toID)
    if err != nil { return err }

    return tx.Commit(ctx)
}
```

为什么需要事务：确保"标记已接受"和"建立好友关系"是原子的，不会出现接受了但关系没建立的中间状态。

`ON CONFLICT DO NOTHING` 处理幂等：即使重复调用也不会报错。

## 6. API 设计

| Method | Path | 说明 | 请求体 |
|--------|------|------|--------|
| GET | `/api/invite-code` | 获取我的邀请码 | - |
| POST | `/api/invite-code/refresh` | 刷新邀请码 | - |
| POST | `/api/friends/add-by-code` | 通过邀请码加好友 | `{code}` |
| GET | `/api/friends` | 好友列表 | - |
| GET | `/api/friends/requests` | 收到的好友申请（全部） | - |
| POST | `/api/friends/handle` | 处理申请 | `{request_id, accept}` |

所有接口通过 `Authorization: Bearer <token>` 鉴权。

## 7. 安全考量

| 风险 | 当前措施 | 改进方向 |
|------|----------|----------|
| 邀请码暴力枚举 | UNIQUE + 16^6 空间 | 加 rate limit |
| 重复发送申请 | 无去重 | 加 UNIQUE(from_id, to_id, status='pending') |
| 已拒绝后再次申请 | 允许 | 可加冷却期 |
| Token 伪造 | JWT 签名校验 | — |

## 8. 前端交互

联系人页面分为左右两栏：

**左栏（平铺列表）：**
- 顶部：我的邀请码 + 输入好友码的表单
- 中间：好友申请列表（有红点标记待处理）
- 下方：好友列表

**右栏（详情）：**
- 点击好友 → 显示头像、用户名、"已是好友"
- 点击待处理申请 → 显示头像、用户名、接受/拒绝按钮
- 默认空状态 → "选择一个联系人查看详情"

## 9. 总结

| 设计要点 | 选择 | 原因 |
|----------|------|------|
| 好友关系存储 | 双向行 | 查询简单，索引高效 |
| 添加方式 | 邀请码 | Web 端无法扫码，码输入更通用 |
| 码格式 | 6 位大写 hex | 好记、好输入、空间够用 |
| 碰撞处理 | DB UNIQUE + 重试 | 简单可靠 |
| 接受事务 | 显式 BEGIN/COMMIT | 保证状态一致性 |
| 申请记录 | 永久保留 | 方便追溯，不删除 |
