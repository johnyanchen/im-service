# IM 服务实现计划

> **自动化执行提示:** 推荐使用 superpowers:subagent-driven-development 或 superpowers:executing-plans 逐任务实现。步骤使用 `- [ ]` 语法跟踪进度。

**目标：** 构建支持单聊和群聊的生产级即时通讯服务，采用 Gateway/Logic/Fanout 三层分离架构。

**架构：** 三个 Go 服务 — Gateway（WebSocket 连接管理）、Logic（无状态业务逻辑）、Fanout Worker（Kafka 消费者，负责写扩散和在线推送）。服务间通过 gRPC 通信；消息持久化到 PostgreSQL；Redis 存储路由表和群成员缓存；Kafka 解耦消息扩散。

**技术栈：** Go 1.22、gRPC (google.golang.org/grpc)、gorilla/websocket、pgx/v5 (PostgreSQL)、go-redis/v9、IBM/sarama (Kafka)、golang-jwt/jwt

---

## 文件地图

```
im-service/
├── proto/
│   └── im.proto                        # gRPC 服务定义
├── pkg/
│   ├── config/config.go                # 共享配置结构体 + 加载器
│   ├── model/
│   │   ├── db.go                       # 数据库连接池
│   │   ├── user.go                     # users 表操作
│   │   ├── conversation.go             # conversations + conversation_members 操作
│   │   ├── message.go                  # messages 表操作
│   │   ├── user_conversation.go        # user_conversations 表操作
│   │   └── group.go                    # groups 表操作
│   ├── redisstore/
│   │   ├── client.go                   # Redis 客户端初始化
│   │   ├── routing.go                  # 用户路由（get/set/del）
│   │   └── members.go                  # 群成员缓存
│   └── kafka/
│       ├── producer.go                 # Kafka 生产者
│       └── consumer.go                 # Kafka 消费者基础封装
├── gateway/
│   ├── main.go
│   ├── server.go                       # HTTP 服务 + WS 升级 + gRPC 服务
│   ├── hub.go                          # 内存 map[userID]→WS conn
│   └── handler.go                      # WS 读循环 → gRPC 调用 Logic
├── logic/
│   ├── main.go
│   ├── server.go                       # gRPC 服务启动
│   ├── auth.go                         # 登录/注册处理（JWT 签发）
│   ├── message.go                      # SendMessage：写 DB + 发 Kafka
│   └── sync.go                         # Sync：上线时返回增量数据
├── fanout/
│   ├── main.go
│   ├── worker.go                       # Kafka 消费循环
│   └── fanout.go                       # 扩散逻辑：更新 user_conversations + 推送
├── web/
│   └── index.html                      # 单页前端
├── migrations/
│   └── 001_init.sql                    # 数据库建表
├── docker-compose.yml                  # PostgreSQL + Redis + Kafka
└── go.mod
```

---

## Task 1：项目脚手架与基础设施

**文件：**
- 创建：`go.mod`
- 创建：`docker-compose.yml`
- 创建：`migrations/001_init.sql`

- [ ] **步骤 1：初始化 Go 模块**

```bash
cd im-service
go mod init im-service
```

- [ ] **步骤 2：创建 docker-compose.yml**

```yaml
version: "3.9"
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: im
      POSTGRES_PASSWORD: im
      POSTGRES_DB: im
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  kafka:
    image: bitnami/kafka:3.7
    environment:
      KAFKA_CFG_NODE_ID: 1
      KAFKA_CFG_PROCESS_ROLES: broker,controller
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "9092:9092"

volumes:
  pgdata:
```

- [ ] **步骤 3：创建 migrations/001_init.sql**

```sql
CREATE TABLE users (
    id BIGSERIAL PRIMARY KEY,
    username VARCHAR(64) UNIQUE NOT NULL,
    password_hash VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE conversations (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(8) NOT NULL CHECK (type IN ('dm', 'group')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE conversation_members (
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    user_id BIGINT NOT NULL REFERENCES users(id),
    PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE groups (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT UNIQUE NOT NULL REFERENCES conversations(id),
    name VARCHAR(128) NOT NULL,
    owner_id BIGINT NOT NULL REFERENCES users(id)
);

CREATE TABLE messages (
    id BIGSERIAL PRIMARY KEY,
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    from_id BIGINT NOT NULL REFERENCES users(id),
    content TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_messages_conv_id ON messages(conversation_id, id);

CREATE TABLE user_conversations (
    user_id BIGINT NOT NULL REFERENCES users(id),
    conversation_id BIGINT NOT NULL REFERENCES conversations(id),
    last_msg_id BIGINT,
    unread_count INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, conversation_id)
);

CREATE INDEX idx_user_conv_updated ON user_conversations(user_id, updated_at);

CREATE TABLE user_sessions (
    user_id BIGINT PRIMARY KEY REFERENCES users(id),
    last_sync_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

- [ ] **步骤 4：启动基础设施并执行迁移**

```bash
docker compose up -d
sleep 5
psql "postgres://im:im@localhost:5432/im" -f migrations/001_init.sql
```

预期：所有表创建成功，无错误。

- [ ] **步骤 5：提交**

```bash
git add go.mod docker-compose.yml migrations/
git commit -m "feat: 项目脚手架，docker-compose 和数据库迁移"
```

---

## Task 2：Proto 定义与代码生成

**文件：**
- 创建：`proto/im.proto`

- [ ] **步骤 1：创建 proto/im.proto**

```protobuf
syntax = "proto3";
package im;
option go_package = "im-service/proto";

message SendMessageRequest {
  string token = 1;
  int64 conversation_id = 2;
  string content = 3;
}

message SendMessageResponse {
  int64 message_id = 1;
  int64 created_at = 2;
}

message SyncRequest {
  string token = 1;
  int64 last_sync_at = 2;
}

message ConversationState {
  int64 conversation_id = 1;
  string type = 2;
  int64 last_msg_id = 3;
  int32 unread_count = 4;
  int64 updated_at = 5;
}

message MessageItem {
  int64 id = 1;
  int64 conversation_id = 2;
  int64 from_id = 3;
  string from_username = 4;
  string content = 5;
  int64 created_at = 6;
}

message SyncResponse {
  repeated ConversationState conversations = 1;
  repeated MessageItem messages = 2;
}

message LoginRequest {
  string username = 1;
  string password = 2;
}

message LoginResponse {
  string token = 1;
  int64 user_id = 2;
}

message RegisterRequest {
  string username = 1;
  string password = 2;
}

message RegisterResponse {
  int64 user_id = 1;
  string token = 2;
}

message CreateGroupRequest {
  string token = 1;
  string name = 2;
  repeated int64 member_ids = 3;
}

message CreateGroupResponse {
  int64 group_id = 1;
  int64 conversation_id = 2;
}

message CreateDMRequest {
  string token = 1;
  int64 peer_id = 2;
}

message CreateDMResponse {
  int64 conversation_id = 1;
}

message PushRequest {
  int64 user_id = 1;
  bytes payload = 2;
}

message PushResponse {}

service LogicService {
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc SendMessage(SendMessageRequest) returns (SendMessageResponse);
  rpc Sync(SyncRequest) returns (SyncResponse);
  rpc CreateGroup(CreateGroupRequest) returns (CreateGroupResponse);
  rpc CreateDM(CreateDMRequest) returns (CreateDMResponse);
}

service GatewayService {
  rpc Push(PushRequest) returns (PushResponse);
}
```

- [ ] **步骤 2：生成 Go 代码**

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
protoc --go_out=. --go-grpc_out=. proto/im.proto
```

预期：生成 `proto/im.pb.go` 和 `proto/im_grpc.pb.go`。

- [ ] **步骤 3：添加依赖**

```bash
go get google.golang.org/grpc
go get google.golang.org/protobuf
go get github.com/gorilla/websocket
go get github.com/jackc/pgx/v5
go get github.com/redis/go-redis/v9
go get github.com/IBM/sarama
go get github.com/golang-jwt/jwt/v5
go get golang.org/x/crypto
go mod tidy
```

- [ ] **步骤 4：提交**

```bash
git add proto/ go.mod go.sum
git commit -m "feat: gRPC proto 定义与代码生成"
```

---

## Task 3：共享配置包

**文件：**
- 创建：`pkg/config/config.go`

- [ ] **步骤 1：创建 pkg/config/config.go**

```go
package config

import "os"

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	KafkaBrokers  []string
	JWTSecret     string
	GatewayGRPC   string
	LogicGRPC     string
	WebSocketAddr string
}

func Load() *Config {
	return &Config{
		PostgresDSN:   getEnv("POSTGRES_DSN", "postgres://im:im@localhost:5432/im?sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:  []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		JWTSecret:     getEnv("JWT_SECRET", "dev-secret-key"),
		GatewayGRPC:   getEnv("GATEWAY_GRPC", "localhost:9001"),
		LogicGRPC:     getEnv("LOGIC_GRPC", "localhost:9002"),
		WebSocketAddr: getEnv("WS_ADDR", "localhost:8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **步骤 2：验证编译通过**

```bash
go build ./pkg/config/
```

预期：无错误。

- [ ] **步骤 3：提交**

```bash
git add pkg/config/
git commit -m "feat: 共享配置包"
```

---

## Task 4：数据库模型层

**文件：**
- 创建：`pkg/model/db.go`
- 创建：`pkg/model/user.go`
- 创建：`pkg/model/conversation.go`
- 创建：`pkg/model/message.go`
- 创建：`pkg/model/user_conversation.go`
- 创建：`pkg/model/group.go`

- [ ] **步骤 1：创建 pkg/model/db.go**

```go
package model

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool
}

func NewDB(ctx context.Context, dsn string) (*DB, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &DB{Pool: pool}, nil
}

func (db *DB) Close() {
	db.Pool.Close()
}
```

- [ ] **步骤 2：创建 pkg/model/user.go**

```go
package model

import (
	"context"
	"time"
)

type User struct {
	ID           int64
	Username     string
	PasswordHash string
	CreatedAt    time.Time
}

func (db *DB) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO users(username, password_hash) VALUES($1, $2) RETURNING id",
		username, passwordHash).Scan(&id)
	return id, err
}

func (db *DB) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	u := &User{}
	err := db.Pool.QueryRow(ctx,
		"SELECT id, username, password_hash, created_at FROM users WHERE username=$1",
		username).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		return nil, err
	}
	return u, nil
}
```

- [ ] **步骤 3：创建 pkg/model/conversation.go**

```go
package model

import "context"

type Conversation struct {
	ID   int64
	Type string
}

func (db *DB) CreateConversation(ctx context.Context, convType string) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO conversations(type) VALUES($1) RETURNING id",
		convType).Scan(&id)
	return id, err
}

func (db *DB) AddConversationMember(ctx context.Context, convID, userID int64) error {
	_, err := db.Pool.Exec(ctx,
		"INSERT INTO conversation_members(conversation_id, user_id) VALUES($1, $2) ON CONFLICT DO NOTHING",
		convID, userID)
	return err
}

func (db *DB) GetConversationMembers(ctx context.Context, convID int64) ([]int64, error) {
	rows, err := db.Pool.Query(ctx,
		"SELECT user_id FROM conversation_members WHERE conversation_id=$1", convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var members []int64
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			return nil, err
		}
		members = append(members, uid)
	}
	return members, nil
}

func (db *DB) IsMember(ctx context.Context, convID, userID int64) (bool, error) {
	var exists bool
	err := db.Pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM conversation_members WHERE conversation_id=$1 AND user_id=$2)",
		convID, userID).Scan(&exists)
	return exists, err
}

func (db *DB) FindDMConversation(ctx context.Context, userA, userB int64) (int64, error) {
	var convID int64
	err := db.Pool.QueryRow(ctx, `
		SELECT cm1.conversation_id FROM conversation_members cm1
		JOIN conversation_members cm2 ON cm1.conversation_id = cm2.conversation_id
		JOIN conversations c ON c.id = cm1.conversation_id
		WHERE cm1.user_id=$1 AND cm2.user_id=$2 AND c.type='dm'
		LIMIT 1`, userA, userB).Scan(&convID)
	return convID, err
}
```

- [ ] **步骤 4：创建 pkg/model/message.go**

```go
package model

import (
	"context"
	"time"
)

type Message struct {
	ID             int64
	ConversationID int64
	FromID         int64
	FromUsername   string
	Content        string
	CreatedAt      time.Time
}

func (db *DB) CreateMessage(ctx context.Context, convID, fromID int64, content string) (*Message, error) {
	msg := &Message{}
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO messages(conversation_id, from_id, content) VALUES($1, $2, $3) RETURNING id, conversation_id, from_id, content, created_at",
		convID, fromID, content).Scan(&msg.ID, &msg.ConversationID, &msg.FromID, &msg.Content, &msg.CreatedAt)
	return msg, err
}

func (db *DB) GetMessagesSince(ctx context.Context, convID, sinceID int64, limit int) ([]*Message, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT m.id, m.conversation_id, m.from_id, u.username, m.content, m.created_at
		 FROM messages m JOIN users u ON u.id = m.from_id
		 WHERE m.conversation_id=$1 AND m.id > $2 ORDER BY m.id ASC LIMIT $3`,
		convID, sinceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		if err := rows.Scan(&m.ID, &m.ConversationID, &m.FromID, &m.FromUsername, &m.Content, &m.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}
```

- [ ] **步骤 5：创建 pkg/model/user_conversation.go**

```go
package model

import (
	"context"
	"time"
)

type UserConversation struct {
	UserID         int64
	ConversationID int64
	LastMsgID      int64
	UnreadCount    int32
	UpdatedAt      time.Time
}

func (db *DB) UpsertUserConversation(ctx context.Context, userID, convID, msgID int64, isSender bool) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_conversations(user_id, conversation_id, last_msg_id, unread_count, updated_at)
		VALUES($1, $2, $3, CASE WHEN $4 THEN 0 ELSE 1 END, NOW())
		ON CONFLICT(user_id, conversation_id) DO UPDATE SET
			last_msg_id = $3,
			unread_count = CASE WHEN $4 THEN 0 ELSE user_conversations.unread_count + 1 END,
			updated_at = NOW()`,
		userID, convID, msgID, isSender)
	return err
}

func (db *DB) GetUpdatedConversations(ctx context.Context, userID int64, since time.Time) ([]*UserConversation, error) {
	rows, err := db.Pool.Query(ctx,
		`SELECT user_id, conversation_id, last_msg_id, unread_count, updated_at
		 FROM user_conversations WHERE user_id=$1 AND updated_at > $2 ORDER BY updated_at DESC`,
		userID, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*UserConversation
	for rows.Next() {
		uc := &UserConversation{}
		if err := rows.Scan(&uc.UserID, &uc.ConversationID, &uc.LastMsgID, &uc.UnreadCount, &uc.UpdatedAt); err != nil {
			return nil, err
		}
		result = append(result, uc)
	}
	return result, nil
}

func (db *DB) UpdateSyncTime(ctx context.Context, userID int64) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO user_sessions(user_id, last_sync_at) VALUES($1, NOW())
		ON CONFLICT(user_id) DO UPDATE SET last_sync_at = NOW()`, userID)
	return err
}

func (db *DB) GetLastSyncAt(ctx context.Context, userID int64) (time.Time, error) {
	var t time.Time
	err := db.Pool.QueryRow(ctx,
		"SELECT last_sync_at FROM user_sessions WHERE user_id=$1", userID).Scan(&t)
	return t, err
}
```

- [ ] **步骤 6：创建 pkg/model/group.go**

```go
package model

import "context"

type Group struct {
	ID             int64
	ConversationID int64
	Name           string
	OwnerID        int64
}

func (db *DB) CreateGroup(ctx context.Context, convID int64, name string, ownerID int64) (int64, error) {
	var id int64
	err := db.Pool.QueryRow(ctx,
		"INSERT INTO groups(conversation_id, name, owner_id) VALUES($1, $2, $3) RETURNING id",
		convID, name, ownerID).Scan(&id)
	return id, err
}
```

- [ ] **步骤 7：验证编译通过**

```bash
go build ./pkg/model/
```

预期：无错误。

- [ ] **步骤 8：提交**

```bash
git add pkg/model/
git commit -m "feat: 数据库模型层（用户、会话、消息）"
```

---

## Task 5：Redis 存储包

**文件：**
- 创建：`pkg/redisstore/client.go`
- 创建：`pkg/redisstore/routing.go`
- 创建：`pkg/redisstore/members.go`

- [ ] **步骤 1：创建 pkg/redisstore/client.go**

```go
package redisstore

import "github.com/redis/go-redis/v9"

type Store struct {
	Client *redis.Client
}

func New(addr string) *Store {
	return &Store{
		Client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}
```

- [ ] **步骤 2：创建 pkg/redisstore/routing.go**

```go
package redisstore

import (
	"context"
	"fmt"
)

func routingKey(userID int64) string {
	return fmt.Sprintf("user:%d:gateway", userID)
}

func (s *Store) SetRoute(ctx context.Context, userID int64, gatewayAddr string) error {
	return s.Client.Set(ctx, routingKey(userID), gatewayAddr, 0).Err()
}

func (s *Store) GetRoute(ctx context.Context, userID int64) (string, error) {
	return s.Client.Get(ctx, routingKey(userID)).Result()
}

func (s *Store) DelRoute(ctx context.Context, userID int64) error {
	return s.Client.Del(ctx, routingKey(userID)).Err()
}
```

- [ ] **步骤 3：创建 pkg/redisstore/members.go**

```go
package redisstore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func membersKey(convID int64) string {
	return fmt.Sprintf("conv:%d:members", convID)
}

func (s *Store) GetMembers(ctx context.Context, convID int64) ([]int64, error) {
	vals, err := s.Client.SMembers(ctx, membersKey(convID)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, redis.Nil
	}
	ids := make([]int64, 0, len(vals))
	for _, v := range vals {
		id, _ := strconv.ParseInt(v, 10, 64)
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Store) SetMembers(ctx context.Context, convID int64, userIDs []int64) error {
	key := membersKey(convID)
	pipe := s.Client.Pipeline()
	pipe.Del(ctx, key)
	if len(userIDs) > 0 {
		members := make([]interface{}, len(userIDs))
		for i, id := range userIDs {
			members[i] = id
		}
		pipe.SAdd(ctx, key, members...)
	}
	_, err := pipe.Exec(ctx)
	return err
}
```

- [ ] **步骤 4：验证编译通过**

```bash
go build ./pkg/redisstore/
```

预期：无错误。

- [ ] **步骤 5：提交**

```bash
git add pkg/redisstore/
git commit -m "feat: Redis 存储包（路由 + 成员缓存）"
```

---

## Task 6：Kafka 生产者与消费者

**文件：**
- 创建：`pkg/kafka/producer.go`
- 创建：`pkg/kafka/consumer.go`

- [ ] **步骤 1：创建 pkg/kafka/producer.go**

```go
package kafka

import (
	"encoding/json"

	"github.com/IBM/sarama"
)

const TopicMessageFanout = "message_fanout"

type FanoutEvent struct {
	MessageID      int64  `json:"message_id"`
	ConversationID int64  `json:"conversation_id"`
	FromID         int64  `json:"from_id"`
	FromUsername   string `json:"from_username"`
	Content        string `json:"content"`
	CreatedAt      int64  `json:"created_at"`
}

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

func (p *Producer) PublishFanout(event *FanoutEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, _, err = p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: TopicMessageFanout,
		Value: sarama.ByteEncoder(data),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
```

- [ ] **步骤 2：创建 pkg/kafka/consumer.go**

```go
package kafka

import (
	"context"
	"encoding/json"
	"log"

	"github.com/IBM/sarama"
)

type Handler func(ctx context.Context, event *FanoutEvent) error

type Consumer struct {
	group   sarama.ConsumerGroup
	handler Handler
	topic   string
}

func NewConsumer(brokers []string, groupID string, handler Handler) (*Consumer, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	g, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: g, handler: handler, topic: TopicMessageFanout}, nil
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		if err := c.group.Consume(ctx, []string{c.topic}, c); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var event FanoutEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("kafka: unmarshal error: %v", err)
			session.MarkMessage(msg, "")
			continue
		}
		if err := c.handler(session.Context(), &event); err != nil {
			log.Printf("kafka: handler error: %v", err)
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.group.Close()
}
```

- [ ] **步骤 3：验证编译通过**

```bash
go build ./pkg/kafka/
```

预期：无错误。

- [ ] **步骤 4：提交**

```bash
git add pkg/kafka/
git commit -m "feat: Kafka 生产者与消费者包"
```

---

## Task 7：Logic 服务

**文件：**
- 创建：`logic/main.go`
- 创建：`logic/server.go`
- 创建：`logic/auth.go`
- 创建：`logic/message.go`
- 创建：`logic/sync.go`

- [ ] **步骤 1：创建 logic/server.go**

```go
package main

import (
	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type Server struct {
	pb.UnimplementedLogicServiceServer
	cfg      *config.Config
	db       *model.DB
	redis    *redisstore.Store
	producer *kafka.Producer
}

func NewServer(cfg *config.Config, db *model.DB, redis *redisstore.Store, producer *kafka.Producer) *Server {
	return &Server{cfg: cfg, db: db, redis: redis, producer: producer}
}
```

- [ ] **步骤 2：创建 logic/auth.go**

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	pb "im-service/proto"
)

func (s *Server) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	user, err := s.db.GetUserByUsername(ctx, req.Username)
	if err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, fmt.Errorf("invalid credentials")
	}
	token, err := s.issueToken(user.ID)
	if err != nil {
		return nil, err
	}
	return &pb.LoginResponse{Token: token, UserId: user.ID}, nil
}

func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	id, err := s.db.CreateUser(ctx, req.Username, string(hash))
	if err != nil {
		return nil, fmt.Errorf("username already exists")
	}
	token, err := s.issueToken(id)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{UserId: id, Token: token}, nil
}

func (s *Server) issueToken(userID int64) (string, error) {
	claims := jwt.MapClaims{
		"uid": userID,
		"exp": time.Now().Add(72 * time.Hour).Unix(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(s.cfg.JWTSecret))
}

func (s *Server) parseToken(tokenStr string) (int64, error) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return []byte(s.cfg.JWTSecret), nil
	})
	if err != nil || !token.Valid {
		return 0, fmt.Errorf("invalid token")
	}
	claims := token.Claims.(jwt.MapClaims)
	uid, ok := claims["uid"].(float64)
	if !ok {
		return 0, fmt.Errorf("invalid token claims")
	}
	return int64(uid), nil
}
```

- [ ] **步骤 3：创建 logic/message.go**

```go
package main

import (
	"context"
	"fmt"

	"im-service/pkg/kafka"
	pb "im-service/proto"
)

func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	ok, err := s.db.IsMember(ctx, req.ConversationId, uid)
	if err != nil || !ok {
		return nil, fmt.Errorf("not a member of this conversation")
	}
	// 获取用户名用于推送显示
	var username string
	s.db.Pool.QueryRow(ctx, "SELECT username FROM users WHERE id=$1", uid).Scan(&username)

	msg, err := s.db.CreateMessage(ctx, req.ConversationId, uid, req.Content)
	if err != nil {
		return nil, err
	}
	// 发布到 Kafka，由 Fanout 负责扩散
	_ = s.producer.PublishFanout(&kafka.FanoutEvent{
		MessageID:      msg.ID,
		ConversationID: msg.ConversationID,
		FromID:         uid,
		FromUsername:   username,
		Content:        msg.Content,
		CreatedAt:      msg.CreatedAt.UnixMilli(),
	})
	return &pb.SendMessageResponse{MessageId: msg.ID, CreatedAt: msg.CreatedAt.UnixMilli()}, nil
}

func (s *Server) CreateDM(ctx context.Context, req *pb.CreateDMRequest) (*pb.CreateDMResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	// 检查 DM 是否已存在
	convID, err := s.db.FindDMConversation(ctx, uid, req.PeerId)
	if err == nil {
		return &pb.CreateDMResponse{ConversationId: convID}, nil
	}
	convID, err = s.db.CreateConversation(ctx, "dm")
	if err != nil {
		return nil, err
	}
	_ = s.db.AddConversationMember(ctx, convID, uid)
	_ = s.db.AddConversationMember(ctx, convID, req.PeerId)
	return &pb.CreateDMResponse{ConversationId: convID}, nil
}

func (s *Server) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	convID, err := s.db.CreateConversation(ctx, "group")
	if err != nil {
		return nil, err
	}
	groupID, err := s.db.CreateGroup(ctx, convID, req.Name, uid)
	if err != nil {
		return nil, err
	}
	// 添加所有成员
	_ = s.db.AddConversationMember(ctx, convID, uid)
	for _, mid := range req.MemberIds {
		_ = s.db.AddConversationMember(ctx, convID, mid)
	}
	// 缓存群成员到 Redis
	allMembers := append([]int64{uid}, req.MemberIds...)
	_ = s.redis.SetMembers(ctx, convID, allMembers)
	return &pb.CreateGroupResponse{GroupId: groupID, ConversationId: convID}, nil
}
```

- [ ] **步骤 4：创建 logic/sync.go**

```go
package main

import (
	"context"
	"time"

	pb "im-service/proto"
)

func (s *Server) Sync(ctx context.Context, req *pb.SyncRequest) (*pb.SyncResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	since := time.UnixMilli(req.LastSyncAt)
	convs, err := s.db.GetUpdatedConversations(ctx, uid, since)
	if err != nil {
		return nil, err
	}
	resp := &pb.SyncResponse{}
	for _, uc := range convs {
		resp.Conversations = append(resp.Conversations, &pb.ConversationState{
			ConversationId: uc.ConversationID,
			LastMsgId:      uc.LastMsgID,
			UnreadCount:    uc.UnreadCount,
			UpdatedAt:      uc.UpdatedAt.UnixMilli(),
		})
		// 获取每个更新会话的最近消息
		msgs, err := s.db.GetMessagesSince(ctx, uc.ConversationID, uc.LastMsgID-50, 50)
		if err == nil {
			for _, m := range msgs {
				resp.Messages = append(resp.Messages, &pb.MessageItem{
					Id:             m.ID,
					ConversationId: m.ConversationID,
					FromId:         m.FromID,
					FromUsername:   m.FromUsername,
					Content:        m.Content,
					CreatedAt:      m.CreatedAt.UnixMilli(),
				})
			}
		}
	}
	_ = s.db.UpdateSyncTime(ctx, uid)
	return resp, nil
}
```

- [ ] **步骤 5：创建 logic/main.go**

```go
package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	db, err := model.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	redis := redisstore.New(cfg.RedisAddr)
	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	srv := NewServer(cfg, db, redis, producer)
	lis, err := net.Listen("tcp", cfg.LogicGRPC)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLogicServiceServer(grpcServer, srv)
	log.Printf("logic 服务监听 %s", cfg.LogicGRPC)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
```

- [ ] **步骤 6：验证编译通过**

```bash
go build ./logic/
```

预期：无错误。

- [ ] **步骤 7：提交**

```bash
git add logic/
git commit -m "feat: Logic 服务（认证、消息、同步、建群）"
```

---

## Task 8：Gateway 服务

**文件：**
- 创建：`gateway/main.go`
- 创建：`gateway/server.go`
- 创建：`gateway/hub.go`
- 创建：`gateway/handler.go`

- [ ] **步骤 1：创建 gateway/hub.go**

```go
package main

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Hub struct {
	mu    sync.RWMutex
	conns map[int64]*websocket.Conn
}

func NewHub() *Hub {
	return &Hub{conns: make(map[int64]*websocket.Conn)}
}

func (h *Hub) Add(userID int64, conn *websocket.Conn) {
	h.mu.Lock()
	h.conns[userID] = conn
	h.mu.Unlock()
}

func (h *Hub) Remove(userID int64) {
	h.mu.Lock()
	delete(h.conns, userID)
	h.mu.Unlock()
}

func (h *Hub) Get(userID int64) *websocket.Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[userID]
}
```

- [ ] **步骤 2：创建 gateway/handler.go**

处理 WebSocket 上行消息，根据 type 字段分发到对应的 Logic gRPC 调用。

```go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
	pb "im-service/proto"
)

type WSMessage struct {
	Type           string  `json:"type"`
	Token          string  `json:"token,omitempty"`
	ConversationID int64   `json:"conversation_id,omitempty"`
	Content        string  `json:"content,omitempty"`
	LastSyncAt     int64   `json:"last_sync_at,omitempty"`
	PeerID         int64   `json:"peer_id,omitempty"`
	GroupName      string  `json:"group_name,omitempty"`
	MemberIDs      []int64 `json:"member_ids,omitempty"`
}

func (s *GatewayServer) handleWS(conn *websocket.Conn, userID int64) {
	defer func() {
		s.hub.Remove(userID)
		s.redis.DelRoute(context.Background(), userID)
		conn.Close()
	}()

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var msg WSMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		ctx := context.Background()
		switch msg.Type {
		case "send":
			resp, err := s.logic.SendMessage(ctx, &pb.SendMessageRequest{
				Token: msg.Token, ConversationId: msg.ConversationID, Content: msg.Content,
			})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "sent", "message_id": resp.MessageId})
		case "sync":
			resp, err := s.logic.Sync(ctx, &pb.SyncRequest{Token: msg.Token, LastSyncAt: msg.LastSyncAt})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "sync_resp", "data": resp})
		case "create_dm":
			resp, err := s.logic.CreateDM(ctx, &pb.CreateDMRequest{Token: msg.Token, PeerId: msg.PeerID})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "dm_created", "conversation_id": resp.ConversationId})
		case "create_group":
			resp, err := s.logic.CreateGroup(ctx, &pb.CreateGroupRequest{
				Token: msg.Token, Name: msg.GroupName, MemberIds: msg.MemberIDs,
			})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "group_created", "group_id": resp.GroupId, "conversation_id": resp.ConversationId})
		}
	}
}

func (s *GatewayServer) writeJSON(conn *websocket.Conn, v interface{}) {
	data, _ := json.Marshal(v)
	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		log.Printf("ws write error: %v", err)
	}
}
```

- [ ] **步骤 3：创建 gateway/server.go**

包含：gRPC Push 接口实现、HTTP 路由（WS 升级 + REST 代理 + 静态文件）、连接认证。

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"im-service/pkg/config"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type GatewayServer struct {
	pb.UnimplementedGatewayServiceServer
	cfg      *config.Config
	hub      *Hub
	redis    *redisstore.Store
	logic    pb.LogicServiceClient
	upgrader websocket.Upgrader
}

func NewGatewayServer(cfg *config.Config, redis *redisstore.Store, logic pb.LogicServiceClient) *GatewayServer {
	return &GatewayServer{
		cfg:   cfg,
		hub:   NewHub(),
		redis: redis,
		logic: logic,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

// Push 被 Fanout Worker 通过 gRPC 调用，推送消息给在线用户
func (s *GatewayServer) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	conn := s.hub.Get(req.UserId)
	if conn == nil {
		return &pb.PushResponse{}, nil
	}
	conn.WriteMessage(websocket.TextMessage, req.Payload)
	return &pb.PushResponse{}, nil
}

func (s *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/ws":
		s.handleWSUpgrade(w, r)
	case r.URL.Path == "/api/login" && r.Method == "POST":
		var req pb.LoginRequest
		json.NewDecoder(r.Body).Decode(&req)
		resp, err := s.logic.Login(r.Context(), &req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(resp)
	case r.URL.Path == "/api/register" && r.Method == "POST":
		var req pb.RegisterRequest
		json.NewDecoder(r.Body).Decode(&req)
		resp, err := s.logic.Register(r.Context(), &req)
		w.Header().Set("Content-Type", "application/json")
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(resp)
	default:
		http.FileServer(http.Dir("web")).ServeHTTP(w, r)
	}
}

func (s *GatewayServer) handleWSUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// 第一条消息必须是认证消息
	_, data, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}
	var authMsg struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(data, &authMsg); err != nil || authMsg.Token == "" {
		conn.Close()
		return
	}
	uid := parseUIDFromToken(authMsg.Token, s.cfg.JWTSecret)
	if uid == 0 {
		conn.Close()
		return
	}
	s.hub.Add(uid, conn)
	s.redis.SetRoute(context.Background(), uid, s.cfg.GatewayGRPC)
	log.Printf("用户 %d 已连接", uid)
	// 发送初始同步数据
	resp, _ := s.logic.Sync(context.Background(), &pb.SyncRequest{Token: authMsg.Token, LastSyncAt: 0})
	if resp != nil {
		s.writeJSON(conn, map[string]interface{}{"type": "sync_resp", "data": resp})
	}
	s.handleWS(conn, uid)
}

func parseUIDFromToken(tokenStr, secret string) int64 {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})
	if err != nil || !token.Valid {
		return 0
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return 0
	}
	uid, ok := claims["uid"].(float64)
	if !ok {
		return 0
	}
	return int64(uid)
}

func (s *GatewayServer) StartGRPC() {
	lis, err := net.Listen("tcp", s.cfg.GatewayGRPC)
	if err != nil {
		log.Fatalf("gateway grpc listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGatewayServiceServer(grpcServer, s)
	log.Printf("gateway gRPC 监听 %s", s.cfg.GatewayGRPC)
	go grpcServer.Serve(lis)
}
```

- [ ] **步骤 4：创建 gateway/main.go**

```go
package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/config"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

func main() {
	cfg := config.Load()
	redis := redisstore.New(cfg.RedisAddr)

	logicConn, err := grpc.NewClient(cfg.LogicGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect logic: %v", err)
	}
	defer logicConn.Close()
	logicClient := pb.NewLogicServiceClient(logicConn)

	srv := NewGatewayServer(cfg, redis, logicClient)
	srv.StartGRPC()

	log.Printf("gateway HTTP/WS 监听 %s", cfg.WebSocketAddr)
	if err := http.ListenAndServe(cfg.WebSocketAddr, srv); err != nil {
		log.Fatalf("http: %v", err)
	}
}
```

- [ ] **步骤 5：验证编译通过**

```bash
go build ./gateway/
```

预期：无错误。

- [ ] **步骤 6：提交**

```bash
git add gateway/
git commit -m "feat: Gateway 服务（WebSocket + gRPC Push）"
```

---

## Task 9：Fanout Worker

**文件：**
- 创建：`fanout/main.go`
- 创建：`fanout/worker.go`
- 创建：`fanout/fanout.go`

- [ ] **步骤 1：创建 fanout/fanout.go**

核心扩散逻辑：查成员列表 → worker pool 并行更新 user_conversations → 推送在线用户。

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type FanoutProcessor struct {
	db       *model.DB
	redis    *redisstore.Store
	gateways map[string]pb.GatewayServiceClient
	mu       sync.RWMutex
}

func NewFanoutProcessor(db *model.DB, redis *redisstore.Store) *FanoutProcessor {
	return &FanoutProcessor{
		db:       db,
		redis:    redis,
		gateways: make(map[string]pb.GatewayServiceClient),
	}
}

func (f *FanoutProcessor) Handle(ctx context.Context, event *kafka.FanoutEvent) error {
	// 获取成员列表（Redis 缓存优先，miss 则查 DB 并回填缓存）
	members, err := f.redis.GetMembers(ctx, event.ConversationID)
	if err == redis.Nil {
		members, err = f.db.GetConversationMembers(ctx, event.ConversationID)
		if err != nil {
			return err
		}
		_ = f.redis.SetMembers(ctx, event.ConversationID, members)
	} else if err != nil {
		return err
	}

	// 构建推送 payload
	payload, _ := json.Marshal(map[string]interface{}{
		"type":            "new_message",
		"message_id":      event.MessageID,
		"conversation_id": event.ConversationID,
		"from_id":         event.FromID,
		"from_username":   event.FromUsername,
		"content":         event.Content,
		"created_at":      event.CreatedAt,
	})

	// Worker pool 并行扩散（上限 50 并发）
	sem := make(chan struct{}, 50)
	var wg sync.WaitGroup
	for _, uid := range members {
		wg.Add(1)
		sem <- struct{}{}
		go func(userID int64) {
			defer func() { <-sem; wg.Done() }()
			// 更新 user_conversations（无论在线离线都更新）
			isSender := userID == event.FromID
			if err := f.db.UpsertUserConversation(ctx, userID, event.ConversationID, event.MessageID, isSender); err != nil {
				log.Printf("fanout: upsert uc error uid=%d: %v", userID, err)
			}
			// 查路由，在线则推送
			route, err := f.redis.GetRoute(ctx, userID)
			if err != nil {
				return // 离线，跳过推送
			}
			client := f.getGatewayClient(route)
			if client == nil {
				return
			}
			_, err = client.Push(ctx, &pb.PushRequest{UserId: userID, Payload: payload})
			if err != nil {
				log.Printf("fanout: push to %d via %s failed: %v", userID, route, err)
			}
		}(uid)
	}
	wg.Wait()
	return nil
}

func (f *FanoutProcessor) getGatewayClient(addr string) pb.GatewayServiceClient {
	f.mu.RLock()
	c, ok := f.gateways[addr]
	f.mu.RUnlock()
	if ok {
		return c
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.gateways[addr]; ok {
		return c
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("fanout: dial gateway %s: %v", addr, err)
		return nil
	}
	client := pb.NewGatewayServiceClient(conn)
	f.gateways[addr] = client
	return client
}
```

- [ ] **步骤 2：创建 fanout/worker.go**

（该文件仅做 grpc 拨号辅助，已内联到 fanout.go，此文件留空占位或删除均可）

```go
package main
```

- [ ] **步骤 3：创建 fanout/main.go**

```go
package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := model.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	redisStore := redisstore.New(cfg.RedisAddr)
	processor := NewFanoutProcessor(db, redisStore)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, "fanout-group", processor.Handle)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("fanout worker 已启动")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer: %v", err)
	}
	log.Println("fanout worker 已停止")
}
```

- [ ] **步骤 4：验证编译通过**

```bash
go build ./fanout/
```

预期：无错误。

- [ ] **步骤 5：提交**

```bash
git add fanout/
git commit -m "feat: Fanout Worker（Kafka 消费 + 写扩散 + 推送）"
```

---

## Task 10：Web 前端

**文件：**
- 创建：`web/index.html`

- [ ] **步骤 1：创建 web/index.html**

单页应用：左侧会话列表 + 右侧聊天记录，支持登录/注册、创建单聊/群聊、发消息、实时接收。

```html
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>IM Service</title>
<style>
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: -apple-system, sans-serif; height:100vh; display:flex; flex-direction:column; }
#auth { padding:20px; text-align:center; }
#auth input, #auth button { margin:4px; padding:8px; }
#app { display:none; flex:1; flex-direction:row; }
#sidebar { width:260px; border-right:1px solid #ddd; overflow-y:auto; }
#sidebar .conv { padding:12px; cursor:pointer; border-bottom:1px solid #eee; }
#sidebar .conv.active { background:#e3f2fd; }
#sidebar .conv .unread { background:#f44336; color:#fff; border-radius:10px; padding:2px 6px; font-size:12px; float:right; }
#chat { flex:1; display:flex; flex-direction:column; }
#messages { flex:1; overflow-y:auto; padding:12px; }
#messages .msg { margin:4px 0; }
#messages .msg .name { font-weight:bold; margin-right:6px; }
#input-bar { display:flex; padding:8px; border-top:1px solid #ddd; }
#input-bar input { flex:1; padding:8px; }
#input-bar button { padding:8px 16px; }
#actions { padding:8px; border-bottom:1px solid #ddd; }
#actions button { margin:4px; padding:6px 12px; }
</style>
</head>
<body>
<div id="auth">
  <h2>IM Service</h2>
  <div>
    <input id="username" placeholder="用户名">
    <input id="password" type="password" placeholder="密码">
  </div>
  <div>
    <button onclick="login()">登录</button>
    <button onclick="register()">注册</button>
  </div>
</div>
<div id="app">
  <div id="sidebar"></div>
  <div id="chat">
    <div id="actions">
      <button onclick="createDM()">新建私聊</button>
      <button onclick="createGroup()">新建群聊</button>
    </div>
    <div id="messages"></div>
    <div id="input-bar">
      <input id="msg-input" placeholder="输入消息..." onkeydown="if(event.key==='Enter')sendMsg()">
      <button onclick="sendMsg()">发送</button>
    </div>
  </div>
</div>
<script>
let ws, token, userId, conversations=[], currentConvId=null, messagesByConv={};

function login() {
  fetch('/api/login', {method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({username:document.getElementById('username').value, password:document.getElementById('password').value})
  }).then(r=>r.json()).then(d=>{
    if(d.token){token=d.token;userId=d.user_id;connectWS();}
    else alert(d.error||'登录失败');
  });
}

function register() {
  fetch('/api/register', {method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({username:document.getElementById('username').value, password:document.getElementById('password').value})
  }).then(r=>r.json()).then(d=>{
    if(d.token){token=d.token;userId=d.user_id;connectWS();}
    else alert(d.error||'注册失败');
  });
}

function connectWS() {
  document.getElementById('auth').style.display='none';
  document.getElementById('app').style.display='flex';
  ws = new WebSocket(`ws://${location.host}/ws`);
  ws.onopen = ()=>{ ws.send(JSON.stringify({token})); };
  ws.onmessage = (e)=>{
    const msg = JSON.parse(e.data);
    if(msg.type==='sync_resp' && msg.data) handleSync(msg.data);
    else if(msg.type==='new_message') handleNewMessage(msg);
    else if(msg.type==='dm_created') { selectConv(msg.conversation_id); sync(); }
    else if(msg.type==='group_created') { selectConv(msg.conversation_id); sync(); }
  };
}

function sync() { ws.send(JSON.stringify({type:'sync',token,last_sync_at:0})); }

function handleSync(data) {
  if(data.conversations) data.conversations.forEach(c=>{
    const cid = c.conversationId||c.conversation_id;
    let existing = conversations.find(x=>x.conversation_id===cid);
    const conv = {conversation_id:cid, unread_count:c.unreadCount||c.unread_count||0, last_msg_id:c.lastMsgId||c.last_msg_id};
    if(existing) Object.assign(existing,conv); else conversations.push(conv);
  });
  if(data.messages) data.messages.forEach(m=>{
    const cid = m.conversationId||m.conversation_id;
    if(!messagesByConv[cid]) messagesByConv[cid]=[];
    if(!messagesByConv[cid].find(x=>x.id===m.id))
      messagesByConv[cid].push({id:m.id, from:m.fromUsername||m.from_username, content:m.content});
  });
  renderSidebar(); renderMessages();
}

function handleNewMessage(m) {
  const cid = m.conversation_id;
  if(!messagesByConv[cid]) messagesByConv[cid]=[];
  messagesByConv[cid].push({id:m.message_id, from:m.from_username, content:m.content});
  let conv = conversations.find(x=>x.conversation_id===cid);
  if(!conv){conv={conversation_id:cid,unread_count:0};conversations.push(conv);}
  if(cid!==currentConvId) conv.unread_count=(conv.unread_count||0)+1;
  renderSidebar(); if(cid===currentConvId) renderMessages();
}

function renderSidebar() {
  document.getElementById('sidebar').innerHTML = conversations.map(c =>
    `<div class="conv ${c.conversation_id===currentConvId?'active':''}" onclick="selectConv(${c.conversation_id})">
      会话 #${c.conversation_id}${c.unread_count?`<span class="unread">${c.unread_count}</span>`:''}
    </div>`
  ).join('');
}

function selectConv(id) {
  currentConvId = id;
  let c = conversations.find(x=>x.conversation_id===id);
  if(c) c.unread_count = 0;
  renderSidebar(); renderMessages();
}

function renderMessages() {
  const msgs = messagesByConv[currentConvId]||[];
  document.getElementById('messages').innerHTML = msgs.map(m =>
    `<div class="msg"><span class="name">${m.from}:</span>${m.content}</div>`
  ).join('');
  document.getElementById('messages').scrollTop = 999999;
}

function sendMsg() {
  const input = document.getElementById('msg-input');
  if(!input.value || !currentConvId) return;
  ws.send(JSON.stringify({type:'send', token, conversation_id:currentConvId, content:input.value}));
  input.value = '';
}

function createDM() {
  const id = prompt('输入对方用户 ID:');
  if(id) ws.send(JSON.stringify({type:'create_dm', token, peer_id:parseInt(id)}));
}

function createGroup() {
  const name = prompt('群组名称:');
  if(!name) return;
  const ids = prompt('成员 ID（逗号分隔）:');
  if(!ids) return;
  ws.send(JSON.stringify({type:'create_group', token, group_name:name, member_ids:ids.split(',').map(Number)}));
}
</script>
</body>
</html>
```

- [ ] **步骤 2：验证 Gateway 仍可编译**

```bash
go build ./gateway/
```

- [ ] **步骤 3：提交**

```bash
git add web/
git commit -m "feat: Web 前端（登录、会话列表、实时聊天）"
```

---

## Task 11：端到端冒烟测试

**文件：** 无新建（手动验证）

- [ ] **步骤 1：启动基础设施**

```bash
docker compose up -d
sleep 5
psql "postgres://im:im@localhost:5432/im" -f migrations/001_init.sql
```

- [ ] **步骤 2：分别启动三个服务**

```bash
# 终端 1：Logic 服务
cd im-service && go run ./logic/

# 终端 2：Fanout Worker
cd im-service && go run ./fanout/

# 终端 3：Gateway 服务
cd im-service && go run ./gateway/
```

- [ ] **步骤 3：测试注册和登录**

打开浏览器访问 `http://localhost:8080`，开两个标签页：
- 标签页 1：注册用户 "alice" / "pass123"
- 标签页 2：注册用户 "bob" / "pass123"

预期：两个标签页都显示应用界面，侧边栏为空。

- [ ] **步骤 4：测试单聊消息**

- 标签页 1（alice）：点"新建私聊"，输入 bob 的用户 ID（2）
- 标签页 1：发送 "hello bob"
- 标签页 2（bob）：侧边栏出现会话，点击后看到 "alice: hello bob"
- 标签页 2：回复 "hi alice"
- 标签页 1：实时收到 "bob: hi alice"

预期：消息通过 WebSocket 实时推送。

- [ ] **步骤 5：测试群聊**

- 标签页 1（alice）：点"新建群聊"，名称 "test-group"，成员 ID 填 "2"
- 标签页 1：发送 "hello group"
- 标签页 2：看到群聊会话并收到消息

预期：群消息扩散正常工作。

- [ ] **步骤 6：最终提交**

```bash
git add -A
git commit -m "feat: IM 服务完成，三个服务 + 前端全部就绪"
```
