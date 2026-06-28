# IM 社交聊天软件 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a production-ready social IM app with friend system, chat (DM + group), and real-time push from scratch.

**Architecture:** Three Go services (Gateway, Logic, Fanout) communicating via gRPC and Kafka, with PostgreSQL for persistence, Redis for routing/caching, and a React SPA frontend. Gateway handles HTTP REST + WebSocket; Logic handles all business logic; Fanout consumes Kafka for write-spread and push.

**Tech Stack:** Go 1.22, gRPC, gorilla/websocket, PostgreSQL, Redis, Kafka, React 19, Vite, Tailwind CSS v4

---

## Task 1: Project Scaffold + Infrastructure

**Files:**
- Create: `im-social/docker-compose.yml`
- Create: `im-social/go.mod`
- Create: `im-social/pkg/config/config.go`
- Create: `im-social/migrations/001_init.sql`

- [ ] **Step 1: Create project directory and Go module**

```bash
mkdir -p im-social && cd im-social
go mod init im-social
```

- [ ] **Step 2: Create docker-compose.yml**

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_DB: im_social
      POSTGRES_USER: im
      POSTGRES_PASSWORD: im_pass
    ports:
      - "5432:5432"
    volumes:
      - ./migrations:/docker-entrypoint-initdb.d

  redis:
    image: redis:7
    ports:
      - "6379:6379"

  kafka:
    image: bitnami/kafka:3.7.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_CFG_NODE_ID: 0
      KAFKA_CFG_PROCESS_ROLES: controller,broker
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: 0@kafka:9093
      KAFKA_CFG_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_CFG_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
```

- [ ] **Step 3: Create migrations/001_init.sql**

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE TABLE users (
  id BIGSERIAL PRIMARY KEY,
  username VARCHAR(32) UNIQUE NOT NULL,
  password_hash VARCHAR(128) NOT NULL,
  nickname VARCHAR(64),
  avatar_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_users_username_trgm ON users USING gin (username gin_trgm_ops);

CREATE TABLE friendships (
  user_id BIGINT NOT NULL,
  friend_id BIGINT NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'accepted',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, friend_id)
);

CREATE TABLE friend_requests (
  id BIGSERIAL PRIMARY KEY,
  from_id BIGINT NOT NULL,
  to_id BIGINT NOT NULL,
  message VARCHAR(128),
  status VARCHAR(16) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  expired_at TIMESTAMPTZ
);

CREATE TABLE conversations (
  id BIGSERIAL PRIMARY KEY,
  type VARCHAR(8) NOT NULL,
  name VARCHAR(64),
  owner_id BIGINT,
  avatar_url TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE conversation_members (
  conversation_id BIGINT NOT NULL REFERENCES conversations(id),
  user_id BIGINT NOT NULL,
  role VARCHAR(8) NOT NULL DEFAULT 'member',
  joined_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE messages (
  id BIGSERIAL PRIMARY KEY,
  conversation_id BIGINT NOT NULL REFERENCES conversations(id),
  from_id BIGINT NOT NULL,
  msg_type VARCHAR(16) NOT NULL DEFAULT 'text',
  content TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_messages_conv_id ON messages (conversation_id, id DESC);

CREATE TABLE user_conversations (
  user_id BIGINT NOT NULL,
  conversation_id BIGINT NOT NULL REFERENCES conversations(id),
  last_msg_id BIGINT DEFAULT 0,
  last_read_msg_id BIGINT DEFAULT 0,
  muted BOOLEAN DEFAULT FALSE,
  updated_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (user_id, conversation_id)
);
```

- [ ] **Step 4: Create pkg/config/config.go**

```go
package config

import "os"

type Config struct {
	PostgresDSN  string
	RedisAddr    string
	KafkaBroker  string
	JWTSecret    string
	GatewayHTTP  string
	GatewayGRPC  string
	LogicGRPC    string
}

func Load() *Config {
	return &Config{
		PostgresDSN:  envOr("POSTGRES_DSN", "postgres://im:im_pass@localhost:5432/im_social?sslmode=disable"),
		RedisAddr:    envOr("REDIS_ADDR", "localhost:6379"),
		KafkaBroker:  envOr("KAFKA_BROKER", "localhost:9092"),
		JWTSecret:    envOr("JWT_SECRET", "im-social-secret-key"),
		GatewayHTTP:  envOr("GATEWAY_HTTP", ":8080"),
		GatewayGRPC:  envOr("GATEWAY_GRPC", ":9001"),
		LogicGRPC:    envOr("LOGIC_GRPC", ":9002"),
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
```

- [ ] **Step 5: Start infrastructure and verify**

```bash
docker-compose up -d
psql "postgres://im:im_pass@localhost:5432/im_social" -c "\dt"
```

Expected: All 6 tables listed.

- [ ] **Step 6: Commit**

```bash
git init
git add .
git commit -m "feat: project scaffold, docker-compose, migrations, config"
```

---

## Task 2: Proto Definitions + Code Generation

**Files:**
- Create: `im-social/proto/logic.proto`
- Create: `im-social/proto/gateway.proto`
- Create: `im-social/proto/generate.sh`

- [ ] **Step 1: Create proto/logic.proto**

```protobuf
syntax = "proto3";
package im;
option go_package = "im-social/proto";

// === Auth ===
message RegisterRequest { string username = 1; string password = 2; }
message RegisterResponse { int64 user_id = 1; string access_token = 2; string refresh_token = 3; }

message LoginRequest { string username = 1; string password = 2; }
message LoginResponse { int64 user_id = 1; string access_token = 2; string refresh_token = 3; }

message RefreshRequest { string refresh_token = 1; }
message RefreshResponse { string access_token = 1; }

// === User ===
message GetUserRequest { int64 user_id = 1; }
message UserProfile { int64 id = 1; string username = 2; string nickname = 3; string avatar_url = 4; }

message UpdateProfileRequest { string token = 1; string nickname = 2; string avatar_url = 3; }
message UpdateProfileResponse {}

// === Relation (Friends) ===
message FriendListRequest { string token = 1; }
message FriendListResponse { repeated UserProfile friends = 1; }

message GenQRCodeRequest { string token = 1; }
message GenQRCodeResponse { string code = 1; string url = 2; }

message AddFriendRequest { string token = 1; string code = 2; }
message AddFriendResponse { int64 request_id = 1; }

message FriendRequestListRequest { string token = 1; }
message FriendRequestItem { int64 id = 1; int64 from_id = 2; string from_username = 3; string message = 4; string status = 5; int64 created_at = 6; }
message FriendRequestListResponse { repeated FriendRequestItem requests = 1; }

message AcceptFriendRequest { string token = 1; int64 request_id = 2; }
message AcceptFriendResponse { int64 conversation_id = 1; }

message RejectFriendRequest { string token = 1; int64 request_id = 2; }
message RejectFriendResponse {}

message DeleteFriendRequest { string token = 1; int64 friend_id = 2; }
message DeleteFriendResponse {}

message BlockFriendRequest { string token = 1; int64 friend_id = 2; }
message BlockFriendResponse {}

// === Conversation ===
message ConversationListRequest { string token = 1; }
message ConversationItem {
  int64 id = 1; string type = 2; string name = 3; int64 last_msg_id = 4;
  int64 last_read_msg_id = 5; bool muted = 6; int64 updated_at = 7;
  string last_msg_content = 8; string last_msg_from = 9; int32 unread_count = 10;
}
message ConversationListResponse { repeated ConversationItem conversations = 1; }

message CreateGroupRequest { string token = 1; string name = 2; repeated int64 member_ids = 3; }
message CreateGroupResponse { int64 conversation_id = 1; }

message ConversationMembersRequest { string token = 1; int64 conversation_id = 2; }
message ConversationMembersResponse { repeated UserProfile members = 1; }

message InviteMembersRequest { string token = 1; int64 conversation_id = 2; repeated int64 user_ids = 3; }
message InviteMembersResponse {}

message RemoveMemberRequest { string token = 1; int64 conversation_id = 2; int64 user_id = 3; }
message RemoveMemberResponse {}

// === Message ===
message SendMessageRequest { string token = 1; int64 conversation_id = 2; string content = 3; string msg_type = 4; }
message SendMessageResponse { int64 message_id = 1; int64 created_at = 2; }

message MessageHistoryRequest { string token = 1; int64 conversation_id = 2; int64 cursor = 3; int32 limit = 4; }
message MessageItem { int64 id = 1; int64 conversation_id = 2; int64 from_id = 3; string from_username = 4; string content = 5; string msg_type = 6; int64 created_at = 7; }
message MessageHistoryResponse { repeated MessageItem messages = 1; int64 next_cursor = 2; }

message MarkReadRequest { string token = 1; int64 conversation_id = 2; int64 msg_id = 3; }
message MarkReadResponse {}

service LogicService {
  // Auth
  rpc Register(RegisterRequest) returns (RegisterResponse);
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Refresh(RefreshRequest) returns (RefreshResponse);
  // User
  rpc GetUser(GetUserRequest) returns (UserProfile);
  rpc UpdateProfile(UpdateProfileRequest) returns (UpdateProfileResponse);
  // Relation
  rpc FriendList(FriendListRequest) returns (FriendListResponse);
  rpc GenQRCode(GenQRCodeRequest) returns (GenQRCodeResponse);
  rpc AddFriend(AddFriendRequest) returns (AddFriendResponse);
  rpc FriendRequestList(FriendRequestListRequest) returns (FriendRequestListResponse);
  rpc AcceptFriend(AcceptFriendRequest) returns (AcceptFriendResponse);
  rpc RejectFriend(RejectFriendRequest) returns (RejectFriendResponse);
  rpc DeleteFriend(DeleteFriendRequest) returns (DeleteFriendResponse);
  rpc BlockFriend(BlockFriendRequest) returns (BlockFriendResponse);
  // Conversation
  rpc ConversationList(ConversationListRequest) returns (ConversationListResponse);
  rpc CreateGroup(CreateGroupRequest) returns (CreateGroupResponse);
  rpc ConversationMembers(ConversationMembersRequest) returns (ConversationMembersResponse);
  rpc InviteMembers(InviteMembersRequest) returns (InviteMembersResponse);
  rpc RemoveMember(RemoveMemberRequest) returns (RemoveMemberResponse);
  // Message
  rpc SendMessage(SendMessageRequest) returns (SendMessageResponse);
  rpc MessageHistory(MessageHistoryRequest) returns (MessageHistoryResponse);
  rpc MarkRead(MarkReadRequest) returns (MarkReadResponse);
}
```

- [ ] **Step 2: Create proto/gateway.proto**

```protobuf
syntax = "proto3";
package im;
option go_package = "im-social/proto";

message PushRequest { int64 user_id = 1; bytes payload = 2; }
message PushResponse {}

service GatewayService {
  rpc Push(PushRequest) returns (PushResponse);
}
```

- [ ] **Step 3: Create proto/generate.sh**

```bash
#!/bin/bash
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/logic.proto proto/gateway.proto
```

- [ ] **Step 4: Generate Go code**

```bash
chmod +x proto/generate.sh
./proto/generate.sh
```

Expected: `proto/logic.pb.go`, `proto/logic_grpc.pb.go`, `proto/gateway.pb.go`, `proto/gateway_grpc.pb.go` generated.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: gRPC proto definitions and generated code"
```

---

## Task 3: Shared Packages (Redis Store, Kafka, Models)

**Files:**
- Create: `im-social/pkg/redisstore/store.go`
- Create: `im-social/pkg/kafka/producer.go`
- Create: `im-social/pkg/kafka/consumer.go`

- [ ] **Step 1: Create pkg/redisstore/store.go**

```go
package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Store struct {
	client *redis.Client
}

func New(addr string) *Store {
	return &Store{client: redis.NewClient(&redis.Options{Addr: addr})}
}

// Route: which gateway a user is connected to
func (s *Store) SetRoute(ctx context.Context, userID int64, gatewayAddr string) error {
	return s.client.Set(ctx, fmt.Sprintf("user:%d:gateway", userID), gatewayAddr, 24*time.Hour).Err()
}

func (s *Store) GetRoute(ctx context.Context, userID int64) (string, error) {
	return s.client.Get(ctx, fmt.Sprintf("user:%d:gateway", userID)).Result()
}

func (s *Store) DelRoute(ctx context.Context, userID int64) error {
	return s.client.Del(ctx, fmt.Sprintf("user:%d:gateway", userID)).Err()
}

// Conv members cache
func (s *Store) SetConvMembers(ctx context.Context, convID int64, members []int64, ttl time.Duration) error {
	data, _ := json.Marshal(members)
	return s.client.Set(ctx, fmt.Sprintf("conv:%d:members", convID), data, ttl).Err()
}

func (s *Store) GetConvMembers(ctx context.Context, convID int64) ([]int64, error) {
	data, err := s.client.Get(ctx, fmt.Sprintf("conv:%d:members", convID)).Bytes()
	if err != nil {
		return nil, err
	}
	var members []int64
	return members, json.Unmarshal(data, &members)
}

func (s *Store) DelConvMembers(ctx context.Context, convID int64) error {
	return s.client.Del(ctx, fmt.Sprintf("conv:%d:members", convID)).Err()
}

// Add-friend token
func (s *Store) SetFriendCode(ctx context.Context, code string, userID int64) error {
	return s.client.Set(ctx, fmt.Sprintf("add_friend:%s", code), userID, 5*time.Minute).Err()
}

func (s *Store) GetFriendCode(ctx context.Context, code string) (int64, error) {
	return s.client.Get(ctx, fmt.Sprintf("add_friend:%s", code)).Int64()
}

// Token blacklist
func (s *Store) BlacklistToken(ctx context.Context, jti string, ttl time.Duration) error {
	return s.client.Set(ctx, fmt.Sprintf("refresh_blacklist:%s", jti), 1, ttl).Err()
}

func (s *Store) IsBlacklisted(ctx context.Context, jti string) (bool, error) {
	n, err := s.client.Exists(ctx, fmt.Sprintf("refresh_blacklist:%s", jti)).Result()
	return n > 0, err
}
```

- [ ] **Step 2: Create pkg/kafka/producer.go**

```go
package kafka

import (
	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer(broker, topic string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	p, err := sarama.NewSyncProducer([]string{broker}, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p, topic: topic}, nil
}

func (p *Producer) Send(key string, value []byte) error {
	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})
	return err
}

func (p *Producer) Close() error {
	return p.producer.Close()
}
```

- [ ] **Step 3: Create pkg/kafka/consumer.go**

```go
package kafka

import (
	"github.com/IBM/sarama"
)

type MessageHandler func(key, value []byte) error

type Consumer struct {
	consumer sarama.Consumer
	topic    string
}

func NewConsumer(broker, topic string) (*Consumer, error) {
	cfg := sarama.NewConfig()
	c, err := sarama.NewConsumer([]string{broker}, cfg)
	if err != nil {
		return nil, err
	}
	return &Consumer{consumer: c, topic: topic}, nil
}

func (c *Consumer) Consume(handler MessageHandler) error {
	partitions, err := c.consumer.Partitions(c.topic)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		pc, err := c.consumer.ConsumePartition(c.topic, p, sarama.OffsetNewest)
		if err != nil {
			return err
		}
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				handler(msg.Key, msg.Value)
			}
		}(pc)
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.consumer.Close()
}
```

- [ ] **Step 4: Install dependencies**

```bash
go get github.com/redis/go-redis/v9
go get github.com/IBM/sarama
go get github.com/golang-jwt/jwt/v5
go get github.com/gorilla/websocket
go get google.golang.org/grpc
go get google.golang.org/protobuf
go get golang.org/x/crypto/bcrypt
go get github.com/lib/pq
go mod tidy
```

- [ ] **Step 5: Verify compilation**

```bash
go build ./...
```

Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: shared packages (redis store, kafka producer/consumer, config)"
```

---

## Task 4: Logic Service — Auth Module

**Files:**
- Create: `im-social/logic/main.go`
- Create: `im-social/logic/server.go`
- Create: `im-social/logic/auth.go`
- Create: `im-social/logic/db/db.go`
- Create: `im-social/logic/db/users.go`

- [ ] **Step 1: Create logic/db/db.go**

```go
package db

import (
	"database/sql"
	_ "github.com/lib/pq"
)

type DB struct {
	conn *sql.DB
}

func New(dsn string) (*DB, error) {
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(50)
	conn.SetMaxIdleConns(10)
	return &DB{conn: conn}, nil
}

func (d *DB) Close() error {
	return d.conn.Close()
}
```

- [ ] **Step 2: Create logic/db/users.go**

```go
package db

import (
	"context"
	"database/sql"
)

type User struct {
	ID           int64
	Username     string
	PasswordHash string
	Nickname     string
	AvatarURL    string
}

func (d *DB) CreateUser(ctx context.Context, username, passwordHash string) (int64, error) {
	var id int64
	err := d.conn.QueryRowContext(ctx,
		`INSERT INTO users (username, password_hash) VALUES ($1, $2) RETURNING id`,
		username, passwordHash).Scan(&id)
	return id, err
}

func (d *DB) GetUserByUsername(ctx context.Context, username string) (*User, error) {
	u := &User{}
	err := d.conn.QueryRowContext(ctx,
		`SELECT id, username, password_hash, COALESCE(nickname,''), COALESCE(avatar_url,'') FROM users WHERE username = $1`,
		username).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Nickname, &u.AvatarURL)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return u, err
}

func (d *DB) GetUserByID(ctx context.Context, id int64) (*User, error) {
	u := &User{}
	err := d.conn.QueryRowContext(ctx,
		`SELECT id, username, password_hash, COALESCE(nickname,''), COALESCE(avatar_url,'') FROM users WHERE id = $1`,
		id).Scan(&u.ID, &u.Username, &u.PasswordHash, &u.Nickname, &u.AvatarURL)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return u, err
}

func (d *DB) UpdateProfile(ctx context.Context, userID int64, nickname, avatarURL string) error {
	_, err := d.conn.ExecContext(ctx,
		`UPDATE users SET nickname = $1, avatar_url = $2 WHERE id = $3`,
		nickname, avatarURL, userID)
	return err
}
```

- [ ] **Step 3: Create logic/auth.go**

```go
package main

import (
	"context"
	"errors"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	pb "im-social/proto"
)

func (s *Server) Register(ctx context.Context, req *pb.RegisterRequest) (*pb.RegisterResponse, error) {
	if req.Username == "" || req.Password == "" {
		return nil, errors.New("username and password required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	userID, err := s.db.CreateUser(ctx, req.Username, string(hash))
	if err != nil {
		return nil, errors.New("username already taken")
	}
	access, refresh, err := s.generateTokens(userID)
	if err != nil {
		return nil, err
	}
	return &pb.RegisterResponse{UserId: userID, AccessToken: access, RefreshToken: refresh}, nil
}

func (s *Server) Login(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	user, err := s.db.GetUserByUsername(ctx, req.Username)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, errors.New("invalid credentials")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return nil, errors.New("invalid credentials")
	}
	access, refresh, err := s.generateTokens(user.ID)
	if err != nil {
		return nil, err
	}
	return &pb.LoginResponse{UserId: user.ID, AccessToken: access, RefreshToken: refresh}, nil
}

func (s *Server) Refresh(ctx context.Context, req *pb.RefreshRequest) (*pb.RefreshResponse, error) {
	claims, err := s.parseToken(req.RefreshToken)
	if err != nil {
		return nil, errors.New("invalid refresh token")
	}
	jti, _ := claims["jti"].(string)
	blacklisted, _ := s.redis.IsBlacklisted(ctx, jti)
	if blacklisted {
		return nil, errors.New("token revoked")
	}
	uid := int64(claims["uid"].(float64))
	access, err := s.generateAccessToken(uid)
	if err != nil {
		return nil, err
	}
	return &pb.RefreshResponse{AccessToken: access}, nil
}

func (s *Server) generateTokens(uid int64) (string, string, error) {
	access, err := s.generateAccessToken(uid)
	if err != nil {
		return "", "", err
	}
	refresh := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid": uid,
		"jti": fmt.Sprintf("%d-%d", uid, time.Now().UnixNano()),
		"exp": time.Now().Add(7 * 24 * time.Hour).Unix(),
	})
	refreshStr, err := refresh.SignedString([]byte(s.cfg.JWTSecret))
	if err != nil {
		return "", "", err
	}
	return access, refreshStr, nil
}

func (s *Server) generateAccessToken(uid int64) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"uid": uid,
		"exp": time.Now().Add(15 * time.Minute).Unix(),
	})
	return token.SignedString([]byte(s.cfg.JWTSecret))
}

func (s *Server) parseToken(tokenStr string) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return []byte(s.cfg.JWTSecret), nil
	})
	if err != nil || !token.Valid {
		return nil, errors.New("invalid token")
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("invalid claims")
	}
	return claims, nil
}

func (s *Server) authUID(token string) (int64, error) {
	claims, err := s.parseToken(token)
	if err != nil {
		return 0, err
	}
	uid, ok := claims["uid"].(float64)
	if !ok {
		return 0, errors.New("invalid uid in token")
	}
	return int64(uid), nil
}
```

- [ ] **Step 4: Create logic/server.go**

```go
package main

import (
	"fmt"

	"im-social/logic/db"
	"im-social/pkg/config"
	"im-social/pkg/kafka"
	"im-social/pkg/redisstore"
	pb "im-social/proto"
)

type Server struct {
	pb.UnimplementedLogicServiceServer
	cfg      *config.Config
	db       *db.DB
	redis    *redisstore.Store
	producer *kafka.Producer
}

func NewServer(cfg *config.Config) (*Server, error) {
	database, err := db.New(cfg.PostgresDSN)
	if err != nil {
		return nil, fmt.Errorf("db connect: %w", err)
	}
	producer, err := kafka.NewProducer(cfg.KafkaBroker, "message_fanout")
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Server{
		cfg:      cfg,
		db:       database,
		redis:    redisstore.New(cfg.RedisAddr),
		producer: producer,
	}, nil
}
```

- [ ] **Step 5: Create logic/main.go**

```go
package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"im-social/pkg/config"
	pb "im-social/proto"
)

func main() {
	cfg := config.Load()
	srv, err := NewServer(cfg)
	if err != nil {
		log.Fatalf("init server: %v", err)
	}
	lis, err := net.Listen("tcp", cfg.LogicGRPC)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLogicServiceServer(grpcServer, srv)
	log.Printf("Logic service listening on %s", cfg.LogicGRPC)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
```

- [ ] **Step 6: Verify compilation**

```bash
go build ./logic/
```

Expected: No errors.

- [ ] **Step 7: Commit**

```bash
git add .
git commit -m "feat: Logic service — auth module (register/login/refresh)"
```

---

## Task 5: Logic Service — User + Relation Module

**Files:**
- Create: `im-social/logic/user.go`
- Create: `im-social/logic/relation.go`
- Create: `im-social/logic/db/friends.go`

- [ ] **Step 1: Create logic/db/friends.go**

```go
package db

import "context"

type FriendRequest struct {
	ID           int64
	FromID       int64
	ToID         int64
	FromUsername string
	Message      string
	Status       string
	CreatedAt    int64
}

func (d *DB) CreateFriendRequest(ctx context.Context, fromID, toID int64, message string) (int64, error) {
	var id int64
	err := d.conn.QueryRowContext(ctx,
		`INSERT INTO friend_requests (from_id, to_id, message) VALUES ($1, $2, $3) RETURNING id`,
		fromID, toID, message).Scan(&id)
	return id, err
}

func (d *DB) GetFriendRequest(ctx context.Context, id int64) (*FriendRequest, error) {
	r := &FriendRequest{}
	err := d.conn.QueryRowContext(ctx,
		`SELECT id, from_id, to_id, COALESCE(message,''), status, EXTRACT(EPOCH FROM created_at)::bigint FROM friend_requests WHERE id = $1`,
		id).Scan(&r.ID, &r.FromID, &r.ToID, &r.Message, &r.Status, &r.CreatedAt)
	return r, err
}

func (d *DB) ListFriendRequests(ctx context.Context, toID int64) ([]FriendRequest, error) {
	rows, err := d.conn.QueryContext(ctx,
		`SELECT fr.id, fr.from_id, fr.to_id, u.username, COALESCE(fr.message,''), fr.status, EXTRACT(EPOCH FROM fr.created_at)::bigint
		 FROM friend_requests fr JOIN users u ON u.id = fr.from_id
		 WHERE fr.to_id = $1 ORDER BY fr.created_at DESC`, toID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []FriendRequest
	for rows.Next() {
		var r FriendRequest
		rows.Scan(&r.ID, &r.FromID, &r.ToID, &r.FromUsername, &r.Message, &r.Status, &r.CreatedAt)
		list = append(list, r)
	}
	return list, nil
}

func (d *DB) UpdateFriendRequestStatus(ctx context.Context, id int64, status string) error {
	_, err := d.conn.ExecContext(ctx,
		`UPDATE friend_requests SET status = $1 WHERE id = $2`, status, id)
	return err
}

func (d *DB) CreateFriendship(ctx context.Context, userID, friendID int64) error {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx,
		`INSERT INTO friendships (user_id, friend_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, userID, friendID)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO friendships (user_id, friend_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, friendID, userID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (d *DB) DeleteFriendship(ctx context.Context, userID, friendID int64) error {
	_, err := d.conn.ExecContext(ctx,
		`DELETE FROM friendships WHERE (user_id=$1 AND friend_id=$2) OR (user_id=$2 AND friend_id=$1)`,
		userID, friendID)
	return err
}

func (d *DB) BlockFriend(ctx context.Context, userID, friendID int64) error {
	_, err := d.conn.ExecContext(ctx,
		`UPDATE friendships SET status='blocked' WHERE user_id=$1 AND friend_id=$2`, userID, friendID)
	return err
}

func (d *DB) ListFriends(ctx context.Context, userID int64) ([]User, error) {
	rows, err := d.conn.QueryContext(ctx,
		`SELECT u.id, u.username, COALESCE(u.nickname,''), COALESCE(u.avatar_url,'')
		 FROM friendships f JOIN users u ON u.id = f.friend_id
		 WHERE f.user_id = $1 AND f.status = 'accepted'`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []User
	for rows.Next() {
		var u User
		rows.Scan(&u.ID, &u.Username, &u.Nickname, &u.AvatarURL)
		list = append(list, u)
	}
	return list, nil
}

func (d *DB) AreFriends(ctx context.Context, a, b int64) (bool, error) {
	var count int
	err := d.conn.QueryRowContext(ctx,
		`SELECT count(*) FROM friendships WHERE user_id=$1 AND friend_id=$2 AND status='accepted'`, a, b).Scan(&count)
	return count > 0, err
}
```

- [ ] **Step 2: Create logic/user.go**

```go
package main

import (
	"context"

	pb "im-social/proto"
)

func (s *Server) GetUser(ctx context.Context, req *pb.GetUserRequest) (*pb.UserProfile, error) {
	u, err := s.db.GetUserByID(ctx, req.UserId)
	if err != nil || u == nil {
		return nil, errors.New("user not found")
	}
	return &pb.UserProfile{Id: u.ID, Username: u.Username, Nickname: u.Nickname, AvatarUrl: u.AvatarURL}, nil
}

func (s *Server) UpdateProfile(ctx context.Context, req *pb.UpdateProfileRequest) (*pb.UpdateProfileResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	if err := s.db.UpdateProfile(ctx, uid, req.Nickname, req.AvatarUrl); err != nil {
		return nil, err
	}
	return &pb.UpdateProfileResponse{}, nil
}
```

- [ ] **Step 3: Create logic/relation.go**

```go
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	pb "im-social/proto"
)

func (s *Server) FriendList(ctx context.Context, req *pb.FriendListRequest) (*pb.FriendListResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	friends, err := s.db.ListFriends(ctx, uid)
	if err != nil {
		return nil, err
	}
	resp := &pb.FriendListResponse{}
	for _, f := range friends {
		resp.Friends = append(resp.Friends, &pb.UserProfile{
			Id: f.ID, Username: f.Username, Nickname: f.Nickname, AvatarUrl: f.AvatarURL,
		})
	}
	return resp, nil
}

func (s *Server) GenQRCode(ctx context.Context, req *pb.GenQRCodeRequest) (*pb.GenQRCodeResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	b := make([]byte, 16)
	rand.Read(b)
	code := hex.EncodeToString(b)
	if err := s.redis.SetFriendCode(ctx, code, uid); err != nil {
		return nil, err
	}
	return &pb.GenQRCodeResponse{Code: code, Url: fmt.Sprintf("/add?code=%s", code)}, nil
}

func (s *Server) AddFriend(ctx context.Context, req *pb.AddFriendRequest) (*pb.AddFriendResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	targetUID, err := s.redis.GetFriendCode(ctx, req.Code)
	if err != nil {
		return nil, errors.New("invalid or expired code")
	}
	if targetUID == uid {
		return nil, errors.New("cannot add yourself")
	}
	reqID, err := s.db.CreateFriendRequest(ctx, uid, targetUID, "")
	if err != nil {
		return nil, err
	}
	return &pb.AddFriendResponse{RequestId: reqID}, nil
}

func (s *Server) FriendRequestList(ctx context.Context, req *pb.FriendRequestListRequest) (*pb.FriendRequestListResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	list, err := s.db.ListFriendRequests(ctx, uid)
	if err != nil {
		return nil, err
	}
	resp := &pb.FriendRequestListResponse{}
	for _, r := range list {
		resp.Requests = append(resp.Requests, &pb.FriendRequestItem{
			Id: r.ID, FromId: r.FromID, FromUsername: r.FromUsername,
			Message: r.Message, Status: r.Status, CreatedAt: r.CreatedAt,
		})
	}
	return resp, nil
}

func (s *Server) AcceptFriend(ctx context.Context, req *pb.AcceptFriendRequest) (*pb.AcceptFriendResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	fr, err := s.db.GetFriendRequest(ctx, req.RequestId)
	if err != nil {
		return nil, err
	}
	if fr.ToID != uid || fr.Status != "pending" {
		return nil, errors.New("invalid request")
	}
	if err := s.db.UpdateFriendRequestStatus(ctx, fr.ID, "accepted"); err != nil {
		return nil, err
	}
	if err := s.db.CreateFriendship(ctx, uid, fr.FromID); err != nil {
		return nil, err
	}
	convID, err := s.db.CreateDMConversation(ctx, uid, fr.FromID)
	if err != nil {
		return nil, err
	}
	return &pb.AcceptFriendResponse{ConversationId: convID}, nil
}

func (s *Server) RejectFriend(ctx context.Context, req *pb.RejectFriendRequest) (*pb.RejectFriendResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	fr, err := s.db.GetFriendRequest(ctx, req.RequestId)
	if err != nil {
		return nil, err
	}
	if fr.ToID != uid || fr.Status != "pending" {
		return nil, errors.New("invalid request")
	}
	s.db.UpdateFriendRequestStatus(ctx, fr.ID, "rejected")
	return &pb.RejectFriendResponse{}, nil
}

func (s *Server) DeleteFriend(ctx context.Context, req *pb.DeleteFriendRequest) (*pb.DeleteFriendResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	if err := s.db.DeleteFriendship(ctx, uid, req.FriendId); err != nil {
		return nil, err
	}
	return &pb.DeleteFriendResponse{}, nil
}

func (s *Server) BlockFriend(ctx context.Context, req *pb.BlockFriendRequest) (*pb.BlockFriendResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	if err := s.db.BlockFriend(ctx, uid, req.FriendId); err != nil {
		return nil, err
	}
	return &pb.BlockFriendResponse{}, nil
}
```

- [ ] **Step 4: Verify compilation**

```bash
go build ./logic/
```

Expected: No errors.

- [ ] **Step 5: Commit**

```bash
git add .
git commit -m "feat: Logic service — user profile + friend relation module"
```

---

## Task 6: Logic Service — Conversation + Message Module

**Files:**
- Create: `im-social/logic/conversation.go`
- Create: `im-social/logic/message.go`
- Create: `im-social/logic/db/conversations.go`
- Create: `im-social/logic/db/messages.go`

- [ ] **Step 1: Create logic/db/conversations.go**

```go
package db

import "context"

type Conversation struct {
	ID        int64
	Type      string
	Name      string
	OwnerID   int64
	UpdatedAt int64
}

type UserConversation struct {
	ConversationID int64
	Type           string
	Name           string
	LastMsgID      int64
	LastReadMsgID  int64
	Muted          bool
	UpdatedAt      int64
	LastMsgContent string
	LastMsgFrom    string
}

func (d *DB) CreateDMConversation(ctx context.Context, userA, userB int64) (int64, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var convID int64
	err = tx.QueryRowContext(ctx,
		`INSERT INTO conversations (type) VALUES ('dm') RETURNING id`).Scan(&convID)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1,$2,'member'),($1,$3,'member')`,
		convID, userA, userB)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO user_conversations (user_id, conversation_id) VALUES ($1,$2),($3,$2)`,
		userA, convID, userB)
	if err != nil {
		return 0, err
	}
	return convID, tx.Commit()
}

func (d *DB) CreateGroupConversation(ctx context.Context, ownerID int64, name string, memberIDs []int64) (int64, error) {
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var convID int64
	err = tx.QueryRowContext(ctx,
		`INSERT INTO conversations (type, name, owner_id) VALUES ('group', $1, $2) RETURNING id`,
		name, ownerID).Scan(&convID)
	if err != nil {
		return 0, err
	}
	// Add owner
	_, err = tx.ExecContext(ctx,
		`INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1,$2,'owner')`,
		convID, ownerID)
	if err != nil {
		return 0, err
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO user_conversations (user_id, conversation_id) VALUES ($1,$2)`, ownerID, convID)
	if err != nil {
		return 0, err
	}
	// Add members
	for _, uid := range memberIDs {
		if uid == ownerID {
			continue
		}
		tx.ExecContext(ctx,
			`INSERT INTO conversation_members (conversation_id, user_id, role) VALUES ($1,$2,'member')`, convID, uid)
		tx.ExecContext(ctx,
			`INSERT INTO user_conversations (user_id, conversation_id) VALUES ($1,$2)`, uid, convID)
	}
	return convID, tx.Commit()
}

func (d *DB) ListUserConversations(ctx context.Context, userID int64) ([]UserConversation, error) {
	rows, err := d.conn.QueryContext(ctx,
		`SELECT uc.conversation_id, c.type, COALESCE(c.name,''), uc.last_msg_id, uc.last_read_msg_id, uc.muted,
		        EXTRACT(EPOCH FROM uc.updated_at)::bigint,
		        COALESCE(m.content,''), COALESCE(u.username,'')
		 FROM user_conversations uc
		 JOIN conversations c ON c.id = uc.conversation_id
		 LEFT JOIN messages m ON m.id = uc.last_msg_id
		 LEFT JOIN users u ON u.id = m.from_id
		 WHERE uc.user_id = $1
		 ORDER BY uc.updated_at DESC`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []UserConversation
	for rows.Next() {
		var uc UserConversation
		rows.Scan(&uc.ConversationID, &uc.Type, &uc.Name, &uc.LastMsgID, &uc.LastReadMsgID,
			&uc.Muted, &uc.UpdatedAt, &uc.LastMsgContent, &uc.LastMsgFrom)
		list = append(list, uc)
	}
	return list, nil
}

func (d *DB) GetConversationMembers(ctx context.Context, convID int64) ([]int64, error) {
	rows, err := d.conn.QueryContext(ctx,
		`SELECT user_id FROM conversation_members WHERE conversation_id = $1`, convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		rows.Scan(&id)
		ids = append(ids, id)
	}
	return ids, nil
}

func (d *DB) GetConversationMemberProfiles(ctx context.Context, convID int64) ([]User, error) {
	rows, err := d.conn.QueryContext(ctx,
		`SELECT u.id, u.username, COALESCE(u.nickname,''), COALESCE(u.avatar_url,'')
		 FROM conversation_members cm JOIN users u ON u.id = cm.user_id
		 WHERE cm.conversation_id = $1`, convID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []User
	for rows.Next() {
		var u User
		rows.Scan(&u.ID, &u.Username, &u.Nickname, &u.AvatarURL)
		list = append(list, u)
	}
	return list, nil
}

func (d *DB) IsMember(ctx context.Context, convID, userID int64) (bool, error) {
	var count int
	err := d.conn.QueryRowContext(ctx,
		`SELECT count(*) FROM conversation_members WHERE conversation_id=$1 AND user_id=$2`,
		convID, userID).Scan(&count)
	return count > 0, err
}

func (d *DB) AddMembers(ctx context.Context, convID int64, userIDs []int64) error {
	for _, uid := range userIDs {
		d.conn.ExecContext(ctx,
			`INSERT INTO conversation_members (conversation_id, user_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, convID, uid)
		d.conn.ExecContext(ctx,
			`INSERT INTO user_conversations (user_id, conversation_id) VALUES ($1,$2) ON CONFLICT DO NOTHING`, uid, convID)
	}
	return nil
}

func (d *DB) RemoveMember(ctx context.Context, convID, userID int64) error {
	d.conn.ExecContext(ctx, `DELETE FROM conversation_members WHERE conversation_id=$1 AND user_id=$2`, convID, userID)
	d.conn.ExecContext(ctx, `DELETE FROM user_conversations WHERE user_id=$1 AND conversation_id=$2`, userID, convID)
	return nil
}

func (d *DB) GetDMConversationName(ctx context.Context, convID, myUID int64) (string, error) {
	var name string
	err := d.conn.QueryRowContext(ctx,
		`SELECT u.username FROM conversation_members cm JOIN users u ON u.id = cm.user_id
		 WHERE cm.conversation_id = $1 AND cm.user_id != $2 LIMIT 1`,
		convID, myUID).Scan(&name)
	return name, err
}

func (d *DB) CountUnread(ctx context.Context, convID, lastReadMsgID int64) (int32, error) {
	var count int32
	err := d.conn.QueryRowContext(ctx,
		`SELECT count(*) FROM messages WHERE conversation_id = $1 AND id > $2`,
		convID, lastReadMsgID).Scan(&count)
	return count, err
}
```

- [ ] **Step 2: Create logic/db/messages.go**

```go
package db

import "context"

type Message struct {
	ID             int64
	ConversationID int64
	FromID         int64
	FromUsername   string
	MsgType        string
	Content        string
	CreatedAt      int64
}

func (d *DB) CreateMessage(ctx context.Context, convID, fromID int64, msgType, content string) (int64, int64, error) {
	var id, createdAt int64
	err := d.conn.QueryRowContext(ctx,
		`INSERT INTO messages (conversation_id, from_id, msg_type, content) VALUES ($1,$2,$3,$4)
		 RETURNING id, EXTRACT(EPOCH FROM created_at)::bigint`,
		convID, fromID, msgType, content).Scan(&id, &createdAt)
	return id, createdAt, err
}

func (d *DB) GetMessageHistory(ctx context.Context, convID, cursor int64, limit int32) ([]Message, error) {
	query := `SELECT m.id, m.conversation_id, m.from_id, u.username, m.msg_type, m.content, EXTRACT(EPOCH FROM m.created_at)::bigint
		 FROM messages m JOIN users u ON u.id = m.from_id
		 WHERE m.conversation_id = $1`
	args := []interface{}{convID}
	if cursor > 0 {
		query += ` AND m.id < $2`
		args = append(args, cursor)
	}
	query += ` ORDER BY m.id DESC LIMIT $` + fmt.Sprintf("%d", len(args)+1)
	args = append(args, limit)

	rows, err := d.conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []Message
	for rows.Next() {
		var m Message
		rows.Scan(&m.ID, &m.ConversationID, &m.FromID, &m.FromUsername, &m.MsgType, &m.Content, &m.CreatedAt)
		list = append(list, m)
	}
	return list, nil
}

func (d *DB) MarkRead(ctx context.Context, userID, convID, msgID int64) error {
	_, err := d.conn.ExecContext(ctx,
		`UPDATE user_conversations SET last_read_msg_id = $1 WHERE user_id = $2 AND conversation_id = $3`,
		msgID, userID, convID)
	return err
}

func (d *DB) UpdateUserConversation(ctx context.Context, userID, convID, lastMsgID int64) error {
	_, err := d.conn.ExecContext(ctx,
		`UPDATE user_conversations SET last_msg_id = $1, updated_at = NOW() WHERE user_id = $2 AND conversation_id = $3`,
		lastMsgID, userID, convID)
	return err
}
```

- [ ] **Step 3: Create logic/conversation.go**

```go
package main

import (
	"context"
	"errors"

	pb "im-social/proto"
)

func (s *Server) ConversationList(ctx context.Context, req *pb.ConversationListRequest) (*pb.ConversationListResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	list, err := s.db.ListUserConversations(ctx, uid)
	if err != nil {
		return nil, err
	}
	resp := &pb.ConversationListResponse{}
	for _, uc := range list {
		name := uc.Name
		if uc.Type == "dm" && name == "" {
			name, _ = s.db.GetDMConversationName(ctx, uc.ConversationID, uid)
		}
		unread, _ := s.db.CountUnread(ctx, uc.ConversationID, uc.LastReadMsgID)
		resp.Conversations = append(resp.Conversations, &pb.ConversationItem{
			Id: uc.ConversationID, Type: uc.Type, Name: name,
			LastMsgId: uc.LastMsgID, LastReadMsgId: uc.LastReadMsgID,
			Muted: uc.Muted, UpdatedAt: uc.UpdatedAt,
			LastMsgContent: uc.LastMsgContent, LastMsgFrom: uc.LastMsgFrom,
			UnreadCount: unread,
		})
	}
	return resp, nil
}

func (s *Server) CreateGroup(ctx context.Context, req *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	convID, err := s.db.CreateGroupConversation(ctx, uid, req.Name, req.MemberIds)
	if err != nil {
		return nil, err
	}
	return &pb.CreateGroupResponse{ConversationId: convID}, nil
}

func (s *Server) ConversationMembers(ctx context.Context, req *pb.ConversationMembersRequest) (*pb.ConversationMembersResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	ok, _ := s.db.IsMember(ctx, req.ConversationId, uid)
	if !ok {
		return nil, errors.New("not a member")
	}
	members, err := s.db.GetConversationMemberProfiles(ctx, req.ConversationId)
	if err != nil {
		return nil, err
	}
	resp := &pb.ConversationMembersResponse{}
	for _, m := range members {
		resp.Members = append(resp.Members, &pb.UserProfile{
			Id: m.ID, Username: m.Username, Nickname: m.Nickname, AvatarUrl: m.AvatarURL,
		})
	}
	return resp, nil
}

func (s *Server) InviteMembers(ctx context.Context, req *pb.InviteMembersRequest) (*pb.InviteMembersResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	ok, _ := s.db.IsMember(ctx, req.ConversationId, uid)
	if !ok {
		return nil, errors.New("not a member")
	}
	if err := s.db.AddMembers(ctx, req.ConversationId, req.UserIds); err != nil {
		return nil, err
	}
	s.redis.DelConvMembers(ctx, req.ConversationId)
	return &pb.InviteMembersResponse{}, nil
}

func (s *Server) RemoveMember(ctx context.Context, req *pb.RemoveMemberRequest) (*pb.RemoveMemberResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	ok, _ := s.db.IsMember(ctx, req.ConversationId, uid)
	if !ok {
		return nil, errors.New("not a member")
	}
	if err := s.db.RemoveMember(ctx, req.ConversationId, req.UserId); err != nil {
		return nil, err
	}
	s.redis.DelConvMembers(ctx, req.ConversationId)
	return &pb.RemoveMemberResponse{}, nil
}
```

- [ ] **Step 4: Create logic/message.go**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	pb "im-social/proto"
)

type FanoutEvent struct {
	MsgID          int64  `json:"msg_id"`
	ConversationID int64  `json:"conversation_id"`
	FromID         int64  `json:"from_id"`
	FromUsername   string `json:"from_username"`
	Content        string `json:"content"`
	MsgType        string `json:"msg_type"`
	CreatedAt      int64  `json:"created_at"`
}

func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	ok, _ := s.db.IsMember(ctx, req.ConversationId, uid)
	if !ok {
		return nil, errors.New("not a member")
	}
	msgType := req.MsgType
	if msgType == "" {
		msgType = "text"
	}
	user, _ := s.db.GetUserByID(ctx, uid)
	msgID, createdAt, err := s.db.CreateMessage(ctx, req.ConversationId, uid, msgType, req.Content)
	if err != nil {
		return nil, err
	}
	event := FanoutEvent{
		MsgID: msgID, ConversationID: req.ConversationId,
		FromID: uid, FromUsername: user.Username,
		Content: req.Content, MsgType: msgType, CreatedAt: createdAt,
	}
	data, _ := json.Marshal(event)
	s.producer.Send(fmt.Sprintf("%d", req.ConversationId), data)
	return &pb.SendMessageResponse{MessageId: msgID, CreatedAt: createdAt}, nil
}

func (s *Server) MessageHistory(ctx context.Context, req *pb.MessageHistoryRequest) (*pb.MessageHistoryResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	ok, _ := s.db.IsMember(ctx, req.ConversationId, uid)
	if !ok {
		return nil, errors.New("not a member")
	}
	limit := req.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}
	msgs, err := s.db.GetMessageHistory(ctx, req.ConversationId, req.Cursor, limit)
	if err != nil {
		return nil, err
	}
	resp := &pb.MessageHistoryResponse{}
	for _, m := range msgs {
		resp.Messages = append(resp.Messages, &pb.MessageItem{
			Id: m.ID, ConversationId: m.ConversationID, FromId: m.FromID,
			FromUsername: m.FromUsername, Content: m.Content, MsgType: m.MsgType, CreatedAt: m.CreatedAt,
		})
	}
	if len(msgs) > 0 {
		resp.NextCursor = msgs[len(msgs)-1].ID
	}
	return resp, nil
}

func (s *Server) MarkRead(ctx context.Context, req *pb.MarkReadRequest) (*pb.MarkReadResponse, error) {
	uid, err := s.authUID(req.Token)
	if err != nil {
		return nil, err
	}
	if err := s.db.MarkRead(ctx, uid, req.ConversationId, req.MsgId); err != nil {
		return nil, err
	}
	return &pb.MarkReadResponse{}, nil
}
```

- [ ] **Step 5: Verify compilation**

```bash
go build ./logic/
```

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: Logic service — conversation + message module"
```

---

## Task 7: Gateway Service

**Files:**
- Create: `im-social/gateway/main.go`
- Create: `im-social/gateway/server.go`
- Create: `im-social/gateway/handler.go`
- Create: `im-social/gateway/hub.go`

- [ ] **Step 1: Create gateway/hub.go**

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

func (h *Hub) Add(uid int64, conn *websocket.Conn) {
	h.mu.Lock()
	h.conns[uid] = conn
	h.mu.Unlock()
}

func (h *Hub) Get(uid int64) *websocket.Conn {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[uid]
}

func (h *Hub) Remove(uid int64) {
	h.mu.Lock()
	delete(h.conns, uid)
	h.mu.Unlock()
}
```

- [ ] **Step 2: Create gateway/server.go**

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"im-social/pkg/config"
	"im-social/pkg/redisstore"
	pb "im-social/proto"
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
		upgrader: websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }},
	}
}

func (s *GatewayServer) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	conn := s.hub.Get(req.UserId)
	if conn != nil {
		conn.WriteMessage(websocket.TextMessage, req.Payload)
	}
	return &pb.PushResponse{}, nil
}

func (s *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// CORS
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	if r.Method == "OPTIONS" {
		return
	}

	path := r.URL.Path
	switch {
	case path == "/ws":
		s.handleWSUpgrade(w, r)
	// Auth
	case path == "/api/auth/register" && r.Method == "POST":
		s.handleRegister(w, r)
	case path == "/api/auth/login" && r.Method == "POST":
		s.handleLogin(w, r)
	case path == "/api/auth/refresh" && r.Method == "POST":
		s.handleRefresh(w, r)
	// User
	case path == "/api/users/me" && r.Method == "GET":
		s.handleGetMe(w, r)
	case path == "/api/users/me" && r.Method == "PUT":
		s.handleUpdateMe(w, r)
	// Friends
	case path == "/api/friends" && r.Method == "GET":
		s.handleFriendList(w, r)
	case path == "/api/friends/qrcode" && r.Method == "POST":
		s.handleGenQRCode(w, r)
	case path == "/api/friends/add" && r.Method == "POST":
		s.handleAddFriend(w, r)
	case path == "/api/friends/requests" && r.Method == "GET":
		s.handleFriendRequests(w, r)
	case strings.HasSuffix(path, "/accept") && r.Method == "POST":
		s.handleAcceptFriend(w, r)
	case strings.HasSuffix(path, "/reject") && r.Method == "POST":
		s.handleRejectFriend(w, r)
	case strings.HasPrefix(path, "/api/friends/") && strings.HasSuffix(path, "/block") && r.Method == "POST":
		s.handleBlockFriend(w, r)
	case strings.HasPrefix(path, "/api/friends/") && r.Method == "DELETE":
		s.handleDeleteFriend(w, r)
	// Conversations
	case path == "/api/conversations" && r.Method == "GET":
		s.handleConversationList(w, r)
	case path == "/api/conversations" && r.Method == "POST":
		s.handleCreateGroup(w, r)
	case strings.HasSuffix(path, "/messages") && r.Method == "GET":
		s.handleMessageHistory(w, r)
	case strings.HasSuffix(path, "/members") && r.Method == "GET":
		s.handleConversationMembers(w, r)
	case strings.HasSuffix(path, "/members") && r.Method == "POST":
		s.handleInviteMembers(w, r)
	case strings.Contains(path, "/members/") && r.Method == "DELETE":
		s.handleRemoveMember(w, r)
	default:
		http.FileServer(http.Dir("frontend/dist")).ServeHTTP(w, r)
	}
}

func (s *GatewayServer) bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && h[:7] == "Bearer " {
		return h[7:]
	}
	return ""
}

func (s *GatewayServer) parseUIDFromToken(tokenStr string) int64 {
	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		return []byte(s.cfg.JWTSecret), nil
	})
	if err != nil || !token.Valid {
		return 0
	}
	claims, _ := token.Claims.(jwt.MapClaims)
	uid, _ := claims["uid"].(float64)
	return int64(uid)
}

func (s *GatewayServer) jsonResp(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *GatewayServer) errResp(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func (s *GatewayServer) pathID(path, prefix string) int64 {
	s1 := strings.TrimPrefix(path, prefix)
	parts := strings.Split(s1, "/")
	if len(parts) == 0 {
		return 0
	}
	id, _ := strconv.ParseInt(parts[0], 10, 64)
	return id
}

// HTTP handlers delegate to Logic gRPC
func (s *GatewayServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req pb.RegisterRequest
	json.NewDecoder(r.Body).Decode(&req)
	resp, err := s.logic.Register(r.Context(), &req)
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req pb.LoginRequest
	json.NewDecoder(r.Body).Decode(&req)
	resp, err := s.logic.Login(r.Context(), &req)
	if err != nil {
		s.errResp(w, 401, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleRefresh(w http.ResponseWriter, r *http.Request) {
	var req pb.RefreshRequest
	json.NewDecoder(r.Body).Decode(&req)
	resp, err := s.logic.Refresh(r.Context(), &req)
	if err != nil {
		s.errResp(w, 401, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleGetMe(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	uid := s.parseUIDFromToken(token)
	if uid == 0 {
		s.errResp(w, 401, "unauthorized")
		return
	}
	resp, err := s.logic.GetUser(r.Context(), &pb.GetUserRequest{UserId: uid})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleUpdateMe(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	var req pb.UpdateProfileRequest
	json.NewDecoder(r.Body).Decode(&req)
	req.Token = token
	resp, err := s.logic.UpdateProfile(r.Context(), &req)
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleFriendList(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	resp, err := s.logic.FriendList(r.Context(), &pb.FriendListRequest{Token: token})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleGenQRCode(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	resp, err := s.logic.GenQRCode(r.Context(), &pb.GenQRCodeRequest{Token: token})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleAddFriend(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	var body struct{ Code string `json:"code"` }
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.AddFriend(r.Context(), &pb.AddFriendRequest{Token: token, Code: body.Code})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleFriendRequests(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	resp, err := s.logic.FriendRequestList(r.Context(), &pb.FriendRequestListRequest{Token: token})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleAcceptFriend(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	id := s.pathID(r.URL.Path, "/api/friends/requests/")
	resp, err := s.logic.AcceptFriend(r.Context(), &pb.AcceptFriendRequest{Token: token, RequestId: id})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleRejectFriend(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	id := s.pathID(r.URL.Path, "/api/friends/requests/")
	resp, err := s.logic.RejectFriend(r.Context(), &pb.RejectFriendRequest{Token: token, RequestId: id})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleDeleteFriend(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	id := s.pathID(r.URL.Path, "/api/friends/")
	resp, err := s.logic.DeleteFriend(r.Context(), &pb.DeleteFriendRequest{Token: token, FriendId: id})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleBlockFriend(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	id := s.pathID(r.URL.Path, "/api/friends/")
	resp, err := s.logic.BlockFriend(r.Context(), &pb.BlockFriendRequest{Token: token, FriendId: id})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleConversationList(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	resp, err := s.logic.ConversationList(r.Context(), &pb.ConversationListRequest{Token: token})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	var body struct {
		Name      string  `json:"name"`
		MemberIDs []int64 `json:"member_ids"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.CreateGroup(r.Context(), &pb.CreateGroupRequest{Token: token, Name: body.Name, MemberIds: body.MemberIDs})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleMessageHistory(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	convID := s.pathID(r.URL.Path, "/api/conversations/")
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 {
		limit = 20
	}
	resp, err := s.logic.MessageHistory(r.Context(), &pb.MessageHistoryRequest{
		Token: token, ConversationId: convID, Cursor: cursor, Limit: int32(limit),
	})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleConversationMembers(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	convID := s.pathID(r.URL.Path, "/api/conversations/")
	resp, err := s.logic.ConversationMembers(r.Context(), &pb.ConversationMembersRequest{Token: token, ConversationId: convID})
	if err != nil {
		s.errResp(w, 500, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleInviteMembers(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	convID := s.pathID(r.URL.Path, "/api/conversations/")
	var body struct{ UserIDs []int64 `json:"user_ids"` }
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.InviteMembers(r.Context(), &pb.InviteMembersRequest{Token: token, ConversationId: convID, UserIds: body.UserIDs})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleRemoveMember(w http.ResponseWriter, r *http.Request) {
	token := s.bearerToken(r)
	// path: /api/conversations/:id/members/:uid
	parts := strings.Split(r.URL.Path, "/")
	convID, _ := strconv.ParseInt(parts[3], 10, 64)
	uid, _ := strconv.ParseInt(parts[5], 10, 64)
	resp, err := s.logic.RemoveMember(r.Context(), &pb.RemoveMemberRequest{Token: token, ConversationId: convID, UserId: uid})
	if err != nil {
		s.errResp(w, 400, err.Error())
		return
	}
	s.jsonResp(w, resp)
}

func (s *GatewayServer) handleWSUpgrade(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		conn.Close()
		return
	}
	var authMsg struct{ Token string `json:"token"` }
	if err := json.Unmarshal(data, &authMsg); err != nil || authMsg.Token == "" {
		conn.Close()
		return
	}
	uid := s.parseUIDFromToken(authMsg.Token)
	if uid == 0 {
		conn.Close()
		return
	}
	s.hub.Add(uid, conn)
	s.redis.SetRoute(context.Background(), uid, s.cfg.GatewayGRPC)
	log.Printf("user %d connected", uid)
	// Send initial conversation list
	resp, _ := s.logic.ConversationList(context.Background(), &pb.ConversationListRequest{Token: authMsg.Token})
	if resp != nil {
		payload, _ := json.Marshal(map[string]interface{}{"type": "conversations", "data": resp.Conversations})
		conn.WriteMessage(websocket.TextMessage, payload)
	}
	s.handleWS(conn, uid, authMsg.Token)
}

func (s *GatewayServer) StartGRPC() {
	lis, err := net.Listen("tcp", s.cfg.GatewayGRPC)
	if err != nil {
		log.Fatalf("gateway grpc: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterGatewayServiceServer(grpcServer, s)
	log.Printf("Gateway gRPC on %s", s.cfg.GatewayGRPC)
	go grpcServer.Serve(lis)
}
```

- [ ] **Step 3: Create gateway/handler.go**

```go
package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
	pb "im-social/proto"
)

type WSMessage struct {
	Type           string `json:"type"`
	ConversationID int64  `json:"conversation_id,omitempty"`
	Content        string `json:"content,omitempty"`
	MsgType        string `json:"msg_type,omitempty"`
	MsgID          int64  `json:"msg_id,omitempty"`
}

func (s *GatewayServer) handleWS(conn *websocket.Conn, userID int64, token string) {
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
		case "send_message":
			msgType := msg.MsgType
			if msgType == "" {
				msgType = "text"
			}
			resp, err := s.logic.SendMessage(ctx, &pb.SendMessageRequest{
				Token: token, ConversationId: msg.ConversationID, Content: msg.Content, MsgType: msgType,
			})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "sent", "message_id": resp.MessageId, "created_at": resp.CreatedAt})
		case "mark_read":
			_, err := s.logic.MarkRead(ctx, &pb.MarkReadRequest{Token: token, ConversationId: msg.ConversationID, MsgId: msg.MsgID})
			if err != nil {
				s.writeJSON(conn, map[string]interface{}{"type": "error", "error": err.Error()})
				continue
			}
			s.writeJSON(conn, map[string]interface{}{"type": "read_ack", "conversation_id": msg.ConversationID})
		case "typing":
			// Broadcast typing to other members (best-effort, no persistence)
			log.Printf("user %d typing in conv %d", userID, msg.ConversationID)
		}
	}
}

func (s *GatewayServer) writeJSON(conn *websocket.Conn, v interface{}) {
	data, _ := json.Marshal(v)
	conn.WriteMessage(websocket.TextMessage, data)
}
```

- [ ] **Step 4: Create gateway/main.go**

```go
package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-social/pkg/config"
	"im-social/pkg/redisstore"
	pb "im-social/proto"
)

func main() {
	cfg := config.Load()
	logicConn, err := grpc.Dial(cfg.LogicGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect logic: %v", err)
	}
	logicClient := pb.NewLogicServiceClient(logicConn)
	redis := redisstore.New(cfg.RedisAddr)
	srv := NewGatewayServer(cfg, redis, logicClient)
	srv.StartGRPC()
	log.Printf("Gateway HTTP on %s", cfg.GatewayHTTP)
	log.Fatal(http.ListenAndServe(cfg.GatewayHTTP, srv))
}
```

- [ ] **Step 5: Verify compilation**

```bash
go build ./gateway/
```

- [ ] **Step 6: Commit**

```bash
git add .
git commit -m "feat: Gateway service (HTTP REST + WebSocket + gRPC Push)"
```

---

## Task 8: Fanout Worker

**Files:**
- Create: `im-social/fanout/main.go`
- Create: `im-social/fanout/worker.go`

- [ ] **Step 1: Create fanout/worker.go**

```go
package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-social/logic/db"
	"im-social/pkg/redisstore"
	pb "im-social/proto"
)

type FanoutEvent struct {
	MsgID          int64  `json:"msg_id"`
	ConversationID int64  `json:"conversation_id"`
	FromID         int64  `json:"from_id"`
	FromUsername   string `json:"from_username"`
	Content        string `json:"content"`
	MsgType        string `json:"msg_type"`
	CreatedAt      int64  `json:"created_at"`
}

type Worker struct {
	db    *db.DB
	redis *redisstore.Store
	mu    sync.Mutex
	gws   map[string]pb.GatewayServiceClient
}

func NewWorker(database *db.DB, redis *redisstore.Store) *Worker {
	return &Worker{db: database, redis: redis, gws: make(map[string]pb.GatewayServiceClient)}
}

func (w *Worker) Handle(key, value []byte) error {
	var event FanoutEvent
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("unmarshal error: %v", err)
		return nil
	}
	ctx := context.Background()

	// Get members (Redis cache first)
	members, err := w.redis.GetConvMembers(ctx, event.ConversationID)
	if err != nil {
		members, err = w.db.GetConversationMembers(ctx, event.ConversationID)
		if err != nil {
			log.Printf("get members error: %v", err)
			return nil
		}
		w.redis.SetConvMembers(ctx, event.ConversationID, members, 10*time.Minute)
	}

	// Update user_conversations + push
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)
	for _, uid := range members {
		wg.Add(1)
		sem <- struct{}{}
		go func(uid int64) {
			defer wg.Done()
			defer func() { <-sem }()
			w.db.UpdateUserConversation(ctx, uid, event.ConversationID, event.MsgID)
			w.pushToUser(ctx, uid, event)
		}(uid)
	}
	wg.Wait()
	return nil
}

func (w *Worker) pushToUser(ctx context.Context, uid int64, event FanoutEvent) {
	addr, err := w.redis.GetRoute(ctx, uid)
	if err != nil {
		return // offline
	}
	client := w.getGatewayClient(addr)
	if client == nil {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"type":            "new_message",
		"message_id":      event.MsgID,
		"conversation_id": event.ConversationID,
		"from_id":         event.FromID,
		"from_username":   event.FromUsername,
		"content":         event.Content,
		"msg_type":        event.MsgType,
		"created_at":      event.CreatedAt,
	})
	client.Push(ctx, &pb.PushRequest{UserId: uid, Payload: payload})
}

func (w *Worker) getGatewayClient(addr string) pb.GatewayServiceClient {
	w.mu.Lock()
	defer w.mu.Unlock()
	if c, ok := w.gws[addr]; ok {
		return c
	}
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	client := pb.NewGatewayServiceClient(conn)
	w.gws[addr] = client
	return client
}
```

- [ ] **Step 2: Create fanout/main.go**

```go
package main

import (
	"log"

	"im-social/logic/db"
	"im-social/pkg/config"
	"im-social/pkg/kafka"
	"im-social/pkg/redisstore"
)

func main() {
	cfg := config.Load()
	database, err := db.New(cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	redis := redisstore.New(cfg.RedisAddr)
	consumer, err := kafka.NewConsumer(cfg.KafkaBroker, "message_fanout")
	if err != nil {
		log.Fatalf("kafka: %v", err)
	}
	worker := NewWorker(database, redis)
	log.Println("Fanout worker started")
	if err := consumer.Consume(worker.Handle); err != nil {
		log.Fatalf("consume: %v", err)
	}
	select {}
}
```

- [ ] **Step 3: Verify compilation**

```bash
go build ./fanout/
```

- [ ] **Step 4: Commit**

```bash
git add .
git commit -m "feat: Fanout worker (Kafka consume + write-spread + push)"
```

---

## Task 9: Frontend — React SPA

**Files:**
- Create: `im-social/frontend/` (Vite + React + Tailwind project)
- Key files: `src/App.jsx`, `src/contexts/AuthContext.jsx`, `src/contexts/WSContext.jsx`, `src/components/NavBar.jsx`, `src/components/ConversationList.jsx`, `src/components/ChatPanel.jsx`, `src/components/FriendPanel.jsx`, `src/pages/Login.jsx`

- [ ] **Step 1: Scaffold React project**

```bash
cd im-social
npm create vite@latest frontend -- --template react
cd frontend
npm install
npm install tailwindcss @tailwindcss/vite
```

- [ ] **Step 2: Configure vite.config.js**

```js
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3000,
    proxy: {
      '/api': 'http://localhost:8080',
      '/ws': { target: 'ws://localhost:8080', ws: true },
    },
  },
})
```

- [ ] **Step 3: Create src/index.css**

```css
@import "tailwindcss";
```

- [ ] **Step 4: Create src/contexts/AuthContext.jsx**

```jsx
import { createContext, useContext, useState } from 'react'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [auth, setAuth] = useState(() => {
    const stored = localStorage.getItem('auth')
    return stored ? JSON.parse(stored) : null
  })

  const login = (data) => {
    localStorage.setItem('auth', JSON.stringify(data))
    setAuth(data)
  }

  const logout = () => {
    localStorage.removeItem('auth')
    setAuth(null)
  }

  return (
    <AuthContext.Provider value={{ auth, login, logout }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => useContext(AuthContext)
```

- [ ] **Step 5: Create src/contexts/WSContext.jsx**

```jsx
import { createContext, useContext, useEffect, useRef, useState } from 'react'
import { useAuth } from './AuthContext'

const WSContext = createContext(null)

export function WSProvider({ children }) {
  const { auth } = useAuth()
  const wsRef = useRef(null)
  const [lastMessage, setLastMessage] = useState(null)

  useEffect(() => {
    if (!auth) return
    const ws = new WebSocket(`ws://${location.host}/ws`)
    wsRef.current = ws
    ws.onopen = () => ws.send(JSON.stringify({ token: auth.access_token }))
    ws.onmessage = (e) => setLastMessage(JSON.parse(e.data))
    ws.onclose = () => { wsRef.current = null }
    return () => ws.close()
  }, [auth])

  const send = (msg) => wsRef.current?.send(JSON.stringify(msg))

  return (
    <WSContext.Provider value={{ lastMessage, send }}>
      {children}
    </WSContext.Provider>
  )
}

export const useWS = () => useContext(WSContext)
```

- [ ] **Step 6: Create src/pages/Login.jsx**

```jsx
import { useState } from 'react'
import { useAuth } from '../contexts/AuthContext'

export default function Login() {
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const { login } = useAuth()

  const submit = async (endpoint) => {
    setError('')
    const res = await fetch(`/api/auth/${endpoint}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password }),
    })
    const data = await res.json()
    if (data.access_token) {
      login({ access_token: data.access_token, refresh_token: data.refresh_token, user_id: data.user_id, username })
    } else {
      setError(data.error || '操作失败')
    }
  }

  return (
    <div className="flex items-center justify-center h-screen bg-[#ebebeb]">
      <div className="w-80 p-8 bg-white rounded-lg shadow">
        <h1 className="text-xl font-semibold text-center mb-6">IM Social</h1>
        <input className="w-full px-4 py-3 mb-3 rounded bg-[#f7f7f7] outline-none text-sm" placeholder="用户名" value={username} onChange={e => setUsername(e.target.value)} />
        <input className="w-full px-4 py-3 mb-4 rounded bg-[#f7f7f7] outline-none text-sm" type="password" placeholder="密码" value={password} onChange={e => setPassword(e.target.value)} onKeyDown={e => e.key === 'Enter' && submit('login')} />
        {error && <p className="text-red-500 text-xs mb-3 text-center">{error}</p>}
        <div className="flex gap-2">
          <button onClick={() => submit('login')} className="flex-1 py-3 bg-[#07c160] text-white rounded text-sm font-medium hover:bg-[#06ad56]">登录</button>
          <button onClick={() => submit('register')} className="flex-1 py-3 bg-[#f7f7f7] text-gray-700 rounded text-sm font-medium hover:bg-[#ededed]">注册</button>
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 7: Create src/components/NavBar.jsx**

```jsx
export default function NavBar({ active, onNav, username }) {
  const items = [
    { key: 'chat', icon: '💬' },
    { key: 'friends', icon: '👥' },
    { key: 'settings', icon: '⚙️' },
  ]
  return (
    <div className="w-[60px] bg-[#2e2e2e] flex flex-col items-center py-4 gap-2">
      <div className="w-9 h-9 rounded-full bg-[#07c160] flex items-center justify-center text-white text-sm font-bold mb-4">
        {username?.[0]?.toUpperCase()}
      </div>
      {items.map(i => (
        <div key={i.key} onClick={() => onNav(i.key)}
          className={`w-10 h-10 rounded-lg flex items-center justify-center cursor-pointer text-lg ${active === i.key ? 'bg-[#3a3a3a] border-2 border-[#07c160]' : 'hover:bg-[#3a3a3a]'}`}>
          {i.icon}
        </div>
      ))}
    </div>
  )
}
```

- [ ] **Step 8: Create src/components/ConversationList.jsx + ChatPanel.jsx + FriendPanel.jsx and App.jsx**

These follow the three-panel layout spec. Full implementation with API calls via `fetch('/api/...')` and WebSocket `send/onmessage` for real-time. Key patterns:

- `ConversationList`: fetches `GET /api/conversations`, renders list with unread badges, green highlight on selected
- `ChatPanel`: fetches `GET /api/conversations/:id/messages?cursor=`, renders bubbles (white left / green right), sends via WS `send_message`
- `FriendPanel`: fetches `GET /api/friends`, shows QR code generation, friend request list
- `App.jsx`: wraps in `AuthProvider` + `WSProvider`, shows Login if no auth, otherwise three-panel layout

```jsx
// src/App.jsx
import { AuthProvider, useAuth } from './contexts/AuthContext'
import { WSProvider } from './contexts/WSContext'
import Login from './pages/Login'
import Main from './pages/Main'

function Inner() {
  const { auth } = useAuth()
  if (!auth) return <Login />
  return <WSProvider><Main /></WSProvider>
}

export default function App() {
  return <AuthProvider><Inner /></AuthProvider>
}
```

- [ ] **Step 9: Verify frontend builds**

```bash
cd frontend && npm run build
```

Expected: Successful build with dist/ output.

- [ ] **Step 10: Commit**

```bash
git add .
git commit -m "feat: React frontend (login, three-panel layout, chat, friends)"
```

---

## Task 10: Integration Test + End-to-End Verification

**Files:**
- No new files, testing existing services together

- [ ] **Step 1: Start all infrastructure**

```bash
cd im-social
docker-compose up -d
```

- [ ] **Step 2: Start Logic service**

```bash
go run ./logic/ &
```

Expected: "Logic service listening on :9002"

- [ ] **Step 3: Start Gateway service**

```bash
go run ./gateway/ &
```

Expected: "Gateway HTTP on :8080" + "Gateway gRPC on :9001"

- [ ] **Step 4: Start Fanout worker**

```bash
go run ./fanout/ &
```

Expected: "Fanout worker started"

- [ ] **Step 5: Start frontend dev server**

```bash
cd frontend && npm run dev &
```

Expected: Vite running on http://localhost:3000

- [ ] **Step 6: Test registration and login**

```bash
curl -s -X POST http://localhost:8080/api/auth/register -H "Content-Type: application/json" -d '{"username":"alice","password":"pass123"}' | jq .
curl -s -X POST http://localhost:8080/api/auth/register -H "Content-Type: application/json" -d '{"username":"bob","password":"pass123"}' | jq .
```

Expected: Both return `access_token`, `refresh_token`, `user_id`.

- [ ] **Step 7: Test friend flow**

```bash
# Alice generates QR code
ALICE_TOKEN="<access_token from step 6>"
curl -s -X POST http://localhost:8080/api/friends/qrcode -H "Authorization: Bearer $ALICE_TOKEN" | jq .

# Bob adds Alice using the code
BOB_TOKEN="<access_token from step 6>"
CODE="<code from previous response>"
curl -s -X POST http://localhost:8080/api/friends/add -H "Authorization: Bearer $BOB_TOKEN" -H "Content-Type: application/json" -d "{\"code\":\"$CODE\"}" | jq .

# Alice accepts
REQ_ID="<request_id from previous response>"
curl -s -X POST "http://localhost:8080/api/friends/requests/$REQ_ID/accept" -H "Authorization: Bearer $ALICE_TOKEN" | jq .
```

Expected: Returns `conversation_id` for the new DM.

- [ ] **Step 8: Test messaging via WebSocket**

Open browser at http://localhost:3000, log in as Alice and Bob in two tabs, verify real-time messaging works.

- [ ] **Step 9: Commit final state**

```bash
git add .
git commit -m "feat: integration verified, all services working end-to-end"
```
