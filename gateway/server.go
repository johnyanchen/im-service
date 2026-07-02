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
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/config"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type GatewayServer struct {
	pb.UnimplementedGatewayServiceServer
	cfg      *config.Config
	hub      *Hub
	redis    *redisstore.Store
	upgrader websocket.Upgrader
}

func NewGatewayServer(cfg *config.Config, redis *redisstore.Store) *GatewayServer {
	return &GatewayServer{
		cfg:   cfg,
		hub:   NewHub(),
		redis: redis,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool { return true },
		},
	}
}

func (s *GatewayServer) Push(ctx context.Context, req *pb.PushRequest) (*pb.PushResponse, error) {
	conn := s.hub.Get(req.UserId)
	if conn == nil {
		return &pb.PushResponse{}, nil
	}
	conn.WriteMessage(websocket.TextMessage, req.Payload) // Conn.WriteMessage 已加锁
	return &pb.PushResponse{}, nil
}

func (s *GatewayServer) Kick(ctx context.Context, req *pb.KickRequest) (*pb.KickResponse, error) {
	conn := s.hub.Get(req.UserId)
	if conn != nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(4001, "kicked"))
		conn.Close()
		s.hub.Remove(req.UserId, conn)
	}
	return &pb.KickResponse{}, nil
}

func (s *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/ws" {
		s.handleWSUpgrade(w, r)
		return
	}
	http.NotFound(w, r)
}

func (s *GatewayServer) handleWSUpgrade(w http.ResponseWriter, r *http.Request) {
	wsConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn := &Conn{id: connSeq.Add(1), ws: wsConn}
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
	myRoute := redisstore.RouteValue(s.cfg.GatewayGRPC, conn.id)

	// 用分布式锁串行化同一 uid 的上线换绑：临界区内只做"读旧路由 + 写新路由 +
	// 登记本地 Hub"这三步快操作，慢 IO（踢前任的 gRPC）放到锁外，
	// 保证持锁时间极短。锁能从根上杜绝两条连接同时上线导致的路由/Hub 错位。
	lockToken, ok, err := s.redis.AcquireLock(context.Background(), uid)
	if err != nil || !ok {
		// 抢锁失败：同一 uid 正在别处上线（极罕见）。直接断开，客户端会重连重试。
		conn.Close()
		return
	}
	oldRoute, _ := s.redis.GetRoute(context.Background(), uid)
	s.redis.SetRoute(context.Background(), uid, myRoute)
	s.hub.Add(uid, conn)
	s.redis.ReleaseLock(context.Background(), uid, lockToken)

	// 锁外踢前任：慢 IO 不占锁。旧路由指向别的连接才需要踢。
	if oldRoute != "" && oldRoute != myRoute {
		s.kickByRoute(uid, oldRoute)
	}
	log.Printf("用户 %d 已连接", uid)
	s.handleWS(conn, uid, myRoute)
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

// kickByRoute 踢掉 oldRoute 指向的旧连接。若旧路由就在本机则走本地 Kick，
// 否则 gRPC 到对应网关。oldRoute 形如 "gatewayAddr#connID"。
func (s *GatewayServer) kickByRoute(uid int64, oldRoute string) {
	oldAddr := redisstore.GatewayAddrOf(oldRoute)
	if oldAddr == "" {
		return
	}
	if oldAddr == s.cfg.GatewayGRPC {
		// 旧连接在本机：本机新旧连接的换绑已由 hub.Add 在锁内完成
		// （踢掉旧 conn、装入新 conn），这里无需再动，否则可能误踢新连接。
		return
	}
	conn, err := grpc.NewClient(oldAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	defer conn.Close()
	client := pb.NewGatewayServiceClient(conn)
	client.Kick(context.Background(), &pb.KickRequest{UserId: uid})
}
