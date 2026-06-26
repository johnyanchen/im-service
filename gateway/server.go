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
