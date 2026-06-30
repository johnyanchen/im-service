package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	conn.WriteMessage(websocket.TextMessage, req.Payload) // Conn.WriteMessage 已加锁
	return &pb.PushResponse{}, nil
}

func (s *GatewayServer) Kick(ctx context.Context, req *pb.KickRequest) (*pb.KickResponse, error) {
	conn := s.hub.Get(req.UserId)
	if conn != nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(4001, "kicked"))
		conn.Close()
		s.hub.Remove(req.UserId)
	}
	return &pb.KickResponse{}, nil
}

func (s *GatewayServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/ws":
		s.handleWSUpgrade(w, r)
	case r.URL.Path == "/api/login" && r.Method == "POST":
		var req pb.LoginRequest
		json.NewDecoder(r.Body).Decode(&req)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		resp, err := s.logic.Login(ctx, &req)
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
	case r.URL.Path == "/api/users" && r.Method == "GET":
		s.handleListUsers(w, r)
	case r.URL.Path == "/api/messages" && r.Method == "POST":
		s.handleSendMessage(w, r)
	case r.URL.Path == "/api/sync" && r.Method == "GET":
		s.handleSync(w, r)
	case r.URL.Path == "/api/conversations/dm" && r.Method == "POST":
		s.handleCreateDM(w, r)
	case r.URL.Path == "/api/conversations/group" && r.Method == "POST":
		s.handleCreateGroup(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/conversations/") && strings.HasSuffix(r.URL.Path, "/read") && r.Method == "POST":
		s.handleMarkRead(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/conversations/") && strings.HasSuffix(r.URL.Path, "/messages") && r.Method == "GET":
		s.handleGetMessages(w, r)
	case r.URL.Path == "/api/friends" && r.Method == "GET":
		s.handleListFriends(w, r)
	case r.URL.Path == "/api/friends/requests" && r.Method == "GET":
		s.handleListFriendRequests(w, r)
	case r.URL.Path == "/api/friends/request" && r.Method == "POST":
		s.handleSendFriendRequest(w, r)
	case r.URL.Path == "/api/friends/handle" && r.Method == "POST":
		s.handleHandleFriendRequest(w, r)
	case r.URL.Path == "/api/invite-code" && r.Method == "GET":
		s.handleGetInviteCode(w, r)
	case r.URL.Path == "/api/invite-code/refresh" && r.Method == "POST":
		s.handleRefreshInviteCode(w, r)
	case r.URL.Path == "/api/friends/add-by-code" && r.Method == "POST":
		s.handleAddFriendByCode(w, r)
	default:
		http.FileServer(http.Dir("web")).ServeHTTP(w, r)
	}
}

func (s *GatewayServer) handleWSUpgrade(w http.ResponseWriter, r *http.Request) {
	wsConn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn := &Conn{ws: wsConn}
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
	s.kickRemote(uid)
	s.hub.Add(uid, conn)
	s.redis.SetRoute(context.Background(), uid, s.cfg.GatewayGRPC)
	log.Printf("用户 %d 已连接", uid)
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

func (s *GatewayServer) kickRemote(uid int64) {
	oldAddr, err := s.redis.GetRoute(context.Background(), uid)
	if err != nil || oldAddr == "" || oldAddr == s.cfg.GatewayGRPC {
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

func (s *GatewayServer) extractToken(r *http.Request) string {
	token := r.Header.Get("Authorization")
	if len(token) > 7 && token[:7] == "Bearer " {
		return token[7:]
	}
	return ""
}

func (s *GatewayServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		ConversationID int64  `json:"conversation_id"`
		Content        string `json:"content"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.SendMessage(r.Context(), &pb.SendMessageRequest{
		Token: token, ConversationId: body.ConversationID, Content: body.Content,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleSync(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	lastSyncAt, _ := strconv.ParseInt(r.URL.Query().Get("last_sync_at"), 10, 64)
	resp, err := s.logic.Sync(r.Context(), &pb.SyncRequest{Token: token, LastSyncAt: lastSyncAt})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleMarkRead(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	// path: /api/conversations/{id}/read
	parts := strings.Split(r.URL.Path, "/")
	convID, _ := strconv.ParseInt(parts[3], 10, 64)
	var body struct {
		MsgID int64 `json:"msg_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	_, err := s.logic.MarkRead(r.Context(), &pb.MarkReadRequest{
		Token: token, ConversationId: convID, MsgId: body.MsgID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *GatewayServer) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	// path: /api/conversations/{id}/messages
	parts := strings.Split(r.URL.Path, "/")
	convID, _ := strconv.ParseInt(parts[3], 10, 64)
	beforeID, _ := strconv.ParseInt(r.URL.Query().Get("before_id"), 10, 64)
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 32)
	resp, err := s.logic.GetMessages(r.Context(), &pb.GetMessagesRequest{
		Token: token, ConversationId: convID, BeforeId: beforeID, Limit: int32(limit),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleCreateDM(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		PeerID int64 `json:"peer_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.CreateDM(r.Context(), &pb.CreateDMRequest{Token: token, PeerId: body.PeerID})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		Name      string  `json:"name"`
		MemberIDs []int64 `json:"member_ids"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.CreateGroup(r.Context(), &pb.CreateGroupRequest{
		Token: token, Name: body.Name, MemberIds: body.MemberIDs,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleListUsers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	resp, err := s.logic.ListUsers(r.Context(), &pb.ListUsersRequest{Token: token})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleListFriends(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	resp, err := s.logic.ListFriends(r.Context(), &pb.ListFriendsRequest{Token: token})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleListFriendRequests(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	resp, err := s.logic.ListFriendRequests(r.Context(), &pb.ListFriendRequestsRequest{Token: token})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleSendFriendRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		ToID int64 `json:"to_id"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.SendFriendRequest(r.Context(), &pb.SendFriendRequestReq{Token: token, ToId: body.ToID})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleHandleFriendRequest(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		RequestID int64 `json:"request_id"`
		Accept    bool  `json:"accept"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.HandleFriendRequest(r.Context(), &pb.HandleFriendRequestReq{Token: token, RequestId: body.RequestID, Accept: body.Accept})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleGetInviteCode(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	resp, err := s.logic.GetInviteCode(r.Context(), &pb.GetInviteCodeRequest{Token: token})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleRefreshInviteCode(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	resp, err := s.logic.RefreshInviteCode(r.Context(), &pb.GetInviteCodeRequest{Token: token})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}

func (s *GatewayServer) handleAddFriendByCode(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	token := s.extractToken(r)
	if token == "" {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
		return
	}
	var body struct {
		Code string `json:"code"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	resp, err := s.logic.AddFriendByCode(r.Context(), &pb.AddFriendByCodeRequest{Token: token, Code: body.Code})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	json.NewEncoder(w).Encode(resp)
}
