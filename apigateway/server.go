package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"im-service/pkg/config"
	pb "im-service/proto"
)

// APIServer 是无状态的 HTTP API 网关：把前端的 /api/* 请求解出 token、
// 转发给 logic，并回写 JSON；同时服务前端静态资源。它不持有任何长连接，
// 可随意水平扩缩、频繁发版，不影响 gateway 上的 WebSocket 长连接。
type APIServer struct {
	cfg   *config.Config
	logic pb.LogicServiceClient
}

func NewAPIServer(cfg *config.Config, logic pb.LogicServiceClient) *APIServer {
	return &APIServer{cfg: cfg, logic: logic}
}

func (s *APIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
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

func (s *APIServer) extractToken(r *http.Request) string {
	token := r.Header.Get("Authorization")
	if len(token) > 7 && token[:7] == "Bearer " {
		return token[7:]
	}
	return ""
}

func (s *APIServer) handleSync(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleSendMessage(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleMarkRead(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleGetMessages(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleCreateDM(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleCreateGroup(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleListUsers(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleListFriends(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleListFriendRequests(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleSendFriendRequest(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleHandleFriendRequest(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleGetInviteCode(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleRefreshInviteCode(w http.ResponseWriter, r *http.Request) {
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

func (s *APIServer) handleAddFriendByCode(w http.ResponseWriter, r *http.Request) {
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
