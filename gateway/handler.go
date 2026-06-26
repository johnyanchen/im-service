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
