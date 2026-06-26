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
