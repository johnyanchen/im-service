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
	limit := req.Limit
	if limit <= 0 {
		limit = 50
	}
	convs, err := s.db.GetUpdatedConversations(ctx, uid, since, limit)
	if err != nil {
		return nil, err
	}
	resp := &pb.SyncResponse{}
	for _, uc := range convs {
		resp.Conversations = append(resp.Conversations, &pb.ConversationState{
			ConversationId: uc.ConversationID,
			Type:           uc.Type,
			LastMsgId:      uc.LastMsgID,
			UnreadCount:    uc.UnreadCount,
			UpdatedAt:      uc.UpdatedAt.UnixMilli(),
			Name:           uc.Name,
			LastMsgContent: uc.LastMsgContent,
			LastMsgFrom:    uc.LastMsgFrom,
		})
	}
	return resp, nil
}

func (s *Server) MarkRead(ctx context.Context, req *pb.MarkReadRequest) (*pb.MarkReadResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	err = s.db.MarkRead(ctx, uid, req.ConversationId, req.MsgId)
	if err != nil {
		return nil, err
	}
	return &pb.MarkReadResponse{}, nil
}
