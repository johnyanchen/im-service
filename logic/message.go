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
	var username string
	s.db.Pool.QueryRow(ctx, "SELECT username FROM users WHERE id=$1", uid).Scan(&username)

	msg, err := s.db.CreateMessage(ctx, req.ConversationId, uid, req.Content)
	if err != nil {
		return nil, err
	}

	meta, _ := s.db.GetConversationMeta(ctx, req.ConversationId, uid)
	convType, convName := "", ""
	if meta != nil {
		convType = meta.Type
		convName = meta.Name
	}

	_ = s.producer.PublishFanout(&kafka.FanoutEvent{
		MessageID:        msg.ID,
		ConversationID:   msg.ConversationID,
		ConversationType: convType,
		ConversationName: convName,
		FromID:           uid,
		FromUsername:     username,
		Content:          msg.Content,
		CreatedAt:        msg.CreatedAt.UnixMilli(),
	})
	return &pb.SendMessageResponse{MessageId: msg.ID, CreatedAt: msg.CreatedAt.UnixMilli()}, nil
}

func (s *Server) CreateDM(ctx context.Context, req *pb.CreateDMRequest) (*pb.CreateDMResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
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

func (s *Server) GetMessages(ctx context.Context, req *pb.GetMessagesRequest) (*pb.GetMessagesResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	ok, err := s.db.IsMember(ctx, req.ConversationId, uid)
	if err != nil || !ok {
		return nil, fmt.Errorf("not a member of this conversation")
	}
	limit := int(req.Limit)
	if limit <= 0 {
		limit = 30
	}
	msgs, err := s.db.GetMessagesBefore(ctx, req.ConversationId, req.BeforeId, limit)
	if err != nil {
		return nil, err
	}
	resp := &pb.GetMessagesResponse{}
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
	return resp, nil
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
	_ = s.db.AddConversationMember(ctx, convID, uid)
	for _, mid := range req.MemberIds {
		_ = s.db.AddConversationMember(ctx, convID, mid)
	}
	allMembers := append([]int64{uid}, req.MemberIds...)
	_ = s.redis.SetMembers(ctx, convID, allMembers)
	return &pb.CreateGroupResponse{GroupId: groupID, ConversationId: convID}, nil
}
