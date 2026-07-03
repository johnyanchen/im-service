package main

import (
	"context"
	"fmt"
	"time"

	"im-service/pkg/kafka"
	pb "im-service/proto"
)

func (s *Server) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}

	convID := req.ConversationId
	// 单聊首条：没有 conversation_id、只带 peer_id，此刻才惰性建会话。
	// 没人说话就没有会话，避免"点开好友就在对方列表上屏一条空会话"。
	if convID == 0 {
		if req.PeerId == 0 {
			return nil, fmt.Errorf("missing conversation_id or peer_id")
		}
		// 单聊必须是好友才能发消息。
		ok, err := s.db.AreFriends(ctx, uid, req.PeerId)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("对方不是你的好友")
		}
		convID, err = s.findOrCreateDM(ctx, uid, req.PeerId)
		if err != nil {
			return nil, err
		}
	}

	ok, err := s.db.IsMember(ctx, convID, uid)
	if err != nil || !ok {
		return nil, fmt.Errorf("not a member of this conversation")
	}
	// 已有会话继续发：若是单聊，同样校验双方仍是好友（删好友后不能再在老会话发消息）。
	if peer, ok, _ := s.db.GetDMPeer(ctx, convID, uid); ok {
		areFriends, err := s.db.AreFriends(ctx, uid, peer)
		if err != nil {
			return nil, err
		}
		if !areFriends {
			return nil, fmt.Errorf("对方不是你的好友")
		}
	}
	var username string
	s.db.Pool.QueryRow(ctx, "SELECT username FROM users WHERE id=$1", uid).Scan(&username)

	msg, err := s.db.CreateMessage(ctx, convID, uid, req.Content)
	if err != nil {
		return nil, err
	}

	meta, _ := s.db.GetConversationMeta(ctx, convID, uid)
	convType, convName := "", ""
	if meta != nil {
		convType = meta.Type
		convName = meta.Name
	}

	_ = s.producer.PublishFanout(&kafka.FanoutEvent{
		EventType:        kafka.EventNewMessage,
		MessageID:        msg.ID,
		ConversationID:   msg.ConversationID,
		ConversationType: convType,
		ConversationName: convName,
		FromID:           uid,
		FromUsername:     username,
		Content:          msg.Content,
		CreatedAt:        msg.CreatedAt.UnixMilli(),
	})
	return &pb.SendMessageResponse{MessageId: msg.ID, CreatedAt: msg.CreatedAt.UnixMilli(), ConversationId: convID}, nil
}

// findOrCreateDM 返回 uid 与 peer 的单聊会话 id，不存在则新建并登记双方成员。
// 会话在此刻（首条消息落库前）才诞生；会话视图行交给随后的 message fanout
// 惰性 upsert，双方都靠这条消息第一次看到会话，无需单独的 conv_created 事件。
func (s *Server) findOrCreateDM(ctx context.Context, uid, peerID int64) (int64, error) {
	if convID, err := s.db.FindDMConversation(ctx, uid, peerID); err == nil {
		return convID, nil
	}
	convID, err := s.db.CreateConversation(ctx, "dm")
	if err != nil {
		return 0, err
	}
	_ = s.db.AddConversationMember(ctx, convID, uid)
	_ = s.db.AddConversationMember(ctx, convID, peerID)
	return convID, nil
}

func (s *Server) CreateDM(ctx context.Context, req *pb.CreateDMRequest) (*pb.CreateDMResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	// 只返回已存在的会话；不存在则返回 0，前端进入草稿会话。
	// 同时把 uid 侧的视图行恢复为未删除（重新加好友后从联系人进入时触发）。
	convID, _ := s.db.FindDMConversation(ctx, uid, req.PeerId)
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
	// 单聊拉历史同样校验好友关系；min_visible 让删好友再加回的一方看不到旧历史。
	if peer, isDM, _ := s.db.GetDMPeer(ctx, req.ConversationId, uid); isDM {
		areFriends, err := s.db.AreFriends(ctx, uid, peer)
		if err != nil {
			return nil, err
		}
		if !areFriends {
			return nil, fmt.Errorf("对方不是你的好友")
		}
	}
	minVisible, _ := s.db.GetMinVisibleMsgID(ctx, uid, req.ConversationId)
	msgs, err := s.db.GetMessagesBefore(ctx, req.ConversationId, req.BeforeId, limit, minVisible)
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
	// 为所有成员插入会话视图行，使新群立即出现在各自的会话列表里
	_ = s.db.SeedUserConversation(ctx, uid, convID, "group", req.Name)
	for _, mid := range req.MemberIds {
		_ = s.db.SeedUserConversation(ctx, mid, convID, "group", req.Name)
	}
	allMembers := append([]int64{uid}, req.MemberIds...)
	_ = s.redis.SetMembers(ctx, convID, allMembers)
	// 实时推送新群给所有在线成员，立即出现在会话列表
	var creatorName string
	if me, _ := s.db.GetUserByID(ctx, uid); me != nil {
		creatorName = me.Username
	}
	_ = s.producer.PublishFanout(&kafka.FanoutEvent{
		EventType:        kafka.EventConvCreated,
		ConversationID:   convID,
		ConversationType: "group",
		ConversationName: req.Name,
		FromID:           uid,
		FromUsername:     creatorName,
		CreatedAt:        time.Now().UnixMilli(),
		Members:          allMembers,
	})
	return &pb.CreateGroupResponse{GroupId: groupID, ConversationId: convID}, nil
}
