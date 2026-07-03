package main

import (
	"context"
	"fmt"
	"time"

	"im-service/pkg/kafka"
	pb "im-service/proto"
)

func (s *Server) SendFriendRequest(ctx context.Context, req *pb.SendFriendRequestReq) (*pb.SendFriendRequestResp, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	if uid == req.ToId {
		return nil, fmt.Errorf("cannot add yourself")
	}
	already, err := s.db.AreFriends(ctx, uid, req.ToId)
	if err != nil {
		return nil, err
	}
	if already {
		return nil, fmt.Errorf("already friends")
	}
	reqID, err := s.db.CreateFriendRequest(ctx, uid, req.ToId)
	if err != nil {
		return nil, err
	}
	s.publishFriendEvent(ctx, req.ToId, uid, reqID, kafka.FriendActionRequest)
	return &pb.SendFriendRequestResp{}, nil
}

func (s *Server) HandleFriendRequest(ctx context.Context, req *pb.HandleFriendRequestReq) (*pb.HandleFriendRequestResp, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	fr, err := s.db.GetFriendRequest(ctx, req.RequestId)
	if err != nil {
		return nil, fmt.Errorf("request not found")
	}
	if fr.ToID != uid {
		return nil, fmt.Errorf("not your request")
	}
	if fr.Status != "pending" {
		return nil, fmt.Errorf("request already handled")
	}
	action := kafka.FriendActionRejected
	if req.Accept {
		if err := s.db.AcceptFriendRequest(ctx, fr.ID, fr.FromID, fr.ToID); err != nil {
			return nil, err
		}
		action = kafka.FriendActionAccepted
	} else {
		if err := s.db.RejectFriendRequest(ctx, fr.ID); err != nil {
			return nil, err
		}
	}
	// 通知发起者（fr.FromID）他的申请被同意/拒绝了。推送者是处理人 uid。
	s.publishFriendEvent(ctx, fr.FromID, uid, fr.ID, action)
	return &pb.HandleFriendRequestResp{}, nil
}

// publishFriendEvent 发一条好友事件到 fanout，推给 targetID。
// actor 是触发方（申请人/处理人），用于填充 from_id/from_username。
func (s *Server) publishFriendEvent(ctx context.Context, targetID, actorID, requestID int64, action string) {
	var actorName string
	if u, err := s.db.GetUserByID(ctx, actorID); err == nil {
		actorName = u.Username
	}
	_ = s.producer.PublishFanout(&kafka.FanoutEvent{
		EventType:    kafka.EventFriend,
		TargetID:     targetID,
		FromID:       actorID,
		FromUsername: actorName,
		RequestID:    requestID,
		FriendAction: action,
		CreatedAt:    time.Now().UnixMilli(),
	})
}

func (s *Server) ListFriends(ctx context.Context, req *pb.ListFriendsRequest) (*pb.ListFriendsResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	friends, err := s.db.ListFriends(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*pb.UserItem
	for _, f := range friends {
		items = append(items, &pb.UserItem{Id: f.ID, Username: f.Username})
	}
	return &pb.ListFriendsResponse{Friends: items}, nil
}

func (s *Server) ListFriendRequests(ctx context.Context, req *pb.ListFriendRequestsRequest) (*pb.ListFriendRequestsResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	reqs, err := s.db.ListFriendRequests(ctx, uid)
	if err != nil {
		return nil, err
	}
	var items []*pb.FriendRequestItem
	for _, r := range reqs {
		items = append(items, &pb.FriendRequestItem{
			Id: r.ID, FromId: r.FromID, FromUsername: r.FromUsername,
			Status: r.Status, CreatedAt: r.CreatedAt,
		})
	}
	return &pb.ListFriendRequestsResponse{Requests: items}, nil
}

func (s *Server) GetInviteCode(ctx context.Context, req *pb.GetInviteCodeRequest) (*pb.GetInviteCodeResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	code, err := s.db.GetInviteCode(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &pb.GetInviteCodeResponse{Code: code}, nil
}

func (s *Server) RefreshInviteCode(ctx context.Context, req *pb.GetInviteCodeRequest) (*pb.GetInviteCodeResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	code, err := s.db.RefreshInviteCode(ctx, uid)
	if err != nil {
		return nil, err
	}
	return &pb.GetInviteCodeResponse{Code: code}, nil
}

func (s *Server) AddFriendByCode(ctx context.Context, req *pb.AddFriendByCodeRequest) (*pb.AddFriendByCodeResponse, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	target, err := s.db.GetUserByInviteCode(ctx, req.Code)
	if err != nil {
		return nil, fmt.Errorf("邀请码无效")
	}
	if target.ID == uid {
		return nil, fmt.Errorf("不能添加自己")
	}
	already, err := s.db.AreFriends(ctx, uid, target.ID)
	if err != nil {
		return nil, err
	}
	if already {
		return nil, fmt.Errorf("已经是好友")
	}
	reqID, err := s.db.CreateFriendRequest(ctx, uid, target.ID)
	if err != nil {
		return nil, err
	}
	s.publishFriendEvent(ctx, target.ID, uid, reqID, kafka.FriendActionRequest)
	return &pb.AddFriendByCodeResponse{Username: target.Username}, nil
}

// DeleteFriend 删除好友（单向语义，对齐微信）：解除双向好友关系，
// 只删发起方自己的会话视图行；对方无感，不推送。重新加回后靠 min_visible_msg_id
// 让发起方在复用的老会话里看不到旧历史。
func (s *Server) DeleteFriend(ctx context.Context, req *pb.DeleteFriendReq) (*pb.DeleteFriendResp, error) {
	uid, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	if err := s.db.DeleteFriend(ctx, uid, req.FriendId); err != nil {
		return nil, err
	}
	return &pb.DeleteFriendResp{}, nil
}
