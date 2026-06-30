package main

import (
	"context"
	"fmt"

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
	if err := s.db.CreateFriendRequest(ctx, uid, req.ToId); err != nil {
		return nil, err
	}
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
	if req.Accept {
		if err := s.db.AcceptFriendRequest(ctx, fr.ID, fr.FromID, fr.ToID); err != nil {
			return nil, err
		}
	} else {
		if err := s.db.RejectFriendRequest(ctx, fr.ID); err != nil {
			return nil, err
		}
	}
	return &pb.HandleFriendRequestResp{}, nil
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
	if err := s.db.CreateFriendRequest(ctx, uid, target.ID); err != nil {
		return nil, err
	}
	return &pb.AddFriendByCodeResponse{Username: target.Username}, nil
}
