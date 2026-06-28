package main

import (
	"context"

	pb "im-service/proto"
)

func (s *Server) ListUsers(ctx context.Context, req *pb.ListUsersRequest) (*pb.ListUsersResponse, error) {
	_, err := s.parseToken(req.Token)
	if err != nil {
		return nil, err
	}
	users, err := s.db.ListUsers(ctx)
	if err != nil {
		return nil, err
	}
	resp := &pb.ListUsersResponse{}
	for _, u := range users {
		resp.Users = append(resp.Users, &pb.UserItem{Id: u.ID, Username: u.Username})
	}
	return resp, nil
}
