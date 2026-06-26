package main

import (
	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

type Server struct {
	pb.UnimplementedLogicServiceServer
	cfg      *config.Config
	db       *model.DB
	redis    *redisstore.Store
	producer *kafka.Producer
}

func NewServer(cfg *config.Config, db *model.DB, redis *redisstore.Store, producer *kafka.Producer) *Server {
	return &Server{cfg: cfg, db: db, redis: redis, producer: producer}
}
