package main

import (
	"context"
	"log"
	"net"

	"google.golang.org/grpc"
	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	db, err := model.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	redis := redisstore.New(cfg.RedisAddr)
	producer, err := kafka.NewProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("kafka producer: %v", err)
	}
	defer producer.Close()

	srv := NewServer(cfg, db, redis, producer)
	lis, err := net.Listen("tcp", cfg.LogicGRPC)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterLogicServiceServer(grpcServer, srv)
	log.Printf("logic 服务监听 %s", cfg.LogicGRPC)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
