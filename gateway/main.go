package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/config"
	"im-service/pkg/redisstore"
	pb "im-service/proto"
)

func main() {
	cfg := config.Load()
	redis := redisstore.New(cfg.RedisAddr)

	logicConn, err := grpc.NewClient(cfg.LogicGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect logic: %v", err)
	}
	defer logicConn.Close()
	logicClient := pb.NewLogicServiceClient(logicConn)

	srv := NewGatewayServer(cfg, redis, logicClient)
	srv.StartGRPC()

	log.Printf("gateway HTTP/WS 监听 %s", cfg.WebSocketAddr)
	if err := http.ListenAndServe(cfg.WebSocketAddr, srv); err != nil {
		log.Fatalf("http: %v", err)
	}
}
