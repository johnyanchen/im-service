package main

import (
	"log"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"im-service/pkg/config"
	pb "im-service/proto"
)

func main() {
	cfg := config.Load()

	logicConn, err := grpc.NewClient(cfg.LogicGRPC, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("connect logic: %v", err)
	}
	defer logicConn.Close()
	logicClient := pb.NewLogicServiceClient(logicConn)

	srv := NewAPIServer(cfg, logicClient)

	log.Printf("api-gateway HTTP 监听 %s", cfg.WebSocketAddr)
	if err := http.ListenAndServe(cfg.WebSocketAddr, srv); err != nil {
		log.Fatalf("http: %v", err)
	}
}
