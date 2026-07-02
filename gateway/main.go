package main

import (
	"log"
	"net/http"

	"im-service/pkg/config"
	"im-service/pkg/redisstore"
)

func main() {
	cfg := config.Load()
	redis := redisstore.New(cfg.RedisAddr)

	srv := NewGatewayServer(cfg, redis)
	srv.StartGRPC()

	log.Printf("gateway WS 监听 %s", cfg.GatewayWSAddr)
	if err := http.ListenAndServe(cfg.GatewayWSAddr, srv); err != nil {
		log.Fatalf("http: %v", err)
	}
}
