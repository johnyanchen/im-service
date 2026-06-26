package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"im-service/pkg/config"
	"im-service/pkg/kafka"
	"im-service/pkg/model"
	"im-service/pkg/redisstore"
)

func main() {
	cfg := config.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := model.NewDB(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer db.Close()

	redisStore := redisstore.New(cfg.RedisAddr)
	processor := NewFanoutProcessor(db, redisStore)

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, "fanout-group", processor.Handle)
	if err != nil {
		log.Fatalf("kafka consumer: %v", err)
	}
	defer consumer.Close()

	log.Println("fanout worker 已启动")
	if err := consumer.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("consumer: %v", err)
	}
	log.Println("fanout worker 已停止")
}
