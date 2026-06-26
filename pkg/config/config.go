package config

import "os"

type Config struct {
	PostgresDSN   string
	RedisAddr     string
	KafkaBrokers  []string
	JWTSecret     string
	GatewayGRPC   string
	LogicGRPC     string
	WebSocketAddr string
}

func Load() *Config {
	return &Config{
		PostgresDSN:   getEnv("POSTGRES_DSN", "postgres://im:im@localhost:5432/im?sslmode=disable"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		KafkaBrokers:  []string{getEnv("KAFKA_BROKER", "localhost:9092")},
		JWTSecret:     getEnv("JWT_SECRET", "dev-secret-key"),
		GatewayGRPC:   getEnv("GATEWAY_GRPC", "localhost:9001"),
		LogicGRPC:     getEnv("LOGIC_GRPC", "localhost:9002"),
		WebSocketAddr: getEnv("WS_ADDR", "localhost:8080"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
