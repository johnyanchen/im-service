package redisstore

import "github.com/redis/go-redis/v9"

type Store struct {
	Client *redis.Client
}

func New(addr string) *Store {
	return &Store{
		Client: redis.NewClient(&redis.Options{Addr: addr}),
	}
}
