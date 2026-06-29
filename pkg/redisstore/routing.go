package redisstore

import (
	"context"
	"fmt"
	"time"
)

const routeTTL = 5 * time.Minute

func routingKey(userID int64) string {
	return fmt.Sprintf("user:%d:gateway", userID)
}

func (s *Store) SetRoute(ctx context.Context, userID int64, gatewayAddr string) error {
	return s.Client.Set(ctx, routingKey(userID), gatewayAddr, routeTTL).Err()
}

func (s *Store) GetRoute(ctx context.Context, userID int64) (string, error) {
	return s.Client.Get(ctx, routingKey(userID)).Result()
}

func (s *Store) DelRoute(ctx context.Context, userID int64) error {
	return s.Client.Del(ctx, routingKey(userID)).Err()
}
