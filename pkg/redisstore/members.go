package redisstore

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

func membersKey(convID int64) string {
	return fmt.Sprintf("conv:%d:members", convID)
}

func (s *Store) GetMembers(ctx context.Context, convID int64) ([]int64, error) {
	vals, err := s.Client.SMembers(ctx, membersKey(convID)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, redis.Nil
	}
	ids := make([]int64, 0, len(vals))
	for _, v := range vals {
		id, _ := strconv.ParseInt(v, 10, 64)
		ids = append(ids, id)
	}
	return ids, nil
}

func (s *Store) SetMembers(ctx context.Context, convID int64, userIDs []int64) error {
	key := membersKey(convID)
	pipe := s.Client.Pipeline()
	pipe.Del(ctx, key)
	if len(userIDs) > 0 {
		members := make([]interface{}, len(userIDs))
		for i, id := range userIDs {
			members[i] = id
		}
		pipe.SAdd(ctx, key, members...)
	}
	_, err := pipe.Exec(ctx)
	return err
}
