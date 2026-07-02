package redisstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// 上线换绑用的分布式锁：同一 uid 的并发上线互斥，保证
// "读旧路由 → 写新路由 → 登记本地" 这段临界区串行执行，从根上避免错位。
// 临界区只做本地/Redis 快操作，慢 IO（踢前任的 gRPC）放在锁外，
// 因此持锁时间是微秒级，lockTTL 只作崩溃兜底，不会被正常业务顶穿。
const lockTTL = 3 * time.Second

func lockKey(userID int64) string {
	return "user:lock:" + strconv.FormatInt(userID, 10)
}

// unlockScript 仅在锁仍是自己持有（value == token）时才释放，
// 避免自己的锁已超时、被他人重新持有后，误删他人的锁。
var unlockScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

// AcquireLock 尝试获取 uid 的上线锁，成功返回一个释放用的 token。
// 未抢到（已有并发上线）返回 ok=false，调用方应放弃本次换绑。
func (s *Store) AcquireLock(ctx context.Context, userID int64) (token string, ok bool, err error) {
	token = randToken()
	ok, err = s.Client.SetNX(ctx, lockKey(userID), token, lockTTL).Result()
	if err != nil {
		return "", false, err
	}
	return token, ok, nil
}

// ReleaseLock 释放锁，仅当锁仍由本 token 持有时才删。
func (s *Store) ReleaseLock(ctx context.Context, userID int64, token string) error {
	return unlockScript.Run(ctx, s.Client, []string{lockKey(userID)}, token).Err()
}

func randToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
