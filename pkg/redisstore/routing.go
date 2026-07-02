package redisstore

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const routeTTL = 5 * time.Minute

// 路由值格式为 "gatewayAddr#connID"，例如 "127.0.0.1:9001#42"。
// gatewayAddr 保证跨网关唯一，connID 保证同网关内新旧连接唯一，
// 二者拼接后全局唯一标识"当前是哪一条连接持有该用户的路由"。
const routeSep = "#"

// delRouteIfScript 仅在路由当前值等于期望值时才删除，
// 是一个 compare-and-delete，避免旧连接退出时删掉别人写入的路由。
var delRouteIfScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0
`)

func routingKey(userID int64) string {
	return fmt.Sprintf("user:%d:gateway", userID)
}

// RouteValue 拼出全局唯一的路由值。
func RouteValue(gatewayAddr string, connID int64) string {
	return fmt.Sprintf("%s%s%d", gatewayAddr, routeSep, connID)
}

// GatewayAddrOf 从路由值中取出网关地址部分，供推送方建立 gRPC 连接。
// 兼容不带 connID 的旧值。
func GatewayAddrOf(routeValue string) string {
	if i := strings.Index(routeValue, routeSep); i >= 0 {
		return routeValue[:i]
	}
	return routeValue
}

// SetRoute 写入路由（覆盖）。routeValue 应由 RouteValue 构造。
// 上线登记和续期都用它——上线时由分布式锁保证同 uid 串行，无需原子换绑。
func (s *Store) SetRoute(ctx context.Context, userID int64, routeValue string) error {
	return s.Client.Set(ctx, routingKey(userID), routeValue, routeTTL).Err()
}

func (s *Store) GetRoute(ctx context.Context, userID int64) (string, error) {
	return s.Client.Get(ctx, routingKey(userID)).Result()
}

func (s *Store) DelRoute(ctx context.Context, userID int64) error {
	return s.Client.Del(ctx, routingKey(userID)).Err()
}

// DelRouteIf 仅在路由当前值仍等于 routeValue 时才删除。
func (s *Store) DelRouteIf(ctx context.Context, userID int64, routeValue string) error {
	return delRouteIfScript.Run(ctx, s.Client, []string{routingKey(userID)}, routeValue).Err()
}
