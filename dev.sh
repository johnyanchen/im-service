#!/bin/bash
set -e
cd "$(dirname "$0")"

stop() {
  echo "停止服务..."
  pkill -f "go run ./logic"    2>/dev/null || true
  pkill -f "go run ./fanout"   2>/dev/null || true
  pkill -f "go run ./gateway"  2>/dev/null || true
  pkill -f "go run ./apigateway" 2>/dev/null || true
  pkill -f "vite"              2>/dev/null || true
  # 等编译出的子进程也退出
  sleep 1
  pkill -f "/exe/logic"       2>/dev/null || true
  pkill -f "/exe/fanout"      2>/dev/null || true
  pkill -f "/exe/gateway"     2>/dev/null || true
  pkill -f "/exe/apigateway"  2>/dev/null || true
  # go build 临时二进制
  pkill -f "im-service/logic"      2>/dev/null || true
  pkill -f "im-service/fanout"     2>/dev/null || true
  pkill -f "im-service/gateway"    2>/dev/null || true
  pkill -f "im-service/apigateway" 2>/dev/null || true
}

if [ "$1" = "stop" ]; then
  stop
  echo "已停止"
  exit 0
fi

stop

# 跑还未执行的 migration（幂等：列已存在会报 duplicate，忽略即可）
echo "应用 migrations..."
for f in migrations/00*.sql migrations/0[0-9][0-9]_*.sql; do
  [ -f "$f" ] || continue
  docker compose exec -T postgres psql -U im -d im -f /dev/stdin < "$f" 2>/dev/null || true
done
# 单独跑，保证顺序
for f in $(ls migrations/*.sql 2>/dev/null | sort); do
  docker compose exec -T postgres psql -U im -d im -f /dev/stdin < "$f" 2>/dev/null || true
done

LOG_DIR=/tmp/im-logs
mkdir -p "$LOG_DIR"

echo "启动服务（日志在 $LOG_DIR）..."
go run ./logic      > "$LOG_DIR/logic.log"      2>&1 &
go run ./fanout     > "$LOG_DIR/fanout.log"     2>&1 &
go run ./gateway    > "$LOG_DIR/gateway.log"    2>&1 &
go run ./apigateway > "$LOG_DIR/apigateway.log" 2>&1 &
cd frontend && npm run dev > "$LOG_DIR/frontend.log" 2>&1 &
cd ..

echo "全部启动，tail 日志："
echo "  tail -f $LOG_DIR/logic.log"
echo "  tail -f $LOG_DIR/apigateway.log"
echo "  tail -f $LOG_DIR/fanout.log"
echo "  tail -f $LOG_DIR/gateway.log"
echo "  tail -f $LOG_DIR/frontend.log"
echo ""
echo "停止：bash dev.sh stop"

# 汇总日志到前台，Ctrl+C 退出 tail 但不杀服务
tail -f "$LOG_DIR/logic.log" "$LOG_DIR/apigateway.log" "$LOG_DIR/fanout.log" "$LOG_DIR/gateway.log" "$LOG_DIR/frontend.log"
