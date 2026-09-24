#!/usr/bin/env bash
# ZSet 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 已知且被 #15 接受的差异不在此脚本覆盖：ZADD/ZINCRBY 的 ±Inf（gedis 拒绝，Redis 接受）。
# ZRANDMEMBER 随机顺序由 Go 单测覆盖，不在此 diff。
set -euo pipefail

GEDIS_PORT=6391
REDIS_PORT=6392
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
GEDIS_OUT="$TMP/gedis.out"
REDIS_OUT="$TMP/redis.out"

cleanup() {
  kill "$GEDIS_PID" "$REDIS_PID" 2>/dev/null || true
  rm -rf "$TMP"
}
trap cleanup EXIT

cd "$ROOT"
CGO_ENABLED=0 go build -trimpath -o "$TMP/gedis" ./cmd/gedis

cat > "$TMP/gedis.toml" <<EOF
[server]
host = "127.0.0.1"
port = $GEDIS_PORT
[storage]
datadir = "$TMP/gdata"
[memory]
maxmemory = 1073741824
policy = "allkeys-lru"
[persistence]
appendonly = false
fsync = "everysec"
[metrics]
enabled = false
port = 0
EOF

"$TMP/gedis" -config "$TMP/gedis.toml" >/dev/null 2>&1 &
GEDIS_PID=$!
redis-server --port "$REDIS_PORT" --save '' --appendonly no --daemonize no >/dev/null 2>&1 &
REDIS_PID=$!

for i in $(seq 1 50); do
  if redis-cli -p "$GEDIS_PORT" ping >/dev/null 2>&1 && redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

run_both() {
  echo "### $*" >> "$GEDIS_OUT"
  echo "### $*" >> "$REDIS_OUT"
  redis-cli -p "$GEDIS_PORT" "$@" >> "$GEDIS_OUT" 2>&1 || true
  redis-cli -p "$REDIS_PORT" "$@" >> "$REDIS_OUT" 2>&1 || true
}

# 写入与基础读
run_both ZADD z 1 a 2 b 3 c 4 d
run_both ZADD z 5 a
run_both ZCARD z
run_both ZCARD missing
run_both ZSCORE z a
run_both ZSCORE z nope
run_both ZSCORE missing a
run_both ZMSCORE z a nope
run_both ZRANK z a
run_both ZRANK z d
run_both ZREVRANK z a
run_both ZRANK z b WITHSCORE
run_both ZRANK z nope
run_both ZCOUNT z 1 3
run_both ZCOUNT z "(1" 3
run_both ZCOUNT z -inf +inf
run_both ZINCRBY z 2.5 b

# ZADD 修饰符
run_both ZADD z NX 9 a 3 c2
run_both ZADD z XX 9 a 5 c3
run_both ZADD z GT 0 a
run_both ZADD z LT 99 a
run_both ZADD z CH 10 a 11 e
run_both ZADD z INCR 1 e
run_both ZADD z GT 5 a LT 1 b

# ZRANGE 新语法 + 老命令
run_both ZRANGE z 0 2
run_both ZRANGE z 0 -1
run_both ZRANGE z 0 2 REV
run_both ZRANGE z 1 3 BYSCORE
run_both ZRANGE z 1 3 BYSCORE WITHSCORES
run_both ZRANGE z "-" "[b" BYLEX
run_both ZRANGE z "(a" "+" BYLEX LIMIT 1 2
run_both ZRANGE z 3 1 BYSCORE REV
run_both ZREVRANGE z 0 1
run_both ZREVRANGE z 0 1 WITHSCORES
run_both ZRANGEBYSCORE z 2 4
run_both ZRANGEBYSCORE z 2 4 WITHSCORES LIMIT 0 1
run_both ZREVRANGEBYSCORE z 4 2
run_both ZRANGEBYLEX z "-" "[c"
run_both ZRANGEBYLEX z "-" "+" LIMIT 1 2
run_both ZREVRANGEBYLEX z "+" "(a"
run_both ZRANGE missing 0 -1
run_both ZRANGEBYSCORE missing 0 1

# 交并差
run_both ZADD z2 3 b 4 f
run_both ZUNION 2 z z2
run_both ZUNION 2 z z2 WITHSCORES
run_both ZUNION 2 z z2 WEIGHTS 2 3 AGGREGATE MAX WITHSCORES
run_both ZINTER 2 z z2
run_both ZINTER 2 z z2 AGGREGATE MIN WITHSCORES
run_both ZDIFF 2 z z2
run_both ZUNIONSTORE out 2 z z2
run_both ZINTERSTORE out2 2 z z2 WEIGHTS 2 3 AGGREGATE MAX
run_both ZDIFFSTORE out3 2 z z2
run_both ZRANGE out 0 -1 WITHSCORES
run_both ZSCORE out2 b
run_both ZRANGESTORE out4 z 0 2
run_both ZRANGE out4 0 -1

# 删除与弹出
run_both ZREM z c2 nope
run_both ZPOPMIN z
run_both ZPOPMAX z 2
run_both ZPOPMIN missing
run_both ZREMRANGEBYRANK z 0 0
run_both ZREMRANGEBYSCORE z 10 99
run_both ZREMRANGEBYLEX z "[z" "+"

# SCAN / 阻塞（命中走即时路径；超时各 1 秒）
run_both ZSCAN z 0
run_both ZSCAN z 0 MATCH "a*"
run_both BZPOPMIN z 1
run_both BZPOPMAX z2 1
run_both BZMPOP 1 2 z z2 MIN COUNT 2
run_both BZPOPMIN missing 1

# 通用语义
run_both TYPE z
run_both OBJECT ENCODING z
run_both EXPIRE z 100
run_both TTL z
run_both KEYS "z*"
run_both DEL z
run_both ZCARD z

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "ZSET COMPAT OK"
else
  echo "ZSET COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
