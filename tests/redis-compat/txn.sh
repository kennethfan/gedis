#!/usr/bin/env bash
# MULTI/EXEC/DISCARD 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 事务需要单连接：每个序列经一次 stdin 管道发给 redis-cli，保证 MULTI 会话存活。
set -euo pipefail

GEDIS_PORT=6397
REDIS_PORT=6398
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

# txn_seq: $1=label, $2=\n 分隔的命令序列，经单连接依次执行。
txn_seq() {
  echo "### TXN $1" >> "$GEDIS_OUT"
  echo "### TXN $1" >> "$REDIS_OUT"
  printf '%b' "$2" | redis-cli -p "$GEDIS_PORT" >> "$GEDIS_OUT" 2>&1 || true
  printf '%b' "$2" | redis-cli -p "$REDIS_PORT" >> "$REDIS_OUT" 2>&1 || true
}

# 基础排队与提交
txn_seq "basic" "MULTI\nSET tk1 v1\nGET tk1\nEXEC\n"
run_both GET tk1

# 空 EXEC
txn_seq "empty-exec" "MULTI\nEXEC\n"

# 嵌套 MULTI 后 DISCARD 收尾
txn_seq "nested" "MULTI\nMULTI\nDISCARD\n"

# 无 MULTI 的 EXEC / DISCARD
txn_seq "no-multi" "EXEC\nDISCARD\n"

# DISCARD 清空队列
txn_seq "discard" "MULTI\nSET td1 x\nDISCARD\n"
txn_seq "exec-after-discard" "EXEC\n"
run_both GET td1

# 未知命令污染会话 → EXECABORT，会话结束
txn_seq "execabort" "MULTI\nSET ta1 ok\nNOSUCHCMD a\nEXEC\n"
txn_seq "exec-after-abort" "EXEC\n"
run_both GET ta1

# 回放期类型错误落进数组，事务继续
run_both SET ts v
txn_seq "runtime-error" "MULTI\nLPUSH ts x\nSET tk2 v2\nEXEC\n"
run_both GET tk2
run_both TYPE ts

# 混合类型回放
txn_seq "mixed" "MULTI\nHSET th f v\nRPUSH tl a b\nSADD tm m\nZADD tz 1 one\nINCR tc\nEXEC\n"
run_both HGETALL th
run_both LRANGE tl 0 -1
run_both SMEMBERS tm
run_both ZRANGE tz 0 -1
run_both GET tc

# 排队期 arity 检查：错了立即报错并污染，EXEC → EXECABORT
txn_seq "arity" "MULTI\nSET onlykey\nGET a b\nMSET ak av\nEXEC\n"
run_both GET onlykey
run_both GET ak

# 连接断开丢弃未提交队列：管道关闭即断开，两侧 GET 都应为 nil
printf 'MULTI\nSET tdisc v\n' | redis-cli -p "$GEDIS_PORT" >/dev/null 2>&1 || true
printf 'MULTI\nSET tdisc v\n' | redis-cli -p "$REDIS_PORT" >/dev/null 2>&1 || true
run_both GET tdisc

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "TXN COMPAT OK"
else
  echo "TXN COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
