#!/usr/bin/env bash
# WATCH/UNWATCH 乐观锁与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 单连接序列经一次 stdin 管道发给 redis-cli；跨连接中止场景用 python3 原始 socket（两个连接）保证确定性。
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

# 无触碰提交
txn_seq "watch-commit" "SET wk1 base\nWATCH wk1\nMULTI\nSET wk1 new\nEXEC\n"
run_both GET wk1

# UNWATCH 后他人修改仍提交
txn_seq "unwatch-commit" "SET wk2 base\nWATCH wk2\nUNWATCH\nMULTI\nSET wk2 new\nEXEC\n"
run_both GET wk2

# MULTI 内 WATCH 被拒且不污染事务
txn_seq "watch-in-multi" "MULTI\nWATCH wk1\nSET wk3 v\nEXEC\n"
run_both GET wk3

# 裸 UNWATCH 回 OK；裸 EXEC 报错
txn_seq "bare-unwatch" "UNWATCH\n"
txn_seq "bare-exec" "EXEC\n"

# WATCH 无参 / 参数过多与真 Redis 文案对齐
txn_seq "watch-arity" "WATCH\nWATCH a b\n"

# 跨连接中止与 UNWATCH 救回：双连接原始 socket，输出逐行 diff。
cross_conn() {
  echo "### CROSS-CONN" >> "$2"
  python3 - "$1" >> "$2" 2>&1 <<'EOF' || true
import socket, sys
port = int(sys.argv[1])
sa = socket.create_connection(('127.0.0.1', port))
sb = socket.create_connection(('127.0.0.1', port))
fa, fb = sa.makefile('r'), sb.makefile('r')
def cmd(f, s, *a):
    s.sendall(("*%d\r\n" % len(a)).encode() + b"".join(("$%d\r\n%s\r\n" % (len(x.encode()), x)).encode() for x in a))
    return f.readline().strip()
print(cmd(fa, sa, "WATCH", "wa"))
print(cmd(fb, sb, "SET", "wa", "changed"))
print(cmd(fa, sa, "MULTI"))
print(cmd(fa, sa, "SET", "wb", "v"))
print(cmd(fa, sa, "EXEC"))
print(cmd(fa, sa, "GET", "wb"))
print(cmd(fa, sa, "WATCH", "wc"))
print(cmd(fa, sa, "UNWATCH"))
print(cmd(fb, sb, "SET", "wc", "changed"))
print(cmd(fa, sa, "MULTI"))
print(cmd(fa, sa, "SET", "wd", "v"))
print(cmd(fa, sa, "EXEC"))
print(cmd(fa, sa, "GET", "wd"))
EOF
}
cross_conn "$GEDIS_PORT" "$GEDIS_OUT"
cross_conn "$REDIS_PORT" "$REDIS_OUT"

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "WATCH COMPAT OK"
else
  echo "WATCH COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
