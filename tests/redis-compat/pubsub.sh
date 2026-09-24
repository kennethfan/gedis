#!/usr/bin/env bash
# SUBSCRIBE/UNSUBSCRIBE/PUBLISH 与真 Redis 的输出对照：同一命令序列分别发往
# gedis 与 redis-server，逐行 diff。订阅需长连接：单连接序列经 stdin 管道，
# 跨连接场景用 python3 原始 socket（两个连接）保证确定性。
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

# arity 文案对照（SUBSCRIBE 无参 / PUBLISH 参数过多过少）
run_both SUBSCRIBE
run_both PUBLISH onlyone
run_both PUBLISH a b c

# 零订阅裸 UNSUBSCRIBE → 单个 [unsubscribe nil 0]
run_both UNSUBSCRIBE

# MULTI 内 SUBSCRIBE 照常排队，EXEC 结果只装确认（单频道，无带外推送）
txn_seq() {
  echo "### TXN $1" >> "$GEDIS_OUT"
  echo "### TXN $1" >> "$REDIS_OUT"
  printf '%b' "$2" | redis-cli -p "$GEDIS_PORT" >> "$GEDIS_OUT" 2>&1 || true
  printf '%b' "$2" | redis-cli -p "$REDIS_PORT" >> "$REDIS_OUT" 2>&1 || true
}
txn_seq "sub-in-multi" "MULTI\nSUBSCRIBE qc\nEXEC\n"

# 跨连接主场景：订阅确认 / 重复订阅 / 推送 / 计数 / 订阅态 PING /
# 订阅态拒绝 / 退订 / 裸退订逆序 / 退订后恢复普通命令。
cross_conn() {
  echo "### CROSS-CONN" >> "$2"
  python3 - "$1" >> "$2" 2>&1 <<'EOF' || true
import socket, sys
port = int(sys.argv[1])
sa = socket.create_connection(('127.0.0.1', port))
sb = socket.create_connection(('127.0.0.1', port))
fa = sa.makefile('rb')
fb = sb.makefile('rb')
def raw(f):
    line = f.readline().decode().rstrip('\r\n')
    if line.startswith('*'):
        n = int(line[1:])
        if n < 0:
            return [line]
        out = [line]
        for _ in range(n):
            out.extend(raw(f))
        return out
    if line.startswith('$'):
        n = int(line[1:])
        if n < 0:
            return [line]
        return [line, f.readline().decode().rstrip('\r\n')]
    return [line]
def cmd(f, s, *a):
    s.sendall(("*%d\r\n" % len(a)).encode() + b"".join(("$%d\r\n%s\r\n" % (len(x.encode()), x)).encode() for x in a))
    return raw(f)
def show(label, lines):
    print(label, *lines, sep='\n')
show("A SUB c1 c2:", cmd(fa, sa, "SUBSCRIBE", "c1", "c2"))
show("A SUB c1 c2 #2:", raw(fa))
show("A DUP SUB c1:", cmd(fa, sa, "SUBSCRIBE", "c1"))
show("B PUBLISH c1 hello:", cmd(fb, sb, "PUBLISH", "c1", "hello"))
show("A gets message:", raw(fa))
show("B PUBLISH noone:", cmd(fb, sb, "PUBLISH", "noone", "v"))
show("A PING:", cmd(fa, sa, "PING"))
show("A PING hello:", cmd(fa, sa, "PING", "hello"))
show("A PING a b:", cmd(fa, sa, "PING", "a", "b"))
show("A SET in sub mode:", cmd(fa, sa, "SET", "k", "v"))
show("A PUBLISH in sub mode:", cmd(fa, sa, "PUBLISH", "c1", "hi"))
show("A UNSUB c1:", cmd(fa, sa, "UNSUBSCRIBE", "c1"))
show("B PUBLISH c1 after unsub:", cmd(fb, sb, "PUBLISH", "c1", "v"))
show("B PUBLISH c2 still subbed:", cmd(fb, sb, "PUBLISH", "c2", "v"))
show("A gets c2 message:", raw(fa))
show("A bare UNSUBSCRIBE:", cmd(fa, sa, "UNSUBSCRIBE"))
show("A SET after unsub-all:", cmd(fa, sa, "SET", "k", "v"))
show("A PUBLISH after unsub-all:", cmd(fa, sa, "PUBLISH", "c2", "v"))
show("A GET k:", cmd(fa, sa, "GET", "k"))
EOF
}
cross_conn "$GEDIS_PORT" "$GEDIS_OUT"
cross_conn "$REDIS_PORT" "$REDIS_OUT"

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "PUBSUB COMPAT OK"
else
  echo "PUBSUB COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
