#!/usr/bin/env bash
# Stream 消费组 + PEL（XREADGROUP/BLOCK/NOACK/XACK/XPENDING/XCLAIM/XAUTOCLAIM）
# 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 归一化（双方输出同样处理）：13 位以上 ms 的自动 ID 整行屏蔽；idle/seen-time/
# active-time/inactive 取值随时间变化只比结构；缩进的 "(integer)"（XPENDING range
# 的 idle 位、FULL 视图 pending 里的投递时刻位、XPENDING 摘要的按消费者计数）
# 只比存在性；radix-tree-keys/nodes 为内部实现细节只比存在性。
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

# XREADGROUP 基础投递 + PEL
run_both XADD g 100-1 f a
run_both XADD g 100-2 f b
run_both XADD g 100-3 f c
run_both XGROUP CREATE g grp 0
run_both XREADGROUP GROUP grp c1 STREAMS g ">"
run_both XPENDING g grp - + 10
run_both XPENDING g grp
run_both XINFO GROUPS g
run_both XINFO CONSUMERS g c1
run_both XREADGROUP GROUP grp c1 STREAMS g ">"
run_both XREADGROUP GROUP grp c2 STREAMS g 0-0
run_both XREADGROUP GROUP grp c1 STREAMS g 0-0
run_both XPENDING g grp - + 10

# NOACK：不建 PEL 但推进 last/read
run_both XADD g 100-4 f d
run_both XREADGROUP GROUP grp c1 NOACK STREAMS g ">"
run_both XPENDING g grp - + 10
run_both XINFO CONSUMERS g c1
run_both XINFO GROUPS g

# XACK 真实现
run_both XACK g grp 100-1 100-9
run_both XPENDING g grp
run_both XACK g grp bad
run_both XACK missing grp 1-1
run_both XACK g nogroup 1-1
run_both XACK g grp

# XPENDING 全形
run_both XPENDING g grp - + 1
run_both XPENDING g grp 100-2 + 10
run_both XPENDING g grp "(100-1" + 10
run_both XPENDING g grp - + 10 COUNT 2
run_both XPENDING g grp - + 10 IDLE 100000
run_both XPENDING g grp - + 10 IDLE x
run_both XPENDING g grp - + 0
run_both XPENDING g grp - + 10 c1
run_both XPENDING g grp 100-1 100-3 10 c9
run_both XPENDING missing grp
run_both XPENDING g nogroup
run_both XPENDING g grp - +
run_both XPENDING g grp bad-id + 10

# XCLAIM
run_both XCLAIM g grp c2 0 100-2
run_both XPENDING g grp - + 10
run_both XCLAIM g grp c2 0 100-2 JUSTID
run_both XCLAIM g grp c9 0 100-3 FORCE
run_both XCLAIM g grp c2 0 999-9 FORCE
run_both XCLAIM g grp c2 5000 100-3
run_both XCLAIM g grp c2 0 100-3 IDLE 5 RETRYCOUNT 7
run_both XCLAIM g grp c2 0 100-3 TIME 1700000000000
run_both XCLAIM g grp c2 0 100-1
run_both XCLAIM g grp c2 0 bad-id
run_both XCLAIM g grp c2 x 100-2
run_both XCLAIM g grp c2 0 100-2 BOGUS
run_both XCLAIM g grp c2

# XAUTOCLAIM
run_both XADD a 100-1 f a
run_both XADD a 100-2 f b
run_both XADD a 100-3 f c
run_both XGROUP CREATE a ag 0
run_both XREADGROUP GROUP ag c1 STREAMS a ">"
run_both XAUTOCLAIM a ag c2 0 0-0
run_both XAUTOCLAIM a ag c2 0 0-0 JUSTID
run_both XAUTOCLAIM a ag c2 0 0-0 COUNT 1
run_both XAUTOCLAIM a ag c9 0 0-0 FORCE
run_both XAUTOCLAIM a ag c2 x 0-0
run_both XAUTOCLAIM a ag c2 0 bad
run_both XAUTOCLAIM a ag c2 0 0-0 COUNT 0
run_both XAUTOCLAIM a ag c2
run_both XAUTOCLAIM missing ag c 0 0-0
run_both XAUTOCLAIM a nogroup c 0 0-0

# orphaned：XDEL 不清 PEL，autoclaim 归入 orphaned 并移除
run_both XDEL a 100-2
run_both XAUTOCLAIM a ag c2 0 0-0
run_both XAUTOCLAIM a ag c2 0 0-0 JUSTID
run_both XPENDING a ag - + 10

# DELCONSUMER 返该消费者 pending 丢弃数
run_both XGROUP DELCONSUMER a ag c2
run_both XPENDING a ag
run_both XGROUP DELCONSUMER a ag c2

# FULL 视图（含 PEL）
run_both XINFO STREAM g FULL
run_both XINFO STREAM g FULL COUNT 1
run_both XINFO STREAM a FULL

# XREADGROUP 错误形
run_both XREADGROUP GROUP grp c1 STREAMS g
run_both XREADGROUP GROUP grp STREAMS g ">"
run_both XREADGROUP GROUP grp c1 STREAMS missing ">"
run_both XREADGROUP GROUP grp c1 STREAMS g '$'
run_both XREADGROUP GROUP grp c1 COUNT x STREAMS g ">"
run_both XREADGROUP GROUP grp c1 BLOCK x STREAMS g ">"
run_both XREADGROUP GROUP grp c1 FOO STREAMS g ">"

# BLOCK + GROUP
run_both XREADGROUP GROUP grp c1 BLOCK 200 STREAMS g ">"
run_both XADD g 100-5 f e
run_both XREADGROUP GROUP grp c1 BLOCK 200 STREAMS g ">"

# 组写路径保留 TTL
run_both EXPIRE g 100
run_both XREADGROUP GROUP grp c1 STREAMS g 0-0
run_both TTL g

# WRONGTYPE
run_both SET w2 v
run_both XREADGROUP GROUP grp c STREAMS w2 ">"
run_both XPENDING w2 grp
run_both XCLAIM w2 grp c 0 1-1
run_both XAUTOCLAIM w2 grp c 0 0-0
run_both XINFO STREAM w2

# 归一化（双方输出同样处理）：自动 ID 整行→AUTOID；13 位以上裸数字
# （FULL 绝对投递时刻等时钟值）→N；range 形 XPENDING 输出按每组 4 行
# （id/owner/idle/count）第 3 行→N；idle/seen/active/inactive/radix
# 字段名下一行→N。
normalize() {
  sed -E -e 's/^[0-9]{13,}-[0-9]+$/AUTOID/' -e 's/^[0-9]{13,}$/N/' "$1" | awk '
    /^### / { hdr=$0; n=0; range=(hdr ~ /^### XPENDING [^ ]+ [^ ]+ /); print; next }
    range { n++; if (n % 4 == 3) { print "N"; next } }
    /^(idle|seen-time|active-time|inactive|radix-tree-keys|radix-tree-nodes)$/ { print; skip=1; next }
    skip==1 { print "N"; skip=0; next }
    { print }
  '
}
normalize "$GEDIS_OUT" > "$TMP/gedis.norm"
normalize "$REDIS_OUT" > "$TMP/redis.norm"

if diff -u "$TMP/redis.norm" "$TMP/gedis.norm"; then
  echo "STREAM-GROUP COMPAT OK"
else
  echo "STREAM-GROUP COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
