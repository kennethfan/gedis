#!/usr/bin/env bash
# Stream 基础（XADD/XLEN/XRANGE/XREVRANGE/XREAD/XTRIM/XDEL/XINFO/XGROUP/XACK）
# 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 归一化（双方输出同样处理）：13 位以上 ms 的自动 ID 整行屏蔽；idle/seen-time/active-time
# 取值随时间变化只比结构；radix-tree-keys/nodes 为内部实现细节只比存在性。
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

# 两侧各自循环扫到 cursor 归零后按集合比对（Redis cursor 非确定）。
scan_all() {
  local port=$1
  shift
  local cursor=0
  local out=""
  local res
  while :; do
    res="$(redis-cli -p "$port" SCAN "$cursor" "$@" 2>&1 || true)"
    cursor="$(echo "$res" | head -1)"
    out="$out
$(echo "$res" | tail -n +2)"
    if [ "$cursor" = "0" ]; then
      break
    fi
  done
  echo "$out" | grep -v '^$' | sort
}
run_both_scan() {
  echo "### SCAN-LOOP $*" >> "$GEDIS_OUT"
  echo "### SCAN-LOOP $*" >> "$REDIS_OUT"
  scan_all "$GEDIS_PORT" "$@" >> "$GEDIS_OUT"
  scan_all "$REDIS_PORT" "$@" >> "$REDIS_OUT"
}

# XADD 基础 + 错误
run_both XADD s 100-1 name Alice age 30
run_both XADD s 100-2 name Bob
run_both XADD s bad a 1
run_both XADD s2 0-0 a 1
run_both XADD s 100-2 a 2
run_both XADD s 99-9 a 2
run_both XADD s "*" auto field
run_both XADD s 5000-\* f v
run_both XADD s 5000-\* f v2
run_both XADD s 6000-\* f v3
run_both XADD s "*" onlyfield
run_both XADD s
run_both XLEN s
run_both XLEN missing

# XRANGE / XREVRANGE
run_both XRANGE s - +
run_both XRANGE s 100-1 100-2
run_both XRANGE s - + COUNT 2
run_both XRANGE s - + COUNT 0
run_both XRANGE s + -
run_both XRANGE s 6000-0 5000-0
run_both XRANGE missing - +
run_both XRANGE s
run_both XRANGE s - + COUNT x
run_both XREVRANGE s + -
run_both XREVRANGE s + - COUNT 1
run_both XREVRANGE missing + -

# XDEL
run_both XDEL s 100-1
run_both XDEL s 100-9
run_both XDEL s bad-id
run_both XDEL missing 100-1
run_both XDEL s

# XTRIM
run_both XADD t 100-1 f v
run_both XADD t 100-2 f v
run_both XADD t 100-3 f v
run_both XADD t 100-4 f v
run_both XTRIM t MAXLEN 2
run_both XLEN t
run_both XRANGE t - +
run_both XTRIM t MAXLEN = 1
run_both XTRIM t MAXLEN "~" 1
run_both XTRIM t MINID 100-4
run_both XTRIM t MINID "~" 999-0 LIMIT 10
run_both XTRIM t MAXLEN 1 LIMIT 2
run_both XTRIM t MAXLEN -5
run_both XTRIM t MAXLEN x
run_both XTRIM t BOGUS 1
run_both XTRIM t MAXLEN
run_both XTRIM missing MAXLEN 5
run_both XTRIM t
run_both XTRIM t MINID bad

# XREAD
run_both XADD k1 100-1 a 1
run_both XADD k1 100-2 a 2
run_both XADD k1 100-3 a 3
run_both XADD k2 200-1 b 1
run_both XADD k2 200-2 b 2
run_both XREAD STREAMS k1 k2 0-0 0-0
run_both XREAD COUNT 2 STREAMS k1 k2 0-0 0-0
run_both XREAD STREAMS k1 '$'
run_both XREAD BLOCK 300 STREAMS k1 '$'
run_both XREAD STREAMS k1 ">"
run_both XREAD STREAMS missing 0-0
run_both XREAD STREAMS k1
run_both XREAD STREAMS k1 k2 0-0
run_both XREAD COUNT x STREAMS k1 0-0
run_both XREAD BLOCK x STREAMS k1 0-0
run_both XREAD BLOCK -1 STREAMS k1 0-0
run_both XREAD FOO STREAMS k1 0-0

# XINFO / XGROUP / XACK
run_both XINFO STREAM s
run_both XINFO STREAM t
run_both XINFO STREAM missing
run_both XINFO GROUPS s
run_both XINFO CONSUMERS s g
run_both XGROUP CREATE s g1 0
run_both XGROUP CREATE s g1 0
run_both XGROUP CREATE missing g 0
run_both XGROUP CREATE missing2 g 0 MKSTREAM
run_both XINFO STREAM missing2
run_both XGROUP CREATE s g2 '$'
run_both XINFO GROUPS s
run_both XGROUP SETID s g1 5-5
run_both XINFO GROUPS s
run_both XGROUP SETID s g1 5-5 ENTRIESREAD 7
run_both XINFO GROUPS s
run_both XGROUP DESTROY s g1
run_both XGROUP DESTROY s g1
run_both XGROUP DESTROY missing g
run_both XGROUP CREATECONSUMER s g2 c1
run_both XGROUP CREATECONSUMER s g2 c1
run_both XINFO CONSUMERS s g2
run_both XINFO STREAM s FULL
run_both XINFO STREAM s FULL COUNT 1
run_both XGROUP DELCONSUMER s g2 c1
run_both XGROUP DELCONSUMER s g2 c1
run_both XGROUP CREATECONSUMER s nogroup c1
run_both XGROUP BOGUS s g
run_both XGROUP CREATE s
run_both XGROUP SETID s g2 bad-id
run_both XGROUP SETID missing g 0-0
run_both XACK s g2 100-2
run_both XACK missing g 1-1
run_both XACK s nogroup 1-1
run_both XACK s g2 bad
run_both XACK s g2

# max-deleted 只跟 XDEL
run_both XADD md 100-1 a 1
run_both XADD md 100-2 a 2
run_both XADD md 100-3 a 3
run_both XDEL md 100-2
run_both XTRIM md MINID 100-3
run_both XINFO STREAM md
run_both XTRIM md MAXLEN 0
run_both XINFO STREAM md
run_both XLEN md
run_both EXISTS md

# 类型 / SCAN / TTL / WRONGTYPE
run_both TYPE s
run_both_scan TYPE stream
run_both SET str v
run_both XADD str "*" a 1
run_both XLEN str
run_both XRANGE str - +
run_both XREVRANGE str + -
run_both XDEL str 1-1
run_both XTRIM str MAXLEN 1
run_both XREAD STREAMS str 0-0
run_both XINFO STREAM str
run_both XINFO GROUPS str
run_both XGROUP CREATE str g 0
run_both XACK str g 1-1
run_both XADD ttlk 100-1 a 1
run_both EXPIRE ttlk 100
run_both XADD ttlk 100-2 a 2
run_both TTL ttlk

normalize() {
  sed -E -e 's/^[0-9]{13,}-[0-9]+$/AUTOID/' \
    -e '/^(idle|seen-time|active-time|radix-tree-keys|radix-tree-nodes)$/{n;s/.*/N/;}' "$1"
}
normalize "$GEDIS_OUT" > "$TMP/gedis.norm"
normalize "$REDIS_OUT" > "$TMP/redis.norm"

if diff -u "$TMP/redis.norm" "$TMP/gedis.norm"; then
  echo "STREAM COMPAT OK"
else
  echo "STREAM COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
