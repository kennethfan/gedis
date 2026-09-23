#!/usr/bin/env bash
# HyperLogLog 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 已知且被接受的差异：dense-only 实现在大基数舍入边界可能差 1（单测用仲裁值 + InDelta 覆盖，
# 此处向量均为远离边界的稳定值）；PFCOUNT 为近似算法，同输入同实现的确定性 dense 路径输出一致。
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
pfadd_bulk() {
  local key=$1 prefix=$2 count=$3
  local -a batch=()
  for ((i = 0; i < count; i++)); do
    batch+=("${prefix}${i}")
    if ((${#batch[@]} == 1000)); then
      run_both PFADD "$key" "${batch[@]}" >/dev/null
      batch=()
    fi
  done
  if ((${#batch[@]} > 0)); then
    run_both PFADD "$key" "${batch[@]}" >/dev/null
  fi
}

# 基础语义
run_both PFCOUNT missing
run_both PFADD h a b c
run_both PFADD h a
run_both PFADD h d
run_both PFCOUNT h
run_both PFADD bare
run_both PFCOUNT bare
run_both PFADD
run_both PFCOUNT
run_both PFMERGE
run_both PFMERGE onlydest
run_both PFCOUNT onlydest

# 类型语义：HLL key 对外就是 string
run_both TYPE h
run_both_scan TYPE string MATCH "h*"
run_both SET s v
run_both PFADD s x
run_both PFMERGE deststr s
run_both PFMERGE deststr missing h

# TTL 沿用
run_both PFADD th a
run_both EXPIRE th 100
run_both PFADD th b
run_both TTL th
run_both PFCOUNT th

# 仲裁向量：1000×e → 1008；10k×w → 10073
pfadd_bulk ekey e 1000
run_both PFCOUNT ekey
pfadd_bulk wkey w 10000
run_both PFCOUNT wkey

# 合并仲裁：mega(100k×k) + 10k×w → 110602；multi 5；基础 5；进已存在 4
pfadd_bulk mega k 100000
run_both PFCOUNT mega
run_both PFMERGE big mega wkey
run_both PFCOUNT big
run_both PFMERGE multi ekey wkey
run_both PFCOUNT multi
run_both PFADD m1 a b c
run_both PFADD m2 c d e
run_both PFMERGE mout m1 m2
run_both PFCOUNT mout
run_both PFADD have x y
run_both PFMERGE have m1 m2
run_both PFCOUNT have

# 100k×k → 100216（mega 已建，直接读）
run_both PFCOUNT mega

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "HLL COMPAT OK"
else
  echo "HLL COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
