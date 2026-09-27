#!/usr/bin/env bash
# Sentinel 双 listener 冒烟：哨兵口 PING/SENTINEL 通、数据口读写正常；
# enabled=false 时哨兵口应拒绝连接。
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
TMP=$(mktemp -d)
DATA_PORT=7399
SENT_PORT=27639

cleanup() { kill "$SRV" 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT

go -C "$ROOT" build -o "$TMP/gedis" ./cmd/gedis

run_gedis() { # $1 = enabled(true/false)
  cat > "$TMP/gedis.toml" <<EOF
[server]
host = "127.0.0.1"
port = $DATA_PORT
[storage]
datadir = "$TMP/data"
[memory]
maxmemory = 1073741824
policy = "allkeys-lru"
[persistence]
appendonly = false
fsync = "everysec"
[sentinel]
enabled = $1
port = $SENT_PORT
down_after_ms = 0
[[sentinel.masters]]
name = "mymaster"
master_addr = "127.0.0.1:6390"
quorum = 1
slaves = ["127.0.0.1:6391"]
EOF
  "$TMP/gedis" -config "$TMP/gedis.toml" >"$TMP/gedis.log" 2>&1 &
  SRV=$!
  for _ in $(seq 1 50); do
    redis-cli -p $DATA_PORT PING >/dev/null 2>&1 && break
    sleep 0.1
  done
}

# Phase 1: enabled=true
run_gedis true
test "$(redis-cli -p $SENT_PORT PING)" = "PONG"
redis-cli -p $SENT_PORT SENTINEL get-master-addr-by-name mymaster | grep -q 6390
redis-cli -p $SENT_PORT SENTINEL masters | grep -q mymaster
test "$(redis-cli -p $DATA_PORT SET k v)" = "OK"
test "$(redis-cli -p $DATA_PORT GET k)" = "v"
kill "$SRV"; wait "$SRV" 2>/dev/null || true

# Phase 2: enabled=false → 哨兵口拒绝
run_gedis false
test "$(redis-cli -p $DATA_PORT PING)" = "PONG"
if redis-cli -p $SENT_PORT PING >/dev/null 2>&1; then
  echo "FAIL: sentinel port should refuse when disabled"; exit 1
fi

echo "SMOKE OK"
