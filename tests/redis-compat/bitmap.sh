#!/usr/bin/env bash
# Bitmap 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 已知且被接受的差异：无（BITOP 取最长输入、SETBIT 自动扩展等均与真行为对齐）。
# HSCAN NOVALUES 之外的 BIT 家族在此全覆盖；READONLY 为 Redis 7.2 不支持的语法，双方同报 syntax error。
set -euo pipefail

GEDIS_PORT=6395
REDIS_PORT=6396
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

# SETBIT / GETBIT 与 string 视图一致性
run_both SETBIT b 7 1
run_both GET b
run_both SETBIT b 0 1
run_both GET b
run_both SETBIT b 0 0
run_both GETBIT b 7
run_both GETBIT b 100
run_both GETBIT nosuch 5
run_both SETBIT b -1 1
run_both SETBIT b 4294967296 1
run_both SETBIT b 1 2
run_both SET s AB
run_both GETBIT s 0
run_both GETBIT s 1
run_both GETBIT s 9
run_both SETBIT s 9 0
run_both GET s
run_both STRLEN s
run_both SET kttl v EX 100
run_both SETBIT kttl 0 1
run_both TTL kttl

# BITCOUNT / BITPOS（f0 0f：bits 0-3、12-15 置位）
run_both SET bc $'\xf0\x0f'
run_both BITCOUNT bc
run_both BITCOUNT bc 0 0
run_both BITCOUNT bc 0 -1
run_both BITCOUNT bc 1 1 BYTE
run_both BITCOUNT bc 0 7 BIT
run_both BITCOUNT bc 4 7 BIT
run_both BITCOUNT bc -8 -1 BIT
run_both BITCOUNT bc 5 30 BIT
run_both BITCOUNT bc 0 -1 BIT
run_both BITCOUNT bc 8 15 BIT
run_both BITCOUNT nosuch
run_both BITCOUNT bc x y
run_both BITPOS bc 0
run_both BITPOS bc 1
run_both BITPOS bc 1 1
run_both BITPOS bc 0 0 -1
run_both BITPOS bc 0 2 3
run_both BITPOS bc 1 4 7 BIT
run_both BITPOS bc 0 0 7 BIT
run_both BITPOS bc 0 8 15 BIT
run_both BITPOS bc 1 8 15 BIT
run_both BITPOS nosuch 0
run_both BITPOS nosuch 1
run_both BITPOS nosuch 0 5
run_both BITPOS bc 0 5
run_both SET ff $'\xff'
run_both BITPOS ff 0
run_both BITPOS ff 0 0
run_both BITPOS ff 0 1
run_both BITPOS ff 0 0 0
run_both BITPOS ff 0 5
run_both BITPOS ff 0 0 5
run_both BITPOS ff 1 8 15 BIT
run_both BITPOS bc 2
run_both BITPOS bc 0 x

# BITOP
run_both SET k1 $'\xff\xff'
run_both SET k2 $'\x0f'
run_both BITOP AND da k1 k2
run_both STRLEN da
run_both GET da
run_both BITOP OR do k1 k2
run_both GET do
run_both BITOP XOR dx k1 k2
run_both GET dx
run_both BITOP NOT dn k2
run_both GET dn
run_both BITOP NOT dn2 k1 k2
run_both BITOP AND dm k1 nosuch
run_both GET dm
run_both BITOP OR dmo k1 nosuch
run_both GET dmo
run_both BITOP OR de nosuch1 nosuch2
run_both EXISTS de
run_both SET dd old
run_both BITOP AND dd nosuch1 nosuch2
run_both EXISTS dd
run_both BITOP DIFF d k1
run_both BITOP AND d

# BITFIELD
run_both BITFIELD bf SET u8 "#0" 200 GET u8 0 INCRBY u8 0 56 GET u8 0
run_both BITFIELD bf OVERFLOW SAT INCRBY u8 "#0" 100
run_both BITFIELD bf SET u8 "#0" 200
run_both BITFIELD bf OVERFLOW SAT INCRBY u8 "#0" 100
run_both BITFIELD bf OVERFLOW FAIL INCRBY u8 "#0" 100
run_both BITFIELD bf GET i8 "#0"
run_both BITFIELD bh SET u4 "#1" 15 GET u4 "#1"
run_both GET bh
run_both BITFIELD sat SET i64 "#0" 9223372036854775807 OVERFLOW SAT INCRBY i64 "#0" 1 GET i64 0
run_both BITFIELD m1 GET u1 0 GET u1 1 SET u1 0 1 GET u1 0
run_both BITFIELD m1
run_both BITFIELD m1 OVERFLOW SAT
run_both BITFIELD x SET u64 "#0" 1
run_both BITFIELD x GET u8 -1
run_both BITFIELD x FOO u8 0
run_both BITFIELD x GET u8 0 EXTRA
run_both BITFIELD r1 READONLY GET u8 0

# WRONGTYPE
run_both HSET h f v
run_both SETBIT h 0 1
run_both GETBIT h 0
run_both BITCOUNT h
run_both BITPOS h 1
run_both BITOP AND d h k1
run_both BITFIELD h GET u8 0

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "BITMAP COMPAT OK"
else
  echo "BITMAP COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
