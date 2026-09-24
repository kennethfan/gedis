#!/usr/bin/env bash
# Geo + SCAN/通用命令与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 已知且被接受的差异：
# - STOREDIST 存入的全精度距离：Go 与 glibc 的 libm 末位 ulp 差 1（单测用 InDelta 覆盖，此处容差比对）。
# - 无 ASC/DESC 的搜索默认顺序：真 Redis 多次运行间本身不确定；此处一律显式指定排序。
# - SCAN 全集顺序：两侧哈希实现不同；此处按键名排序后比对，分页语义由 Go 单测覆盖。
# - HSCAN NOVALUES 需 Redis 7.4+，此处 Redis 为 7.2，仅 Go 单测覆盖。
set -euo pipefail

GEDIS_PORT=6393
REDIS_PORT=6394
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

# SCAN 类：两侧各自循环扫到 cursor 归零后按集合比对（Redis cursor 非确定，
# 分页走完语义由 Go 单测 Test_Scan_when_GlobalMatchType 覆盖）。
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

# 全精度浮点：容差 1e-9 相对误差比对，日志仍落盘走精确 diff（值已在容差内则末位差会触发 diff，故此处只记命令头）。
run_both_approx() {
  echo "### $* (approx)" >> "$GEDIS_OUT"
  echo "### $* (approx)" >> "$REDIS_OUT"
  g="$(redis-cli -p "$GEDIS_PORT" "$@" 2>&1 || true)"
  r="$(redis-cli -p "$REDIS_PORT" "$@" 2>&1 || true)"
  python3 -c "import sys; g=float(sys.argv[1]); r=float(sys.argv[2]); sys.exit(0 if abs(g-r) <= 1e-9*max(1,abs(r)) else 1)" "$g" "$r" \
    || { echo "APPROX MISMATCH: $* gedis=$g redis=$r"; exit 1; }
  echo "approx-ok" >> "$GEDIS_OUT"
  echo "approx-ok" >> "$REDIS_OUT"
}

# Geo 写入与基础读
run_both GEOADD sicily 13.361389 38.115556 Palermo 15.087269 37.502669 Catania 13.583333 37.316667 Agrigento
run_both GEOADD sicily NX 13.361389 38.115556 Palermo 10 10 Roma
run_both GEODIST sicily Palermo Catania
run_both GEODIST sicily Palermo Catania km
run_both GEODIST sicily Palermo Catania mi
run_both GEODIST sicily Palermo Catania ft
run_both GEODIST sicily Palermo Nope
run_both GEODIST missing Palermo Catania
run_both GEOPOS sicily Palermo Catania Nope
run_both ZSCORE sicily Palermo
run_both ZCARD sicily
run_both GEOADD bad 200 30 X

# Geo 搜索
run_both GEOSEARCH sicily FROMMEMBER Palermo BYRADIUS 200 km ASC
run_both GEOSEARCH sicily FROMMEMBER Palermo BYRADIUS 200 km DESC WITHDIST
run_both GEOSEARCH sicily FROMLONLAT 15 37 BYRADIUS 100 km ASC
run_both GEOSEARCH sicily FROMLONLAT 15 37 BYBOX 400 400 km ASC WITHCOORD WITHDIST WITHHASH
run_both GEOSEARCH sicily FROMMEMBER Palermo BYRADIUS 200 km ASC COUNT 2
run_both GEOSEARCH sicily FROMMEMBER Nope BYRADIUS 200 km ASC
run_both GEORADIUS sicily 15 37 200 km ASC
run_both GEORADIUS sicily 15 37 200 km ASC WITHDIST WITHCOORD WITHHASH
run_both GEORADIUS sicily 15 37 200 km ASC COUNT 2
run_both GEORADIUS_RO sicily 15 37 200 km ASC
run_both GEORADIUSBYMEMBER sicily Palermo 200 km ASC
run_both GEORADIUSBYMEMBER_RO sicily Palermo 200 km ASC WITHDIST
run_both GEOSEARCHSTORE out sicily FROMMEMBER Palermo BYRADIUS 200 km ASC
run_both ZRANGE out 0 -1 WITHSCORES
run_both GEOSEARCHSTORE outd sicily FROMMEMBER Palermo BYRADIUS 200 km ASC STOREDIST
run_both ZRANGE outd 0 -1
run_both GEORADIUS sicily 15 37 200 km STORE rout ASC
run_both ZRANGE rout 0 -1

# STOREDIST 全精度：容差比对
run_both GEORADIUS sicily 15 37 200 km STOREDIST routd
run_both_approx ZSCORE routd Catania
run_both_approx ZSCORE routd Agrigento
run_both_approx ZSCORE routd Palermo

# SCAN 家族
run_both SET str1 v
run_both HSET h1 f v
run_both RPUSH l1 e
run_both SADD s1 m
run_both ZADD z1 1 m
run_both_scan
run_both_scan MATCH "s*"
run_both_scan TYPE zset
run_both_scan TYPE hash
run_both_scan TYPE string COUNT 100
run_both ZSCAN z1 0
run_both ZSCAN z1 0 MATCH "m*"
run_both SSCAN s1 0 MATCH "*"
run_both HSCAN h1 0 MATCH "*"

# 通用 key 命令
run_both SET ks v EX 100
run_both COPY ks kd
run_both GET kd
run_both TTL kd
run_both COPY ks kd
run_both COPY ks kd REPLACE
run_both COPY nosuch x
run_both COPY ks ks
run_both HSET kh f v
run_both COPY kh kh2
run_both HGET kh2 f
run_both RENAME kd kd2
run_both EXISTS kd kd2
run_both RENAMENX kd2 kd3
run_both SET busy x
run_both RENAMENX kd3 busy
run_both RENAME nosuch x
run_both TYPE kd3

# SORT
run_both RPUSH mylist 3 1 2
run_both SADD myset b a c
run_both ZADD sc2 2 10 1 20
run_both SORT mylist
run_both SORT mylist DESC
run_both SORT myset ALPHA DESC
run_both SORT sc2
run_both SORT myset
run_both SET w_a 5
run_both SET w_b 3
run_both SET w_c 9
run_both RPUSH items a b c
run_both SORT items BY "w_*"
run_both SORT items BY "w_*" LIMIT 1 1
run_both SET o_a A1
run_both SORT items BY "w_*" GET "o_*" GET "w_*"
run_both HSET uh_a name Alice
run_both HSET uh_b name Bob
run_both SORT items BY "w_*" GET "uh_*->name"
run_both SORT items ALPHA GET "#"
run_both SORT items BY "w_*" STORE sout
run_both LRANGE sout 0 -1
run_both SORT nosuch
run_both SORT nosuch STORE so0
run_both SORT ks

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "GEO-SCAN COMPAT OK"
else
  echo "GEO-SCAN COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
