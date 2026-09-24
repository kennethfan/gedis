#!/usr/bin/env bash
# EVAL/EVALSHA/SCRIPT 与真 Redis 的输出对照：同一命令序列分别发往 gedis 与 redis-server，逐行 diff。
# 已知差异：编译错误措辞（gopher-lua 解析器 vs Lua 5.1，见 lua.go compileDetail），该用例经
# normalize 抹掉 user_script 之后的部分再比；死循环超时用例跳过（两侧各 5s+ 且真机进 BUSY 态）。
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

# lua_seq: $1=label, $2=\n 分隔的命令序列，经单连接依次执行（MULTI 会话用）。
lua_seq() {
  echo "### LUA $1" >> "$GEDIS_OUT"
  echo "### LUA $1" >> "$REDIS_OUT"
  printf '%b' "$2" | redis-cli -p "$GEDIS_PORT" >> "$GEDIS_OUT" 2>&1 || true
  printf '%b' "$2" | redis-cli -p "$REDIS_PORT" >> "$REDIS_OUT" 2>&1 || true
}

# 返回值映射：int/string/嵌套拍平/true→1/false→nil/小数截断
run_both EVAL "return 1" 0
run_both EVAL "return 'hello'" 0
run_both EVAL "return {1,{2,3}}" 0
run_both EVAL "return true" 0
run_both EVAL "return false" 0
run_both EVAL "return nil" 0
run_both EVAL "return 3.5" 0
run_both EVAL "return {err='my error'}" 0
run_both EVAL "return {ok='fine'}" 0

# KEYS/ARGV
run_both EVAL "return {KEYS[1],KEYS[2],ARGV[1]}" 2 a b c
run_both EVAL "return #KEYS" 0

# redis.call 读写往返 + 缺失 key 回 false
run_both EVAL "return redis.call('SET','k','v')" 0
run_both EVAL "return redis.call('GET','k')" 0
run_both EVAL "return redis.call('GET','missing') == false" 0
run_both EVAL "redis.call('SET','w','1'); return redis.call('INCR','w')" 0
run_both EVAL "redis.call('HSET','h','f','v'); return redis.call('HGETALL','h')" 0 x
run_both EVAL "redis.call('RPUSH','lst','a','b'); return redis.call('LRANGE','lst',0,-1)" 0 x

# call 抛错带 script 后缀；pcall 装表（r.err→bulk，r→error）
run_both EVAL "return redis.call('INCR','k')" 0
run_both EVAL "local r=redis.pcall('INCR','k'); return r.err" 0
run_both EVAL "local r=redis.pcall('INCR','k'); return r" 0
run_both EVAL "return redis.call('NOSUCHCMD')" 0
run_both EVAL "return redis.call('GET','k','extra')" 0

# WRONGTYPE 不补 ERR 前缀；error() 补 ERR 前缀
run_both EVAL "return redis.call('LPUSH','k','v')" 0
run_both EVAL "error('boom')" 0

# numkeys 非法三件套 + argc 不足 + 未知子命令
run_both EVAL "return 1" foo
run_both EVAL "return 1" -1
run_both EVAL "return 1" 5 a
run_both EVAL "return 1"
run_both SCRIPT NOSUCH
run_both SCRIPT

# SCRIPT 缓存流：LOAD→EVALSHA→EXISTS→FLUSH→NOSCRIPT；EVAL 自动缓存
SHA1=$(redis-cli -p "$REDIS_PORT" SCRIPT LOAD "return 41+1")
run_both SCRIPT LOAD "return 41+1"
run_both EVALSHA "$SHA1" 0
run_both SCRIPT EXISTS "$SHA1" deadbeefdeadbeefdeadbeefdeadbeefdeadbeef
run_both SCRIPT FLUSH
run_both EVALSHA "$SHA1" 0
run_both EVAL "return 2-1" 0
SHA2=$(redis-cli -p "$REDIS_PORT" SCRIPT LOAD "return 2-1")
run_both EVALSHA "$SHA2" 0
run_both EVALSHA deadbeefdeadbeefdeadbeefdeadbeefdeadbeef 0

# 杂项：sha1hex/log/status_reply/error_reply
run_both EVAL "return redis.sha1hex('abc')" 0
run_both EVAL "return redis.log(redis.LOG_WARNING,'hi')" 0
run_both EVAL "return redis.status_reply('fine')" 0
run_both EVAL "return redis.error_reply('bad')" 0

# MULTI 内 EVAL 整体排队，EXEC 回放执行脚本内写
lua_seq "eval-in-multi" "MULTI\nEVAL \"return redis.call('SET','mk','1')\" 0\nEXEC\n"
run_both GET mk

# 编译错误：两侧措辞不同，normalize 后只比前缀
echo "### COMPILE" >> "$GEDIS_OUT"
echo "### COMPILE" >> "$REDIS_OUT"
redis-cli -p "$GEDIS_PORT" EVAL "return {{{" 0 2>&1 | sed 's/user_script.*/user_script NORM/' >> "$GEDIS_OUT" || true
redis-cli -p "$REDIS_PORT" EVAL "return {{{" 0 2>&1 | sed 's/user_script.*/user_script NORM/' >> "$REDIS_OUT" || true

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "LUA COMPAT OK"
else
  echo "LUA COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
