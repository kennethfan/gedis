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
  # UNKILLABLE 场景会在真机上留一个永不结束的脏脚本（BUSY 态，trap 的 kill 收不掉），
  # 先 SHUTDOWN NOSAVE（BUSY 态仍允许）放倒它，避免污染端口的下一次运行。
  redis-cli -p "$REDIS_PORT" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
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

# cjson：encode/decode 映射与错误文案（同体 EVAL → 同 sha，错误确定性可比；
# 对象只用单键，避开双方键序差异）
run_both EVAL "return cjson.encode('hello')" 0
run_both EVAL "return cjson.encode({1,2,3})" 0
run_both EVAL "return cjson.encode({})" 0
run_both EVAL "return cjson.encode(1/3)" 0
run_both EVAL "return cjson.encode({name='bob'})" 0
run_both EVAL "return cjson.encode(0/0)" 0
run_both EVAL "return cjson.encode({[true]='x'})" 0
run_both EVAL "return cjson.encode()" 0
run_both EVAL "return cjson.decode('{\"a\":1}').a" 0
run_both EVAL "return cjson.decode('[1,2]')[2]" 0
run_both EVAL "return cjson.decode('null') == cjson.null" 0
run_both EVAL "return cjson.null" 0
run_both EVAL "return cjson.decode('{bad')" 0
run_both EVAL "return cjson.decode('[1,]')" 0
run_both EVAL "local ok,err=pcall(cjson.decode,'{bad'); return err" 0
run_both EVAL "return cjson.decode(42)" 0
run_both EVAL "return cjson.decode()" 0
run_both EVAL "return cjson.decode('\"\\u0041\"')" 0
run_both EVAL "return cjson.encode('😀')" 0
run_both EVAL "return cjson.encode(cjson.decode('{\"a\":1}'))" 0
run_both EVAL "local c=cjson.new(); return c.encode({1})" 0
run_both EVAL "return cjson.decode('[+2]')[1]" 0
run_both EVAL "return cjson.decode('[1 2]')" 0
run_both EVAL "return cjson.decode('{\"a\" 1}')" 0
run_both EVAL "return cjson.decode('[1}]')" 0

# SCRIPT KILL：NOTBUSY 与 arity 为确定性对照
run_both SCRIPT KILL
run_both SCRIPT KILL extra

# kill_clean: $1=port $2=side-out : 后台 EVAL 死循环，重试 KILL 直到 OK（收敛掉
# EVAL 注册前的 NOTBUSY 空窗），只记录终态 KILL 回复 + EVAL 侧输出
kill_clean() {
  local port=$1 out=$2
  echo "### KILL-CLEAN" >> "$out"
  redis-cli -p "$port" EVAL "while true do end return 1" 0 > "$TMP/eval_$port.out" 2>&1 &
  local pid=$!
  local reply=""
  for _ in $(seq 1 100); do
    reply=$(redis-cli -p "$port" SCRIPT KILL 2>&1)
    if [ "$reply" = "OK" ]; then break; fi
    sleep 0.1
  done
  wait "$pid" || true
  cat "$TMP/eval_$port.out" >> "$out"
  echo "$reply" >> "$out"
}

# kill_dirty: $1=port $2=side-out : 写后循环脚本，KILL 收敛到 UNKILLABLE（若抢跑
# 误杀则本轮作废重发，最多 5 轮）；EVAL 侧输出为空（脚本仍在跑），只记 KILL 回复
kill_dirty() {
  local port=$1 out=$2
  echo "### KILL-DIRTY" >> "$out"
  local reply=""
  for _ in $(seq 1 5); do
    redis-cli -p "$port" EVAL "redis.call('set','uk','1') while true do end return 1" 0 > "$TMP/evald_$port.out" 2>&1 &
    local pid=$!
    sleep 1
    reply=$(redis-cli -p "$port" SCRIPT KILL 2>&1)
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    if [ "$reply" != "OK" ]; then break; fi
  done
  cat "$TMP/evald_$port.out" >> "$out"
  echo "$reply" >> "$out"
}

kill_clean "$GEDIS_PORT" "$GEDIS_OUT"
kill_clean "$REDIS_PORT" "$REDIS_OUT"
# UNKILLABLE 压尾：真机侧脚本永不结束（trap 负责收尸），之后不再发任何命令
kill_dirty "$GEDIS_PORT" "$GEDIS_OUT"
kill_dirty "$REDIS_PORT" "$REDIS_OUT"

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "LUA COMPAT OK"
else
  echo "LUA COMPAT MISMATCH (redis vs gedis above)"
  exit 1
fi
