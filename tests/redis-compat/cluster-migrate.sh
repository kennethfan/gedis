#!/usr/bin/env bash
# Cluster slot 迁移与 MIGRATE 同真 Redis 7.2.6 的输出对照。
# 拓扑：双 gedis（G1 持 0-8191，G2 持 8192-16383 作 RESTORE 目标）；
# 真机侧为 2 主（R1 持 0-8191，R2 持 8192-16383 作迁移目标）。
# 两侧均为“源→同类目标”，逐行 diff；目标端口、BUSYKEY 包裹层与 NOKEY
# 后缀做归一化。迁移收尾含 SETSLOT NODE（覆盖 NODE 分支），末尾 kill -9
# 源节点重启验证迁移态落盘恢复。
set -euo pipefail

G1=7711
G2=7721
R1=7712
R2=7722
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
GEDIS_OUT="$TMP/gedis.out"
REDIS_OUT="$TMP/redis.out"
G1DATA="$TMP/g1data"
G2DATA="$TMP/g2data"
: > "$GEDIS_OUT"
: > "$REDIS_OUT"

cleanup() {
  kill -9 "$G1_PID" "$G2_PID" 2>/dev/null || true
  redis-cli -p "$R1" shutdown nosave >/dev/null 2>&1 || true
  redis-cli -p "$R2" shutdown nosave >/dev/null 2>&1 || true
  rm -rf "$TMP"
}
trap cleanup EXIT

# R1 新鲜度护栏：按端口杀掉占住测试端口的陈旧进程（崩溃残留的 redis
# 或 gedis；gedis 无 SHUTDOWN，trap/TERM 杀不掉，必须按端口 kill -9）。
for p in "$G1" "$G2" "$R1" "$R2"; do
  redis-cli -p "$p" shutdown nosave >/dev/null 2>&1 || true
  for pid in $(lsof -ti tcp:"$p" 2>/dev/null); do kill -9 "$pid" 2>/dev/null || true; done
done
sleep 0.5

cd "$ROOT"
CGO_ENABLED=0 go build -trimpath -o "$TMP/gedis" ./cmd/gedis

mk_toml() { # $1=文件 $2=端口 $3=自id $4=自slots $5=对端id $6=对端端口 $7=对端slots $8=datadir
  cat > "$1" <<EOF
[server]
host = "127.0.0.1"
port = $2
[storage]
datadir = "$8"
[memory]
maxmemory = 1073741824
policy = "allkeys-lru"
[persistence]
appendonly = false
fsync = "everysec"
[metrics]
enabled = false
port = 0
[cluster]
enabled = true
[[cluster.nodes]]
id = "$3"
addr = "127.0.0.1:$2"
slots = ["$4"]
[[cluster.nodes]]
id = "$5"
addr = "127.0.0.1:$6"
slots = ["$7"]
EOF
}

mk_toml "$TMP/g1.toml" "$G1" g1src 0-8191 g2tgt "$G2" 8192-16383 "$G1DATA"
mk_toml "$TMP/g2.toml" "$G2" g2tgt 8192-16383 g1src "$G1" 0-8191 "$G2DATA"

"$TMP/gedis" -config "$TMP/g1.toml" >/dev/null 2>&1 &
G1_PID=$!
"$TMP/gedis" -config "$TMP/g2.toml" >/dev/null 2>&1 &
G2_PID=$!

mkdir -p "$TMP/r1" "$TMP/r2"
redis-server --port "$R1" --dir "$TMP/r1" --dbfilename dump.rdb \
  --cluster-enabled yes --cluster-config-file "$TMP/nodes-$R1.conf" \
  --cluster-node-timeout 5000 --save '' --appendonly no --daemonize yes >/dev/null 2>&1
redis-server --port "$R2" --dir "$TMP/r2" --dbfilename dump.rdb \
  --cluster-enabled yes --cluster-config-file "$TMP/nodes-$R2.conf" \
  --cluster-node-timeout 5000 --save '' --appendonly no --daemonize yes >/dev/null 2>&1

for i in $(seq 1 100); do
  if redis-cli -p "$G1" ping >/dev/null 2>&1 \
    && redis-cli -p "$G2" ping >/dev/null 2>&1 \
    && redis-cli -p "$R1" ping >/dev/null 2>&1 \
    && redis-cli -p "$R2" ping >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

# gedis 双节点新鲜度：静态拓扑 known_nodes 恒为 2。
for i in $(seq 1 100); do
  if [ "$(redis-cli -p "$G1" cluster info 2>/dev/null | grep -c cluster_known_nodes:2)" = "1" ] \
    && [ "$(redis-cli -p "$G2" cluster info 2>/dev/null | grep -c cluster_known_nodes:2)" = "1" ]; then
    break
  fi
  sleep 0.1
done

# 身份护栏：确认监听的是本次启动的实例（陈旧冒名者 Topo 不同）。
[ "$(redis-cli -p "$G1" cluster myid 2>/dev/null)" = "g1src" ] || { echo "G1 identity mismatch, abort"; exit 1; }
[ "$(redis-cli -p "$G2" cluster myid 2>/dev/null)" = "g2tgt" ] || { echo "G2 identity mismatch, abort"; exit 1; }

redis-cli -p "$R1" cluster meet 127.0.0.1 "$R2" >/dev/null
for i in $(seq 1 100); do
  if [ "$(redis-cli -p "$R1" cluster info 2>/dev/null | grep -c cluster_known_nodes:2)" = "1" ]; then
    break
  fi
  sleep 0.1
done
# shellcheck disable=SC2046
redis-cli -p "$R1" cluster addslots $(seq 0 8191) >/dev/null
# shellcheck disable=SC2046
redis-cli -p "$R2" cluster addslots $(seq 8192 16383) >/dev/null
for i in $(seq 1 100); do
  if [ "$(redis-cli -p "$R1" cluster info 2>/dev/null | grep -c cluster_state:ok)" = "1" ] \
    && [ "$(redis-cli -p "$R2" cluster info 2>/dev/null | grep -c cluster_state:ok)" = "1" ]; then
    break
  fi
  sleep 0.1
done

R1ID="$(redis-cli -p "$R1" cluster myid)"
R2ID="$(redis-cli -p "$R2" cluster myid)"
KEY1="migkey1"
SLOT1="$(redis-cli -p "$G1" cluster keyslot "$KEY1")"
[ "$SLOT1" -lt 8192 ] || { echo "KEY1 slot $SLOT1 not on source, abort"; exit 1; }
[ "$(redis-cli -p "$R1" cluster keyslot "$KEY1")" = "$SLOT1" ] || {
  echo "SLOT MISMATCH between gedis and redis, abort"
  exit 1
}
# 第二个 key 必须落在源端槽位（<8192）且与 KEY1 不同槽。
KEY2=""
SLOT2=""
for cand in mgc1 mgc2 mgc3 mgc4 mgc5 mgc6 mgc7 mgc8; do
  s="$(redis-cli -p "$G1" cluster keyslot "$cand")"
  if [ "$s" -lt 8192 ] && [ "$s" != "$SLOT1" ]; then KEY2="$cand"; SLOT2="$s"; break; fi
done
[ -n "$KEY2" ] || { echo "no suitable KEY2, abort"; exit 1; }
[ "$(redis-cli -p "$R1" cluster keyslot "$KEY2")" = "$SLOT2" ] || {
  echo "SLOT2 MISMATCH between gedis and redis, abort"
  exit 1
}
# 第三个 key 必须落在源端稳定槽位（<8192 且非 SLOT1/SLOT2），专测缺 key 的 NOKEY。
KEY0=""
for cand in nok0 nok1 nok2 nok3 nok4 nok5; do
  s="$(redis-cli -p "$G1" cluster keyslot "$cand")"
  if [ "$s" -lt 8192 ] && [ "$s" != "$SLOT1" ] && [ "$s" != "$SLOT2" ]; then KEY0="$cand"; break; fi
done
[ -n "$KEY0" ] || { echo "no suitable KEY0, abort"; exit 1; }
[ "$(redis-cli -p "$R1" cluster keyslot "$KEY0")" != "$SLOT1" ] || { echo "KEY0 collides, abort"; exit 1; }

# norm：源/目标端口差异与 BUSYKEY 包裹层差异归一化；NOKEY 两側已同为裸 +NOKEY。
norm() {
  sed -e "s/127\.0\.0\.1:$G2/127.0.0.1:@TGT@/" \
      -e "s/127\.0\.0\.1:$R2/127.0.0.1:@TGT@/" \
      -e "s/127\.0\.0\.1:$G1/127.0.0.1:@SRC@/" \
      -e "s/127\.0\.0\.1:$R1/127.0.0.1:@SRC@/" \
      -e "s/ERR Target instance replied with error: //"
}
g() { # gedis 源节点 G1
  echo "### $*" >> "$GEDIS_OUT"
  redis-cli -p "$G1" "$@" 2>&1 | norm >> "$GEDIS_OUT" || true
}
r() { # 真机源节点 R1
  echo "### $*" >> "$REDIS_OUT"
  redis-cli -p "$R1" "$@" 2>&1 | norm >> "$REDIS_OUT" || true
}
gt() { # gedis 目标节点 G2
  echo "### TGT $*" >> "$GEDIS_OUT"
  redis-cli -p "$G2" "$@" 2>&1 | norm >> "$GEDIS_OUT" || true
}
rt() { # 真机目标节点 R2
  echo "### TGT $*" >> "$REDIS_OUT"
  redis-cli -p "$R2" "$@" 2>&1 | norm >> "$REDIS_OUT" || true
}
label2() { # 两侧固定标（命令拼写不同，只比回复）
  echo "### $1" >> "$GEDIS_OUT"
  echo "### $1" >> "$REDIS_OUT"
}

# 迁移前本地读写一致。
g SET "$KEY1" hello
r SET "$KEY1" hello

# 同一 slot 置 MIGRATING / IMPORTING（命令拼写两侧不同，只比对回复）。
label2 SETSLOT-MIGRATING
redis-cli -p "$G1" CLUSTER SETSLOT "$SLOT1" MIGRATING g2tgt 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" CLUSTER SETSLOT "$SLOT1" MIGRATING "$R2ID" 2>&1 | norm >> "$REDIS_OUT" || true
label2 SETSLOT-IMPORTING
redis-cli -p "$G2" CLUSTER SETSLOT "$SLOT1" IMPORTING g1src 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$G2" CLUSTER SETSLOT "$SLOT2" IMPORTING g1src 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R2" CLUSTER SETSLOT "$SLOT1" IMPORTING "$R1ID" 2>&1 | norm >> "$REDIS_OUT" || true
redis-cli -p "$R2" CLUSTER SETSLOT "$SLOT2" IMPORTING "$R1ID" 2>&1 | norm >> "$REDIS_OUT" || true

# key 仍在源端：本地命中。
g GET "$KEY1"
r GET "$KEY1"

# key 离开源端：ASK 转目标。
g DEL "$KEY1"
r DEL "$KEY1"
g GET "$KEY1"
r GET "$KEY1"

# ASKING 后打到源端：key 不存在，两侧同为 nil。
label2 ASKING-GET
printf 'ASKING\nGET %s\n' "$KEY1" | redis-cli -p "$G1" 2>&1 | norm >> "$GEDIS_OUT" || true
printf 'ASKING\nGET %s\n' "$KEY1" | redis-cli -p "$R1" 2>&1 | norm >> "$REDIS_OUT" || true

# MIGRATE 搬迁（move）：目标落值，随后源侧做 SETSLOT NODE 收尾。
g SET "$KEY1" world
r SET "$KEY1" world
label2 MIGRATE
redis-cli -p "$G1" MIGRATE 127.0.0.1 "$G2" "$KEY1" 0 5000 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" MIGRATE 127.0.0.1 "$R2" "$KEY1" 0 5000 2>&1 | norm >> "$REDIS_OUT" || true
label2 SETSLOT-NODE
redis-cli -p "$G1" CLUSTER SETSLOT "$SLOT1" NODE g2tgt 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$G2" CLUSTER SETSLOT "$SLOT1" NODE g2tgt 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" CLUSTER SETSLOT "$SLOT1" NODE "$R2ID" 2>&1 | norm >> "$REDIS_OUT" || true
redis-cli -p "$R2" CLUSTER SETSLOT "$SLOT1" NODE "$R2ID" 2>&1 | norm >> "$REDIS_OUT" || true
gt GET "$KEY1"
rt GET "$KEY1"
g GET "$KEY1"
r GET "$KEY1"

# MIGRATE COPY：源端保留；目标 importing 态无 ASKING 时读回 MOVED。
g SET "$KEY2" cval
r SET "$KEY2" cval
label2 MIGRATE-COPY
redis-cli -p "$G1" MIGRATE 127.0.0.1 "$G2" "$KEY2" 0 5000 COPY 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" MIGRATE 127.0.0.1 "$R2" "$KEY2" 0 5000 COPY 2>&1 | norm >> "$REDIS_OUT" || true
g GET "$KEY2"
r GET "$KEY2"
gt GET "$KEY2"
rt GET "$KEY2"

# 目标已存在：BUSYKEY；REPLACE 后覆盖。目标写需同连接先 ASKING。
label2 TGT-SETUP
printf 'ASKING\nSET %s other\n' "$KEY2" | redis-cli -p "$G2" 2>&1 | norm >> "$GEDIS_OUT" || true
printf 'ASKING\nSET %s other\n' "$KEY2" | redis-cli -p "$R2" 2>&1 | norm >> "$REDIS_OUT" || true
label2 MIGRATE-BUSYKEY
redis-cli -p "$G1" MIGRATE 127.0.0.1 "$G2" "$KEY2" 0 5000 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" MIGRATE 127.0.0.1 "$R2" "$KEY2" 0 5000 2>&1 | norm >> "$REDIS_OUT" || true
label2 MIGRATE-REPLACE
redis-cli -p "$G1" MIGRATE 127.0.0.1 "$G2" "$KEY2" 0 5000 REPLACE 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" MIGRATE 127.0.0.1 "$R2" "$KEY2" 0 5000 REPLACE 2>&1 | norm >> "$REDIS_OUT" || true
gt GET "$KEY2"
rt GET "$KEY2"
# 源端 key 清空后做 SLOT2 的 NODE 收尾，随后目标可读、源端 MOVED。
g DEL "$KEY2"
r DEL "$KEY2"
label2 SETSLOT-NODE2
redis-cli -p "$G1" CLUSTER SETSLOT "$SLOT2" NODE g2tgt 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$G2" CLUSTER SETSLOT "$SLOT2" NODE g2tgt 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" CLUSTER SETSLOT "$SLOT2" NODE "$R2ID" 2>&1 | norm >> "$REDIS_OUT" || true
redis-cli -p "$R2" CLUSTER SETSLOT "$SLOT2" NODE "$R2ID" 2>&1 | norm >> "$REDIS_OUT" || true
gt GET "$KEY2"
rt GET "$KEY2"
g GET "$KEY2"
r GET "$KEY2"

# 缺 key：NOKEY。
label2 MIGRATE-NOKEY
redis-cli -p "$G1" MIGRATE 127.0.0.1 "$G2" "$KEY0" 0 5000 2>&1 | norm >> "$GEDIS_OUT" || true
redis-cli -p "$R1" MIGRATE 127.0.0.1 "$R2" "$KEY0" 0 5000 2>&1 | norm >> "$REDIS_OUT" || true

# kill -9 源端后重启：属主映射按落盘恢复，源端读已迁走 key 仍 MOVED。
kill -9 "$G1_PID" 2>/dev/null || { echo "G1 already dead before restart, abort"; exit 1; }
wait "$G1_PID" 2>/dev/null || true
[ -f "$G1DATA/nodes.conf" ] || { echo "nodes.conf missing after SETSLOT, abort"; exit 1; }
"$TMP/gedis" -config "$TMP/g1.toml" >/dev/null 2>&1 &
G1_PID=$!
redis-cli -p "$R1" shutdown nosave >/dev/null 2>&1 || true
redis-server --port "$R1" --dir "$TMP/r1" --dbfilename dump.rdb \
  --cluster-enabled yes --cluster-config-file "$TMP/nodes-$R1.conf" \
  --cluster-node-timeout 5000 --save '' --appendonly no --daemonize yes >/dev/null 2>&1
for i in $(seq 1 100); do
  if redis-cli -p "$G1" ping >/dev/null 2>&1 \
    && redis-cli -p "$R1" ping >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
# R1 重启后需等集群状态收敛为 ok，否则读报 CLUSTERDOWN。
for i in $(seq 1 100); do
  if [ "$(redis-cli -p "$R1" cluster info 2>/dev/null | grep -c cluster_state:ok)" = "1" ]; then
    break
  fi
  sleep 0.1
done
g GET "$KEY1"
r GET "$KEY1"

if diff -u "$REDIS_OUT" "$GEDIS_OUT"; then
  echo "CLUSTER-MIGRATE COMPAT OK"
else
  echo "CLUSTER-MIGRATE COMPAT MISMATCH (redis vs gedis above)"
  echo "preserved: $TMP"
  trap - EXIT
  exit 1
fi
