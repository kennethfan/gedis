#!/usr/bin/env bash
# Sentinel 自动转移集成验收：3 哨兵 + 1 主 2 从，kill -9 主后 30s 内
# 恰好一次 +switch-master，且 GET-MASTER-ADDR 指向同一新主；随后手动
# SENTINEL failover 仍可用（OK 且无崩溃）。
set -euo pipefail

M=7390; S1=7391; S2=7392
A=27400; B=27401; C=27402
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="$(mktemp -d)"
SWLOG="$TMP/switch.log"

cleanup() {
  for v in PID_M PID_S1 PID_S2 PID_A PID_B PID_C PID_SUB_A PID_SUB_B PID_SUB_C; do
    pid="${!v:-}"
    [[ -n "$pid" ]] && kill "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP"
}
trap cleanup EXIT

cd "$ROOT"
CGO_ENABLED=0 go build -trimpath -o "$TMP/gedis" ./cmd/gedis

# 预清理：历史残留进程可能占着端口（脚本被超时杀掉时 trap 来不及跑）。
for p in $M $S1 $S2 $A $B $C 27500 27501 27502; do
  holder="$(lsof -ti :$p 2>/dev/null || true)"
  if [[ -n "$holder" ]]; then kill -9 $holder 2>/dev/null || true; fi
done

data_toml() { # $1=port $2=file
  cat > "$2" <<EOF
[server]
host = "127.0.0.1"
port = $1
[storage]
datadir = "$TMP/d$1"
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
}

sent_toml() { # $1=server-port $2=sentinel-port $3=self-seed-excluded-peer1 $4=peer2 $5=file
  cat > "$5" <<EOF
[server]
host = "127.0.0.1"
port = $1
[storage]
datadir = "$TMP/d$1"
[memory]
maxmemory = 1073741824
policy = "allkeys-lru"
[persistence]
appendonly = false
fsync = "everysec"
[metrics]
enabled = false
port = 0
[sentinel]
enabled = true
port = $2
down_after_ms = 1500
failover_timeout_ms = 15000
sentinels = ["127.0.0.1:$3", "127.0.0.1:$4"]
[[sentinel.masters]]
name = "mymaster"
master_addr = "127.0.0.1:$M"
quorum = 2
slaves = ["127.0.0.1:$S1", "127.0.0.1:$S2"]
EOF
}

data_toml $M "$TMP/m.toml"; data_toml $S1 "$TMP/s1.toml"; data_toml $S2 "$TMP/s2.toml"
sent_toml 27500 $A $B $C "$TMP/a.toml"
sent_toml 27501 $B $A $C "$TMP/b.toml"
sent_toml 27502 $C $A $B "$TMP/c.toml"

"$TMP/gedis" -config "$TMP/m.toml" >"$TMP/m.log" 2>&1 &
PID_M=$!
"$TMP/gedis" -config "$TMP/s1.toml" >"$TMP/s1.log" 2>&1 &
PID_S1=$!
"$TMP/gedis" -config "$TMP/s2.toml" >"$TMP/s2.log" 2>&1 &
PID_S2=$!

wait_port() { # $1=port $2=what
  for _ in $(seq 1 100); do
    if redis-cli -p "$1" ping >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "FAIL: $2 (port $1) not ready"; return 1
}
wait_port $M master || exit 1; wait_port $S1 slave1 || exit 1; wait_port $S2 slave2 || exit 1
redis-cli -p $S1 REPLICAOF 127.0.0.1 $M >/dev/null
redis-cli -p $S2 REPLICAOF 127.0.0.1 $M >/dev/null
sleep 1

"$TMP/gedis" -config "$TMP/a.toml" >"$TMP/a.log" 2>&1 &
PID_A=$!
"$TMP/gedis" -config "$TMP/b.toml" >"$TMP/b.log" 2>&1 &
PID_B=$!
"$TMP/gedis" -config "$TMP/c.toml" >"$TMP/c.log" 2>&1 &
PID_C=$!
wait_port $A sentinelA || { tail -20 "$TMP/a.log"; cp "$TMP/a.log" /tmp/sentinelA-fail.log; exit 1; }
wait_port $B sentinelB || exit 1; wait_port $C sentinelC || exit 1

sub() { # $1=port $2=logfile → PID_SUB_$1
  python3 - "$1" "$2" <<'EOF' &
import socket, sys
port, path = int(sys.argv[1]), sys.argv[2]
s = socket.create_connection(("127.0.0.1", port))
s.sendall(b"*2\r\n$9\r\nSUBSCRIBE\r\n$14\r\n+switch-master\r\n")
f = open(path, "w", buffering=1)
buf = b""
while True:
    d = s.recv(4096)
    if not d:
        break
    buf += d
    while b"\n" in buf:
        line, buf = buf.split(b"\n", 1)
        f.write(line.decode(errors="replace") + "\n")
EOF
}
SWLOG_A="$TMP/switch-a.log"; SWLOG_B="$TMP/switch-b.log"; SWLOG_C="$TMP/switch-c.log"
sub $A "$SWLOG_A"; PID_SUB_A=$!
sub $B "$SWLOG_B"; PID_SUB_B=$!
sub $C "$SWLOG_C"; PID_SUB_C=$!
PID_SUB=$PID_SUB_A
for _ in $(seq 1 50); do
  if grep -q subscribe "$SWLOG_A" "$SWLOG_B" "$SWLOG_C" 2>/dev/null; then break; fi
  sleep 0.1
done

if kill -0 "$PID_M" 2>/dev/null; then kill -9 "$PID_M"; else
  echo "FAIL: master already dead before kill:"; tail -20 "$TMP/m.log"; exit 1
fi
PID_M=""
echo "master killed, waiting for auto failover..."

NEW=""
for _ in $(seq 1 60); do
  sleep 0.5
  got="$(redis-cli -p $A SENTINEL get-master-addr-by-name mymaster 2>/dev/null | tr '\n' ' ' || true)"
  if [[ "$got" != *"7390"* && -n "$got" ]]; then NEW="$got"; break; fi
done
if [[ -z "$NEW" ]]; then
  echo "FAIL: no auto failover within 30s (master still 7390)"; exit 1
fi
sleep 3 # 静默期：确认无第二次切换
TOTAL="$(grep -h 'mymaster 127.0.0.1' "$SWLOG_A" "$SWLOG_B" "$SWLOG_C" | wc -l | tr -d ' ')"
if [[ "$TOTAL" != "1" ]]; then
  echo "FAIL: expected exactly 1 +switch-master total, got $TOTAL"; exit 1
fi
LINE="$(grep -h 'mymaster 127.0.0.1' "$SWLOG_A" "$SWLOG_B" "$SWLOG_C" | head -1)"
if [[ "$LINE" != *"${NEW% }"* ]]; then
  echo "FAIL: switch event [$LINE] disagrees with GET-MASTER-ADDR [$NEW]"; exit 1
fi
echo "auto failover OK: $LINE"

MAN="$(redis-cli -p $A SENTINEL failover mymaster 2>&1 || true)"
if [[ "$MAN" != "OK" ]]; then
  echo "FAIL: manual failover after auto: $MAN"; exit 1
fi
echo "manual failover still OK"
echo "PASS"
