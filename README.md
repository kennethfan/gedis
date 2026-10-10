# Gedis

A Redis-compatible storage engine in Go, backed by Pebble (pure-Go LSM-tree).

[中文文档](README.zh-CN.md)

## Features

- **Redis compatible** — RESP2/RESP3, drop-in replacement (`redis-cli` works directly)
- **Pebble storage** — pure Go, no CGo, WAL + tunable fsync
- **Data structures** — String, Hash, List, Set, Sorted Set, Geo, Bitmap, HyperLogLog, Stream (with adaptive encodings; Geo reuses ZSet, Bitmap reuses String)
- **Expiration** — key-level TTL + Hash field expiration (HEXPIRE family), lazy + background active deletion
- **Replication** — PSYNC (full RDB + backlog partial sync), read-only replicas
- **Memory management** — maxmemory + 8 eviction policies (allkeys/volatile × lru/lfu/random/ttl), runtime CONFIG
- **Keyspace notifications** — `CONFIG SET notify-keyspace-events KEA` full-class events (g/s/h/l/z/x/e/m), keyspace + keyevent channels
- **New commands** — GETSET/SETEX/PSETEX/SETNX/TOUCH, LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH, READONLY/READWRITE, OBJECT IDLETIME/FREQ, HEXPIRETIME/HPEXPIRETIME, SUBSTR, SWAPDB, COMMAND LIST
- **Live command stream** — MONITOR / CLIENT MONITOR with per-conn serialized outbound pipe + per-conn write lock
- **Client-side caching** — CLIENT TRACKING (on/off, BCAST/PREFIX, OPTIN/CACHING, OPTOUT, NOLOOP); invalidation via RESP3 push / RESP2 `__redis__:invalidate`, independent of notify-keyspace-events
- **Monitoring** — six-section INFO + SLOWLOG + Prometheus `/metrics`
- **Server-side TLS** — listener-side `[server] tls_cert/tls_key` (outbound dial-side TLS follows `[tls]` config via `tlsdial`)

## Quick Start

```bash
# Build (static binary, no CGo)
make build

# Start (default gedis.toml, port 6380)
./bin/gedis

# Connect with redis-cli
redis-cli -p 6380
```

## Docker

```bash
# Build image (multi-stage, scratch runtime ~25MB)
make docker

# Single node
docker run -d -p 6380:6380 -v gedis-data:/data gedis:latest

# Local master-replica test
make compose-up
redis-cli -p 6381 REPLICAOF 127.0.0.1 6380
make compose-down
```

## Configuration

TOML format (see `gedis.toml`; inside containers use `gedis.docker.toml`):

```toml
[server]
host = "127.0.0.1"
port = 6380

[storage]
datadir = "./data"

[memory]
maxmemory = 1073741824
policy = "allkeys-lru"      # or volatile-lru

[persistence]
appendonly = true
fsync = "everysec"          # always | everysec | no

[metrics]
enabled = false
port = 9121
```

Runtime tuning: `CONFIG SET maxmemory <bytes>`, `CONFIG SET maxmemory-policy <allkeys-lru|allkeys-lfu|allkeys-random|volatile-lru|volatile-lfu|volatile-random|volatile-ttl|noeviction>`, `CONFIG GET maxmemory`, `CONFIG SET notify-keyspace-events <KgEgse...>`.

## Replication

```bash
# On the replica (read-only mode turns on automatically)
REPLICAOF <master-host> <master-port>

# Promote to master
REPLICAOF NO ONE
```

## Monitoring

```bash
INFO [server|clients|memory|stats|replication|keyspace]
SLOWLOG GET [n] | LEN | RESET
curl localhost:9121/metrics   # requires [metrics] enabled
```

## Project Structure

```
gedis/
├── cmd/gedis/          # Main entrypoint
├── internal/
│   ├── network/          # TCP server + Router + stats/slowlog
│   ├── protocol/         # RESP2/RESP3 codec
│   ├── storage/          # Pebble storage + WAL/fsync/batch/memory accounting
│   ├── datastruct/       # Encodings (string/hash/list/set/zset/stream/hll + adaptive; geo→zset, bitmap→string)
│   ├── commands/         # All command implementations
│   ├── replication/      # Backlog/RDB/Hub/replica client
│   ├── metrics/          # Prometheus exposition
│   └── config/           # TOML configuration
├── docs/
│   └── adr/              # Architecture decision records
├── Dockerfile            # Multi-stage build
├── docker-compose.yml    # Local master-replica test
├── gedis.toml          # Local config example
└── gedis.docker.toml   # In-container config
```

## Development

```bash
make test    # Full suite (-race)
make vet     # Static checks
make bench   # Benchmarks
```

## Scope (not yet implemented)

RDB/AOF persistence (BGSAVE/SAVE return an honest error; no RDB/AOF) and CONFIG REWRITE (honest rejection).

## License

MIT License
