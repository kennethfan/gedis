# Redrock

A Redis-compatible storage engine in Go, backed by Pebble (pure-Go LSM-tree).

[中文文档](README.zh-CN.md)

## Features

- **Redis compatible** — RESP2/RESP3, drop-in replacement (`redis-cli` works directly)
- **Pebble storage** — pure Go, no CGo, WAL + tunable fsync
- **Data structures** — String, Hash, List, Set (with adaptive encodings)
- **Expiration** — key-level TTL + Hash field expiration (HEXPIRE family), lazy + background active deletion
- **Replication** — PSYNC (full RDB + backlog partial sync), read-only replicas
- **Memory management** — maxmemory + allkeys-lru / volatile-lru, runtime CONFIG
- **Monitoring** — six-section INFO + SLOWLOG + Prometheus `/metrics`

## Quick Start

```bash
# Build (static binary, no CGo)
make build

# Start (default redrock.toml, port 6380)
./bin/redrock

# Connect with redis-cli
redis-cli -p 6380
```

## Docker

```bash
# Build image (multi-stage, scratch runtime ~25MB)
make docker

# Single node
docker run -d -p 6380:6380 -v redrock-data:/data redrock:latest

# Local master-replica test
make compose-up
redis-cli -p 6381 REPLICAOF 127.0.0.1 6380
make compose-down
```

## Configuration

TOML format (see `redrock.toml`; inside containers use `redrock.docker.toml`):

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

Runtime tuning: `CONFIG SET maxmemory <bytes>`, `CONFIG SET maxmemory-policy <allkeys-lru|volatile-lru>`, `CONFIG GET maxmemory`.

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
redrock/
├── cmd/redrock/          # Main entrypoint
├── internal/
│   ├── network/          # TCP server + Router + stats/slowlog
│   ├── protocol/         # RESP2/RESP3 codec
│   ├── storage/          # Pebble storage + WAL/fsync/batch/memory accounting
│   ├── datastruct/       # Encodings (string/hash/list/set + adaptive)
│   ├── commands/         # All command implementations
│   ├── replication/      # Backlog/RDB/Hub/replica client
│   ├── metrics/          # Prometheus exposition
│   └── config/           # TOML configuration
├── docs/
│   └── adr/              # Architecture decision records
├── Dockerfile            # Multi-stage build
├── docker-compose.yml    # Local master-replica test
├── redrock.toml          # Local config example
└── redrock.docker.toml   # In-container config
```

## Development

```bash
make test    # Full suite (-race)
make vet     # Static checks
make bench   # Benchmarks
```

## Scope (not in v1)

ZSet, Streams, Pub/Sub, Lua, MULTI/transactions, Cluster, Sentinel.

## License

MIT License
