# Gedis

基于 Pebble（纯 Go LSM-tree）的 Redis 兼容存储引擎，用 Go 实现。

[English](README.md)

## Features

- **Redis 兼容** — RESP2/RESP3，可作为 drop-in 替换（`redis-cli` 直连）
- **Pebble 存储引擎** — 纯 Go，无 CGo 依赖，WAL + 可调 fsync
- **数据结构** — String， Hash， List， Set（含自适应编码）
- **过期** — key 级 TTL + Hash field 过期（HEXPIRE 系），被动 + 后台主动删除
- **主从复制** — PSYNC（全量 RDB + backlog 部分同步），只读复本
- **内存管理** — maxmemory + allkeys-lru / volatile-lru，CONFIG 运行时调整
- **监控** — INFO 六 section + SLOWLOG + Prometheus `/metrics`

## Quick Start

```bash
# 编译（纯静态二进制，无 CGo）
make build

# 启动（默认 gedis.toml，端口 6380）
./bin/gedis

# 使用 redis-cli 连接
redis-cli -p 6380
```

## Docker

```bash
# 构建镜像（多阶段，scratch 运行时约 25MB）
make docker

# 单节点
docker run -d -p 6380:6380 -v gedis-data:/data gedis:latest

# 一主一从本地测试
make compose-up
redis-cli -p 6381 REPLICAOF 127.0.0.1 6380
make compose-down
```

## Configuration

TOML 格式（见 `gedis.toml`，容器内用 `gedis.docker.toml`）：

```toml
[server]
host = "127.0.0.1"
port = 6380

[storage]
datadir = "./data"

[memory]
maxmemory = 1073741824
policy = "allkeys-lru"      # 或 volatile-lru

[persistence]
appendonly = true
fsync = "everysec"          # always | everysec | no

[metrics]
enabled = false
port = 9121
```

运行时调整：`CONFIG SET maxmemory <bytes>`，`CONFIG SET maxmemory-policy <allkeys-lru|volatile-lru>`，`CONFIG GET maxmemory`。

## Replication

```bash
# 从库执行（只读模式自动开启）
REPLICAOF <master-host> <master-port>

# 升主
REPLICAOF NO ONE
```

## Monitoring

```bash
INFO [server|clients|memory|stats|replication|keyspace]
SLOWLOG GET [n] | LEN | RESET
curl localhost:9121/metrics   # 需 [metrics] enabled
```

## Project Structure

```
gedis/
├── cmd/gedis/          # 主程序入口
├── internal/
│   ├── network/          # TCP 服务 + Router + 统计/慢日志
│   ├── protocol/         # RESP2/RESP3 编解码
│   ├── storage/          # Pebble 存储 + WAL/fsync/批量/内存核算
│   ├── datastruct/       # 编码（string/hash/list/set + 自适应）
│   ├── commands/         # 全部命令实现
│   ├── replication/      # Backlog/RDB/Hub/复本客户端
│   ├── metrics/          # Prometheus exposition
│   └── config/           # TOML 配置
├── docs/
│   └── adr/              # 架构决策记录
├── Dockerfile            # 多阶段构建
├── docker-compose.yml    # 一主一从本地测试
├── gedis.toml          # 本地配置示例
└── gedis.docker.toml   # 容器内配置
```

## Development

```bash
make test    # 全量测试（-race）
make vet     # 静态检查
make bench   # 基准测试
```

## Scope（v1 未实现）

ZSet、Streams、Pub/Sub、Lua、MULTI/事务、Cluster、Sentinel。

## License

MIT License
