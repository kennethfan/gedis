# Redrock Domain Model

## Glossary

| Term | Definition |
|------|------------|
| **Redrock** | 基于 Pebble 的 Redis 兼容存储引擎，用 Go 实现 |
| **Redis 兼容** | 完全兼容 Redis 协议和命令，可作为 drop-in 替换 |
| **存储引擎** | Pebble（纯 Go LSM-tree），提供持久化、高吞吐的键值存储 |
| **数据结构** | Redis 支持的数据类型：String, Hash, List, Set, Sorted Set 等 |
| **RESP 协议** | Redis Serialization Protocol，支持 RESP2 和 RESP3 两个版本 |
| **主从复制** | Redis 的异步复制机制，支持全量同步（PSYNC）和增量同步 |
| **LRU 缓存** | Least Recently Used 缓存策略，用于分离热数据和冷数据 |
| **WAL** | Write-Ahead Log，预写日志，用于崩溃恢复（Pebble 内建） |
| **Write Batch** | 批量写入机制，提升写入吞吐（Pebble `Batch`） |
| **Prefix** | Key 前缀，用于在 Pebble 中组织不同数据结构 |
| **惰性删除** | 读取时检查 key 是否过期，过期则删除 |
| **定时删除** | 后台线程定期扫描并删除过期 key |
| **INFO 命令** | Redis 的信息查询命令，返回服务器状态和统计信息 |
| **Prometheus** | 开源监控系统，通过 /metrics 端点暴露指标 |

## Core Concepts

### 数据模型

Redrock 使用 Pebble 作为底层存储引擎，通过前缀方案在同一个 Pebble 实例中存储所有 Redis 数据结构：

- `s:` 前缀 - String 类型
- `h:` 前缀 - Hash 类型
- `l:` 前缀 - List 类型
- `st:` 前缀 - Set 类型
- `z:` 前缀 - Sorted Set 类型

### 并发模型

采用 Redis 6+ 的多线程 I/O 模型：
- I/O 线程池处理网络读写
- 命令执行保持单线程，避免锁竞争
- 兼容 Redis 的命令执行语义

### 持久化策略

支持可配置的持久化策略：
- WAL 可选开启
- Write Batch 批量写入
- fsync 策略可配置：always, everysec, no

## Design Decisions

- 使用 Pebble 作为存储引擎（ADR-0003，取代 ADR-0001）
- 采用 Redis 6+ 并发模型（ADR-0002）
- 实现主从复制协议（ADR-0002）

## References

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Pebble](https://github.com/cockroachdb/pebble)
- [KeyDB](https://keydb.dev/) - 参考项目
- [Dragonfly](https://www.dragonflydb.io/) - 参考项目
