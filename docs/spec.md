# Gedis Specification

## Problem Statement

现有的 Redis 持久化方案（RDB 快照和 AOF 日志）存在以下问题：
- RDB 快照在写入密集场景下性能较差
- AOF 文件会随时间增长，重启恢复时间长
- 内存限制导致无法存储超过物理内存的数据量
- 缺乏灵活的压缩和存储优化选项

用户需要一个基于现代 LSM-tree 存储引擎的 Redis 兼容方案，提供更好的持久化性能、更灵活的存储配置，同时保持完全的 Redis 兼容性。

## Solution

构建 Gedis —— 一个基于 Pebble（纯 Go LSM-tree）的 Redis 兼容存储引擎，用 Go 实现。该方案：

- 使用 Pebble 作为底层存储引擎，利用 LSM-tree 架构提升写入性能
- 保持完全的 Redis 协议和命令兼容性，可作为 drop-in 替换
- 支持可配置的持久化策略（WAL、fsync 策略）
- 通过 LRU 缓存实现冷热数据分离
- 支持主从复制，保证高可用性

## User Stories

### 核心功能

1. As a Redis user, I want to use Gedis as a drop-in replacement for Redis, so that I don't need to change my application code
2. As a Redis user, I want Gedis to support all Redis String commands, so that I can use it for basic key-value operations
3. As a Redis user, I want Gedis to support all Redis Hash commands, so that I can store and retrieve structured data
4. As a Redis user, I want Gedis to support all Redis List commands, so that I can use it for queue and stack operations
5. As a Redis user, I want Gedis to support all Redis Set commands, so that I can perform set operations like intersection and union
6. As a Redis user, I want Gedis to support all Redis Sorted Set commands, so that I can use it for ranking and scoring operations
7. As a Redis user, I want Gedis to support RESP2 and RESP3 protocols, so that I can use it with any Redis client

### 持久化

8. As a Redis user, I want Gedis to persist data to disk using Pebble, so that my data survives restarts
9. As a Redis user, I want to configure the WAL (Write-Ahead Log) behavior, so that I can balance between performance and durability
10. As a Redis user, I want to choose the fsync strategy (always, everysec, no), so that I can control the durability-performance tradeoff
11. As a Redis user, I want Gedis to use Write Batch for efficient writes, so that I get better write throughput
12. As a Redis user, I want Gedis to support configurable compression algorithms, so that I can optimize for storage space vs CPU usage

### 内存管理

13. As a Redis user, I want to set a maximum memory limit, so that Gedis doesn't consume excessive memory
14. As a Redis user, I want Gedis to use LRU eviction, so that hot data stays in memory while cold data is evicted
15. As a Redis user, I want to change the memory limit at runtime using CONFIG SET, so that I can adjust without restarting
16. As a Redis user, I want to monitor memory usage via INFO command, so that I can track resource consumption

### 复制

17. As a Redis user, I want Gedis to support master-slave replication, so that I can achieve high availability
18. As a Redis user, I want Gedis to implement PSYNC protocol, so that it works with existing Redis replication infrastructure
19. As a Redis user, I want Gedis to support full synchronization via RDB transfer, so that new replicas can join the cluster
20. As a Redis user, I want Gedis to support incremental synchronization, so that temporary disconnections don't require full re-sync

### 过期策略

21. As a Redis user, I want Gedis to support key expiration, so that I can use it for cache with TTL
22. As a Redis user, I want Gedis to use lazy deletion (check on read), so that expired keys don't consume memory
23. As a Redis user, I want Gedis to use periodic deletion (background scan), so that expired keys are cleaned up proactively

### 监控与运维

24. As a Redis user, I want Gedis to support the INFO command, so that I can get server status and statistics
25. As a Redis user, I want Gedis to expose Prometheus metrics, so that I can integrate with monitoring systems
26. As a Redis user, I want Gedis to support slow query log, so that I can identify performance bottlenecks
27. As a Redis user, I want Gedis to support CONFIG GET/SET commands, so that I can manage configuration at runtime

### 部署

28. As a DevOps engineer, I want Gedis to be a single binary, so that deployment is simple
29. As a DevOps engineer, I want Gedis to provide a Docker image, so that I can deploy in containerized environments
30. As a DevOps engineer, I want Gedis to use TOML configuration, so that configuration is easy to read and modify

## Implementation Decisions

### 项目结构

采用模块化目录结构：
- `cmd/gedis/` - 主程序入口
- `internal/server/` - 网络服务器
- `internal/protocol/` - RESP 协议解析
- `internal/storage/` - Pebble 存储层
- `internal/datastruct/` - 数据结构实现
- `internal/replication/` - 主从复制
- `internal/config/` - 配置管理

### 存储引擎

- 使用 Pebble（纯 Go LSM-tree）作为底层存储引擎（ADR-0003，取代 ADR-0001）
- 通过 `github.com/cockroachdb/pebble` 直接集成，无 CGo
- 使用前缀方案组织数据：
  - `s:` 前缀 - String 类型
  - `h:` 前缀 - Hash 类型
  - `l:` 前缀 - List 类型
  - `st:` 前缀 - Set 类型
  - `z:` 前缀 - Sorted Set 类型

### 并发模型

采用 Redis 6+ 的多线程 I/O 模型：
- I/O 线程池处理网络读写
- 命令解析和执行保持单线程
- 避免锁竞争，兼容 Redis 命令语义

### 数据结构编码

- **String**：直接存储，支持整数优化
- **Hash**：小 hash 用 ziplist 编码，大 hash 用 hashtable
- **List**：小 list 用 ziplist，大 list 用 quicklist
- **Set**：小 set 用 intset/ziplist，大 set 用 hashtable
- **Sorted Set**：小 zset 用 ziplist，大 zset 用 skiplist + hashtable

### 持久化策略

- WAL 可选开启
- 使用 Write Batch 批量写入
- fsync 策略可配置：always, everysec, no

### 配置管理

- 使用 TOML 格式的配置文件
- 支持 CONFIG GET/SET 命令进行运行时配置
- 配置项包括：端口、数据目录、Pebble 参数、内存限制、持久化策略等

### 测试策略

- 集成测试：启动 Gedis 和 Redis，对比命令执行结果
- 使用 Redis 官方测试套件进行回归测试
- 单元测试覆盖核心数据结构和存储逻辑

## Testing Decisions

### 测试原则

- 只测试外部行为，不测试实现细节
- 与 Redis 官方行为进行对比验证
- 覆盖正常路径和错误路径

### 测试模块

- 协议解析测试：验证 RESP2/RESP3 协议的正确解析
- 数据结构测试：验证每种数据结构的命令执行结果
- 存储层测试：验证 Pebble 的读写操作
- 复制测试：验证主从同步的正确性
- 集成测试：验证与 Redis 客户端的兼容性

### 参考测试

- Redis 官方测试套件（tests/ 目录）
- 使用 redis-cli 进行手动验证
- 使用现有 Redis 客户端库进行自动化测试

## Out of Scope

以下功能不在本规格范围内：

- **集群模式**：第一版只支持单节点，集群接口预留但不实现
- **Sentinel 支持**：Redis 哨兵模式不在第一版范围
- **Module 系统**：不支持 Redis Module API
- **Lua 脚本**：不支持 EVAL/EVALSHA 命令
- **事务支持**：MULTI/EXEC 事务不在第一版范围
- **Pub/Sub**：发布订阅功能不在第一版范围
- **Stream 数据结构**：第一版不实现，预留接口

## Further Notes

### 实现顺序

按照 ADR-0002 的实现计划：
1. Phase 1 (v0.1)：核心功能 - String, Hash, List, Set 命令
2. Phase 2 (v0.2)：复制功能 - 主从复制，PSYNC 协议
3. Phase 3 (v0.3)：高级功能 - Sorted Set, Stream, HyperLogLog
4. Phase 4 (v1.0)：生产就绪 - 完整监控，性能优化

### 参考项目

- [KeyDB](https://keydb.dev/) - Redis 兼容的高性能存储
- [Dragonfly](https://www.dragonflydb.io/) - 现代 Redis 替代方案
- [Pebble](https://github.com/cockroachdb/pebble) - Go 原生 LSM-tree 存储

### 性能目标

- 写入吞吐：> 100K ops/sec
- 读取延迟：P99 < 1ms
- 内存效率：比原生 Redis 节省 30%+ 内存（通过压缩和编码优化）
