# ADR-0002: 架构设计

## Status

Accepted

## Context

需要设计 Redrock 的整体架构，包括并发模型、网络层、数据结构实现、复制机制等。

## Decision

### 并发模型

采用 **多线程 I/O + 单线程命令执行** 模型（类似 Redis 6+）：

- I/O 线程池处理网络读写
- 命令解析和执行保持单线程
- 避免锁竞争，兼容 Redis 命令语义

### 网络层

- 使用 epoll/kqueue 事件驱动
- 支持 RESP2 和 RESP3 协议
- 客户端握手时协商协议版本

### 数据结构实现

- **String**：直接存储，支持整数优化
- **Hash**：小 hash 用 ziplist 编码，大 hash 用 hashtable
- **List**：小 list 用 ziplist，大 list 用 quicklist
- **Set**：小 set 用 intset/ziplist，大 set 用 hashtable
- **Sorted Set**：小 zset 用 ziplist，大 zset 用 skiplist + hashtable

### 存储组织

使用前缀方案在 RocksDB 中组织数据：

```
s:<key> -> String value
h:<key>:<field> -> Hash value
l:<key>:<index> -> List element
st:<key>:<member> -> Set member
z:<key>:<score>:<member> -> Sorted Set member
```

### 持久化

- WAL 可选开启
- 使用 Write Batch 批量写入
- fsync 策略可配置：always, everysec, no

### 复制

- 实现 PSYNC 协议
- 支持全量同步（RDB 传输）
- 支持增量同步（积压缓冲区）

### 内存管理

- 使用 LRU 缓存分离热数据和冷数据
- 配置文件指定最大内存
- 运行时支持 `CONFIG SET` 动态调整

## Consequences

### 优势

- 与 Redis 6+ 行为一致，兼容性好
- 多线程 I/O 提升网络吞吐
- 单线程命令执行避免锁竞争
- 灵活的数据结构编码策略

### 劣势

- 实现复杂度较高
- 需要维护多种数据结构的编码逻辑
- 复制协议实现复杂

## Implementation Plan

### Phase 1: Core (v0.1)

- String 命令
- Hash 命令
- List 命令
- Set 命令
- 基础持久化
- 单线程命令执行

### Phase 2: Replication (v0.2)

- 主从复制
- PSYNC 协议
- RDB 传输

### Phase 3: Advanced (v0.3)

- Sorted Set
- Stream
- HyperLogLog
- 集群接口预留

### Phase 4: Production (v1.0)

- 完整监控
- 性能优化
- 压力测试
- 文档完善

## References

- [Redis 6.0 非阻塞 I/O](https://redis.io/topics/threads)
- [Redis 复制文档](https://redis.io/topics/replication)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
