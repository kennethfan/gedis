# ADR-0001: RocksDB 作为存储引擎

## Status

Superseded by ADR-0003 (2026-09-22) — 切换到纯 Go 的 Pebble，消除 CGo 依赖。

## Context

需要为 Redis 兼容存储选择底层存储引擎。候选方案：

1. **RocksDB** - Facebook 开发的嵌入式 KV 存储， LSM-tree 架构
2. **Pebble** - Go 原生实现的 LSM-tree，兼容 RocksDB API
3. **Badger** - Go 原生 KV 存储，WiscKey 架构
4. **自定义实现** - 基于 B+tree 或其他数据结构

## Decision

选择 **RocksDB** 作为存储引擎。

## Consequences

### 优势

- **成熟的 LSM-tree 实现**：经过大规模生产验证（MySQL, PostgreSQL, CockroachDB 等都在使用）
- **高性能写入**：LSM-tree 架构天然适合写密集场景
- **可配置的压缩策略**：支持多种压缩算法和压缩级别
- **丰富的功能**：Column Family、Write Batch、事务支持等

### 劣势

- **CGo 依赖**：RocksDB 是 C++ 实现，Go 需要通过 CGo 调用
- **编译复杂**：需要编译 RocksDB 静态库
- **内存管理**：需要与 Go 的 GC 协调

### 缓解措施

- 使用 `gorocksdb` 绑定库，成熟度高
- 提供预编译的静态库
- 通过配置项限制 RocksDB 的内存使用

## Alternatives Considered

- **Pebble**：Go 原生实现，但功能不如 RocksDB 丰富，且团队更熟悉 RocksDB
- **Badger**：性能不错，但生产验证不如 RocksDB
- **自定义实现**：工作量太大，且难以达到 RocksDB 的性能水平
