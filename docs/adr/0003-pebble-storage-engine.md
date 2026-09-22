# ADR-0003: Pebble 作为存储引擎（取代 RocksDB）

## Status

Accepted (2026-09-22)，取代 ADR-0001。

## Context

ADR-0001 选择了 RocksDB + `gorocksdb`（CGo 绑定）。Ticket #2 实施时发现：

1. macOS 本机没有 `librocksdb`，`brew install rocksdb` 需从源码编译 cmake 等依赖，耗时 10 分钟以上；
2. CGo 导致交叉编译困难，与“单 binary + Docker 顺滑部署”的目标冲突；
3. CI 需要额外安装 `librocksdb-dev`，增加构建复杂度。

## Decision

切换到 **Pebble**（`github.com/cockroachdb/pebble`，CockroachDB 出品的纯 Go LSM 引擎）。

## Consequences

### 优势

- **零 CGo**：`go build` 即走，交叉编译无压力，Docker 单阶段构建即可；
- **自带 WAL**：崩溃恢复开箱即用，Ticket #4 的持久化层可直接复用其语义；
- **生产验证**：CockroachDB、etcd（已迁移）都在用；
- **API 稳定**：Open/Get/Set/Delete/Batch 语义与 RocksDB 对齐，后续虚拟 Column Family 可用前缀模拟。

### 劣势

- **单机极限性能略低于 C++ RocksDB**：差距小，且 Redrock 首版瓶颈在协议层而非引擎；
- **生态工具少**：没有 `ldb` 这类离线检修工具，调试靠 Go 测试 + `pebble tool`。

### 变更面

- `internal/storage` 改用 Pebble；`go.mod` 移除 `gorocksdb`；
- CI 去掉 `librocksdb-dev` 安装步骤；
- Ticket #2 描述更新；Ticket #4/#5 备注 Pebble 影响（WAL 内建、静态二进制无 CGO）。

## Alternatives Considered

- **坚持 RocksDB**：性能天花板高（Kvrocks 路线），但部署/编译成本与当前阶段目标不符；
- **Badger**：纯 Go 但 WiscKey 架构的 SSD 写放大和生产验证不如 Pebble。
