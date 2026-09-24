# ADR-0004: 数据结构补齐分两期，Stream 独立成 M2

M1 先交付 ZSet 全量 + Geo + Bitmap + HLL（4 票），Stream 全量（含消费组/PEL/阻塞读）独立为 M2（2 票），M1 合完即可发版、不被 Stream 阻塞。

## Status

Accepted (2026-09-22)

## Considered Options

- **A) 一期全吃（4-6 周、5-6 票）**：范围完整但反馈周期长，Stream 的阻塞读+消费组语义一旦延期会拖死 ZSet 等已就绪部分。
- **B) P0 子集先行**：被 grill 否决——用户明确要全量命令（ZSet 含 LEX/STORE/阻塞，Stream 含消费组），子集会被 redis-cli 随便敲穿。
- **C) 两期（ adopted ）**：M1 四票纯函数居多（`z:`/`s:`/`hll:` 前缀上），M2 两票独占 Stream 新语义（`x:` 前缀 + PEL + BLOCK），天然可切。

## Consequences

- `docs/spec.md` Phase 3 按 M1/M2 重写状态；每票自带 `tests/redis-compat/<type>.sh` 对比脚本并进 `make test` 门禁。
- 存储沿用前缀+自适应编码，不重构 `codec.go`（后续如需重构另立 ADR）。
