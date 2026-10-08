# Cluster Slot 迁移（resharding）设计

- 日期：2026-09-27
- 基线：静态拓扑（`internal/cluster/topo.go` owned/owner 恒定）+ `intercept` 只会 MOVED/ASK + SETSLOT 系直接报错
- 对标：Redis 7.2.6 真机措辞与流程
- 范围：Redis 一致；MIGRATE 单 key 全参；迁移态落盘恢复；方案 A（原生 MIGRATE）

## §1 架构

- `Topology` 新增 `migrating map[int]string`、`importing map[int]string`（slot→对端 nodeID），+ `epoch` 自增。
- `SETSLOT` 写内存即落盘（nodes.conf 式快照：slots 归属 + 双态 + epoch），启动重放恢复。
- `intercept` 分支：
  - 源端 `MIGRATING`：本地有 key 放行执行，无 key 返回 `ASK slot owner`。
  - 目标端 `IMPORTING`：沿用 `ASKING + keyPresent` 放行，否则 `MOVED` 按现有逻辑。
  - 非迁移槽保持现有 MOVED/CLUSTERDOWN/CROSSSLOT。
- `MIGRATE` 复用 `DUMP / DEL + 对端 RESTORE`，不引入新编码。

## §2 MIGRATE 数据流

- 语法：`MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password | AUTH2 user pass]`，单 key。
- 流程：客户端→源端→源 `DUMP + TTL`→TCP 连目标 `RESTORE`→成功且非 `COPY` 则源 `DEL`→超时按 `timeout` 杀连接。
- 语义：`COPY` 留源；`REPLACE` 覆目标；目标已存在且无 `REPLACE` 报错；源无 key 报 `NOKEY` 系措辞；非同槽（扩展预留）报 `CROSSSLOT`；超时/IO 报 `IOERR` 系措辞，均对齐 7.2.6。

## §3 异常与状态机

- `SETSLOT MIGRATING/IMPORTING` 做前置校验（owner 自检 + 幂等），`STABLE` 清本槽双态，`NODE <id>` 做归属原子切换 + 清双态 + epoch++ 落盘。
- 迁移中崩溃按落盘态续跑；目标失联 `MIGRATE` 不删源；重复 `SETSLOT` 幂等成功。
- 措辞对齐：`CROSSSLOT / CLUSTERDOWN / ASK / MOVED / TRYAGAIN` 与 7.2.6 一致。

## §4 测试与文件

- 改动：`internal/cluster/topo.go`（双态+落盘/重放）、`internal/cluster/slot.go`（复用）、`internal/commands/cluster.go`（拦截+SETSLOT）、新增 `MIGRATE` 命令（含 AUTH 超时）。
- 测试：TDD 单测（状态机+落盘重放+intercept 分支）+ `testdata/cluster/` 真机 7.2.6 fixtures（ASK/MOVED/MIGRATE 全参/COPY/REPLACE）+ 双节点 `redis-cli` 验收 + `-race` 干净。
- 不做：多 key `KEYS` 扩展、在线批量快照自创协议、`MEET/RESET/REPLICATE` 从角色（仍拒绝）。

## 自检

- 无 TBD/TODO；§1–§4 无矛盾（落盘双态 ↔ 重启续跑 ↔ NODE 原子切换一致）；范围单计划可执行，无需再拆。
