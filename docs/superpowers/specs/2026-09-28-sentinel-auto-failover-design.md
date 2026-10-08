# Sentinel 自动故障转移设计（2026-09-28）

## 背景与范围

- 基线：M7 Sentinel 最小发现版已交付——静态 `Registry`（主缓存 + 二元 down 标记）、`ProbeOnce`（Dial+PING，按 downAfter 周期）、手动 `Failover(name)`（首个健康从提升、同 master 串行）、26379 独立口、仅 `+switch-master` 事件。
- 目标：在基线上新增自动故障转移：SDOWN/ODOWN 判定、leader 选举、自动执行、哨兵间 gossip、多哨兵 quorum 协商。
- 非目标：TLS、Cluster 迁移、ACL 边角。

## 已确认约束

1. 拓扑：多哨兵 quorum（生产向，贴近 Redis 官方）。
2. 发现：hello gossip（`__sentinel__:hello` 发布订阅互发现）。
3. 选从：priority → offset → runid 排序优选。
4. 并发：failover-timeout 内同一 master 只允许一次自动转移，超时后可重试。

## 路线

官方对齐完整版（SDOWN → ODOWN(quorum) → epoch 投票选 leader → 自动执行 → timeout 冷却 → hello gossip + 全事件）。

## 一、架构总览

在现有 `Registry` / `ProbeOnce` / 手动 `Failover` 上加四件套：

1. SDOWN 判定器：复用 `ProbeOnce` + down-after 连续失败计数。
2. ODOWN 协商器：含自己 + 直连问询 `IS-MASTER-DOWN-BY-ADDR` 计 quorum。
3. Leader 选举：epoch 自增 + majority 投票。
4. 自动执行器：复用 `sendReplicaof` 链路，选从换成 priority/offset/runid 排序 + failover-timeout 冷却。

哨兵间走 hello gossip 经 `__sentinel__:hello` 互发现；26379 口新增 `IS-MASTER-DOWN-BY-ADDR` / `SENTINELS` 查询扩展。

## 二、组件与状态

- `masterState` 扩展：sdown 计数/阈值、odown 标志、`currentEpoch`、`votedEpoch`、`lastFailover` 时间。
- 新增 `sentinelPeer` 表（地址、自报 epoch、sdown 投票），由 hello 包维持。
- 新增 `slaveInfo` 表（priority、offset、runid），由 `INFO replication` 解析得到，缺失时退化为地址序。
- 并发：`Registry` 读写锁保护状态，`failMu` 仍串行同 master 执行。

## 三、数据流

1. `ProbeOnce` 失败计数达 down-after → 本哨兵标 SDOWN，发 `+sdown`。
2. 向已知 peers 发 `IS-MASTER-DOWN-BY-ADDR`（带 epoch）计票，含自己达 quorum 判 ODOWN，发 `+odown`。
3. epoch+1 发起投票，收 majority 即当选，发 `+elected-leader` / `+try-failover`。
4. 按 priority → offset → runid 选从 → 提升（`REPLICAOF NO ONE`）+ 旧主降级 → 翻 `current`，发 `+switch-master`。

## 四、错误与冷却

- failover-timeout 内同一 master 拒绝第二次自动转移（手动 `FAILOVER` 不受限）。
- 选从为空 / 提升失败则 abort，不翻 `current`，发 `-failover-aborted`。
- 旧主降级失败不回滚（沿用手动语义）。
- 对端不可达计为反对票，不阻塞。
- epoch 只增不减，低 epoch 投票直接拒绝。

## 五、测试验收

- 单测（TDD）：SDOWN 计数、quorum 边界（含自己）、epoch 投票 majority、低 epoch 拒绝、选从排序、timeout 冷却。
- 集成：3 哨兵 + 1 主 2 从真机/仿真，杀主后 30s 内 `+switch-master` 且仅一次，事件序列断言。
- 既有 `testdata/sentinel` fixtures 扩展。

## 自检

- 无 TBD/TODO 占位；各节与已确认约束一致；架构与特性描述一致。
- 单个实施计划可承载（四件套 +  gossip + 命令扩展 + 测试）。
- 歧义已收敛：quorum 含自己、epoch 拒绝规则、降级失败不回滚、手动不受冷却限制。
