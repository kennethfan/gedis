# Gedis Domain Model

## Glossary

| Term | Definition |
|------|------------|
| **Gedis** | 基于 Pebble 的 Redis 兼容存储引擎，用 Go 实现 |
| **Redis 兼容（目标）** | 完全兼容 Redis 协议和命令，可作为 drop-in 替换（验收以 `redis-cli` + 官方命令行为为准） |
| **已兼容子集（现状）** | v1 已实现：String, Hash, List, Set（含 TTL/HEXPIRE、PSYNC 主从、LRU、INFO/SLOWLOG/Prometheus）；M1 已交付：ZSet 全量（含 LEX/STORE/阻塞）+ Geo 套壳 + Bitmap 全量 + HLL 全量（dense-only）+ SCAN/TYPE 补齐；M2 已交付：Stream 全量（含消费组/PEL/阻塞读）；M4 已交付：事务（MULTI/EXEC/WATCH）；M5 已交付：Pub/Sub（含 pattern 订阅）；M6 已交付：Lua（EVAL/EVALSHA/SCRIPT + cjson 全量 + bit/cmsgpack/struct 三库 + SCRIPT KILL + 超时可配 + 脚本内禁用命令 + 编译措辞对齐 + 沙箱只读化）；M7 已交付：Cluster 最小行为集（静态拓扑 + MOVED/ASK + 只读四件套）+ Cluster slot 迁移（migrating/importing 双态 + SETSLOT 状态机 + MIGRATE 单key/COPY/REPLACE/AUTH + ASK/MOVED 按key存在拦截）+ Sentinel 最小发现版（独立 26379 端口 + 哨兵命令子集 + 手动 FAILOVER）+ Sentinel 自动故障转移（sdown 计数→odown quorum→epoch 投票多数派→hello gossip 编解码 + peer 表→按 priority-offset-runid 选从→auto executor 冷却 + 事件）；M8 已交付：ACL（AUTH 双形态 + 用户管理命令 + key-spec 全命令登记 + key/channel/selectors 三阶鉴权 + DRYRUN/LOG + aclfile/requirepass + Lua 内检查，真机 7.2.6 对齐）；Redis 差距路线图已交付：Phase 1 连接层（HELLO/SELECT/QUIT/ECHO/RESET/COMMAND）+ Phase 2 运维（DBSIZE/FLUSHDB/CLIENT/TIME/ROLE/MEMORY/LATENCY/DEBUG/SHUTDOWN）+ Phase 3 复制协议面（REPLCONF/WAIT/SLAVEOF/SYNC/FAILOVER，措辞与真机 7.2.6 对齐，副本可挂真机全量同步）+ Phase 4 散装命令（GEOHASH/HRANDFIELD/HSTRLEN/SINTERCARD/ZINTERCARD/LMPOP/ZMPOP/XSETID/SORT_RO/BITFIELD_RO/LCS/EXPIRETIME/PEXPIRETIME/MOVE/RANDOMKEY + SENTINEL MYID/REPLICAS/CKQUORUM/MONITOR，真机 7.2.6 对齐；单库 MOVE 异库恒 0、XSETID ENTRIES-ADDED 按文档实现与 7.2.6 有差，见 ledger）；2026-10-10 批次已交付：Phase 7 Keyspace 通知（notify-keyspace-events KEA 全量发布点）+ Phase 9 逐出策略 8 种 + OBJECT IDLETIME/FREQ 真值 + 新命令 GETSET/SETEX/PSETEX/SETNX/TOUCH/LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH/READONLY/READWRITE + 服务端 TLS 监听（git 87bda4f，见下批次段）；2026-10-10 批次2已交付：Phase 8 CLIENT TRACKING（on/off + BCAST/PREFIX/OPTIN/CACHING/OPTOUT/NOLOOP，RESP3 push / RESP2 `__redis__:invalidate` 失效，独立于 notify-keyspace-events）+ MONITOR 实时命令流 + 新命令 HEXPIRETIME/HPEXPIRETIME/SUBSTR/SWAPDB/COMMAND LIST + CONFIG REWRITE（诚实拒绝）+ 官方事件表核对修正（HDEL 空 hash del / TOUCH arity），见 batch2 段|
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

Gedis 使用 Pebble 作为底层存储引擎，通过前缀方案在同一个 Pebble 实例中存储所有 Redis 数据结构：

- `s:` 前缀 - String 类型
- `h:` 前缀 - Hash 类型
- `l:` 前缀 - List 类型
- `st:` 前缀 - Set 类型
- `z:` 前缀 - Sorted Set 类型（Geo 复用，score 存 geohash）
- `x:` 前缀 - Stream 类型（含 PEL/消费组状态）
- `hll:` 前缀 - HyperLogLog 类型（dense-only）
- Bitmap 复用 `s:` 前缀（String 原生字节语义）

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

## 2026-10-10 批次交付（Redis 缺口 batch1）

计划：`docs/superpowers/plans/2026-10-10-redis-gap-batch1.md`（T1-T9，commits 365c950..T9）。ledger：`.superpowers/sdd/2026-10-10-redis-gap-batch1/progress.md`。

### 交付声明

- **Phase 7 Keyspace 通知 完成**：`Notify` 三重门（pub/mask/class）、K/E 双频道、db0；generic/string/hash/list/set/zset/stream/expired/evicted 全类发布点；`CONFIG SET notify-keyspace-events` 生效。
- **Phase 9 逐出策略 完成**：8 种策略（noeviction/allkeys/volatile × lru/lfu/random/ttl）+ `OBJECT IDLETIME/FREQ` 真值。
- **新命令**：GETSET/SETEX/PSETEX/SETNX/TOUCH；LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH；READONLY/READWRITE（注册级）。
- **WriteCommandSet 审计补漏 28 条**（含 ZADD 整族、EVAL/EVALSHA、MIGRATE/RESTORE、HEXPIRE 族、XREADGROUP/XACK/XCLAIM/XAUTOCLAIM）。
- **SAVE** 注册为诚实失败（与 BGSAVE 同：`ERR not supported on this engine: no RDB/AOF persistence`）。
- **未做（batch1 时点，见下文 batch2 段接续）**：Phase 8（CLIENT TRACKING，batch2 已交付）与 Phase 10（RDB/AOF，仍未做）；缺口命令 HEXPIRETIME/HPEXPIRETIME、MONITOR、SUBSTR、SWAPDB、COMMAND LIST、CONFIG REWRITE 等均已在 batch2 交付。

### 偏差记录（T1-T8 report 汇总）

1. 服务端 TLS **已实现**（cmd/gedis/main.go 监听口 TLS 包装 + 自签测试，git 87bda4f）——本文档与 README 旧"未实现"措辞已修正。
2. LFU 为简化实现：访问 +1、饱和 255；无 Redis 概率增量与时间衰减。OBJECT FREQ 与 LFU 计数同源。
3. OBJECT IDLETIME/FREQ：OBJECT 读取自身会刷新统计（非 LOOKUP_NOTOUCH）；未被 track 的 key（重启后仅被读过）回 0。
4. READONLY/READWRITE 为注册级支持（+OK + per-conn 标志位），集群副本读路由未接线。
5. 事件表外命令不发事件：ZPOPMIN/ZPOPMAX/BZPOP\*、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE、GETEX、TOUCH；notify 配置字母 d/m/n/o/c 接受但无对应事件；MOVE 事件在单库实现下不可达（无成功移库分支）；ZADD INCR 早退路径不发事件；LPUSHX/RPUSHX 未实现故无事件。
6. 结构语义差：HDEL 不删空 hash（gedis 保留空结构，无 del 补发）；XDEL/XGROUP DESTROY 删空后保留 key 无 del；XADD 无 MAXLEN 选项解析（仅发单条 xadd）。
7. `CONFIG SET notify-keyspace-events` 非法字母文案未与真机核对。
8. WriteCommandSet 判定口径：MULTI/EXEC/DISCARD/WATCH/UNWATCH（事务控制）、SCRIPT/FUNCTION、CLUSTER/CONFIG/DEBUG 等管理类不入集合；TOUCH/READONLY/READWRITE 为反向钉（只读）。
9. 测试基建注意：事件测试须显式 Register 被测命令（openMonitorSetup 不含 conn/list/generic）；mask 测试须含类位（KEA/KEg），纯通道位零投递。

## 2026-10-10 批次交付（Redis 缺口 batch2）

计划：`docs/superpowers/plans/2026-10-10-redis-gap-batch2.md`（T1-T7）。ledger：`.superpowers/sdd/2026-10-10-redis-gap-batch2/progress.md`。

### 交付声明

- **Phase 8 CLIENT TRACKING 完成**：`CLIENT TRACKING on/off` + BCAST（PREFIX 可重复，无 PREFIX 通配）/ OPTIN+`CLIENT CACHING yes|no` / OPTOUT / NOLOOP；读注册表（TrackTable 正反向）+ BCAST 前缀订阅表；失效推送 = 存储层 change hook（Set/Delete/WriteBatch，op 's'/'d'，ctx 透传）+ 逐出 evictHook（op 'e'）→ 跟踪表反查 → RESP3 `invalidate` KindPush 直推（per-conn ConnPipe 复用 MONITOR 管道）/ RESP2 发布 `__redis__:invalidate`；NOLOOP 跳写者自己；独立于 notify-keyspace-events（S7 硬约束）；清理三路收敛 `untrack`（off / 断连 ConnClosed / 推送管道写错）。
- **新命令**：HEXPIRETIME/HPEXPIRETIME、SUBSTR、SWAPDB、COMMAND LIST（COUNT 归一）、CONFIG REWRITE（诚实拒绝 `ERR not supported on this engine: config rewrite not implemented`）。
- **MONITOR**：实时命令流（顶层 `MONITOR` + `CLIENT MONITOR` 双入口），per-conn 串行出站管道 + per-conn 写锁（`network.LockedWrite`，serve 循环单写点加锁）。
- **官方事件表核对**（webfetch redis.io keyspace-notifications 全量 diff）：6 候选组（ZPOPMIN/BZPOP\*、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE、GETEX、TOUCH）确认**表外不发**（现状已正确，pin 测试钉零事件）；修复 2 真缺口：HDEL 删至空 hash 补 del 事件（key 移除）、TOUCH 单 key arity 误报（`<2`→`<1`）。

### 偏差记录（T1-T6 report 汇总）

1. HEXPIRETIME/HPEXPIRETIME arity 取 -3（镜像既有 HTTL:-3 先例 + legacy 无 FIELDS 形态；真机为 -4，未逐字核对）；`COMMAND LIST` 带参错误文案、SWAPDB 非整数文案沿用先例未与真机逐字核对。
2. `CONFIG REWRITE` 为计划既定诚实拒绝；CONFIG handler 实际位于 monitor.go（计划写 server.go 是定位偏差）。顺带修 keys.go singleFirstCmd 登记缺口（HEXPIRE 族/HTTL/HPTTL/HPERSIST）。
3. MONITOR：addr 取不到记 `local`（无 lua 场景判定）；admin 跳过名单为显式集合，与真机 admin 全集可能有差；观测点在 handler 前，语法错误命令也会进流。
4. CLIENT TRACKING：读 hook 成功口径 = 回复非 KindError；hook 在 stats 块后、return 前（slowlog 计时不含 hook）；无连接文案仿 SETNAME 模式（非真机官方文案）；BCAST 连接不做读注册（防双路径重复推送）。
5. 失效推送：RESP2 通道广播无法对已订阅的 NOLOOP 写者单独过滤（通道级语义，真机行为待核对）；WriteBatch（MSET/Z\*STORE）亦接 change hook（brief 未列，行为超集）；逐出唯一走 evictHook op 'e'（evictOne 不经 Delete 的 op 'd'，避免双重推送）；ConnByID O(n) 反查；内部键（PEL 等）userKeyFromRaw 归一未专项断言。
6. 生命周期：复用既有 ConnRegistry.ConnClosed 作断连清理（未新增 Remove）；推送管道写错 → dropPipe + untrack。CachingYes 在每次 TRACKING 调用与 off 时复位、不跨 conn。

## References

- [Redis Protocol Specification](https://redis.io/docs/reference/protocol-spec/)
- [Pebble](https://github.com/cockroachdb/pebble)
- [KeyDB](https://keydb.dev/) - 参考项目
- [Dragonfly](https://www.dragonflydb.io/) - 参考项目
