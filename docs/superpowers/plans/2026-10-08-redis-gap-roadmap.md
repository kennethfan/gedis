# Redis 缺口补齐路线图（P0 → P1）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按 P0（连接/运维断裂带）→ P1（功能代差）顺序补齐 Gedis 相对 Redis 7.x 的缺口，每阶段独立可交付、可回归。

**Architecture:** 全部走现有命令注册表模式（`r.Register` + `acl.Meta` + `arity.go` + `cluster/keys.go` 分类）；跨切面特性（通知/Tracking）挂已有写路径与 Hub，不另起总线。

**Tech Stack:** Go, Pebble, RESP2/RESP3, 自研 Lua 沙箱（复用于 Functions）

**Spec:** 本路线即 spec；验收以 `redis-cli` + 真机行为 diff 为准（repo CONTEXT 约定）。

## Global Constraints

- 每个新命令必须四件套：`r.Register` + `acl.RegisterMeta` + `arity.go` 条目 + `cluster/keys.go` 分类（`meta_complete_test` / `arity_test` 会强制卡）。
- 写命令必须同步进 `WriteCommandSet()`（`write.go`），否则只读副本会放行写。
- TDD：先 `dispatch(r, ...)` 单测红，再最小实现绿；真机对齐用 `redis-cli -p` 手工 diff。
- 每个阶段结束更新 `CONTEXT.md` + `README.md` 对应行（repo 惯例，见 git log docs 提交）。
- 不提交、不推送（用户确认前）。

## Review Focus

- `SELECT` 非 0 库的语义：拒绝还是兼容？合理人期望客户端不断连（见 Phase 1 决策）。
- `FLUSHDB`/`FLUSHALL` 在有副本拓扑时的复制语义：是否进 Hub？默认应进，否则主从分叉。
- Keyspace 通知的发布点遗漏：任何绕过标准写路径的命令都会静默丢通知。
- `CLIENT TRACKING` 的 RESP3 push 与当前 server 推送能力是否匹配（无 push 则只能做 RESP2 兼容子集）。
- RDB 互操作的字节级兼容：自研编码器极易在 `RESTORE` 真机 dump 上翻车，需以真机 dump 为 fixtures。

---

### Phase 0: 文档过期修复（热身，S）

**Files:**
- Modify: `CONTEXT.md:9`（"未实现：TLS" → "服务端 TLS 未实现，dial 端已交付"）
- Modify: `README.md:120-122`（Scope 行同步）

**Interfaces:** 无。

- [ ] **Step 1:** 改两行措辞，提交 `docs: 同步TLS现状措辞`

---

### Phase 1: 连接层补齐（P0，M）—— 解锁所有客户端库

**Goal:** `redis-cli` 默认连接流程不断裂；各语言客户端能完成握手。

**Files:**
- Create: `internal/commands/conn.go`（HELLO/SELECT/QUIT/ECHO/RESET/COMMAND 分发）
- Modify: `internal/network/router.go:102-104`（HELLO/QUIT 免鉴保留，SELECT 纳入鉴权豁免评估）
- Modify: `internal/commands/arity.go`（+6 条目）、`internal/cluster/keys.go`（归入无 key/连接类）
- Test: `internal/commands/conn_test.go`

**Interfaces:**
- Consumes: `protocol.ParseHello`（已存在）、`network.Router.Register`
- Produces: `RegisterConn(r)`（main 装配，与其他 Register* 并列）

**Decisions（实现前定）：**
- `SELECT`: 单库引擎。采用兼容策略——`SELECT 0` 回 OK，其余回 `ERR DB index is out of range`（对齐大多数单库 clone，不断连）。
- `HELLO`: 支持 `HELLO 2/3 [AUTH user pass] [SETNAME name]`；RESP3 模式标记进 conn 上下文（Phase 8 依赖此标记）。
- `COMMAND`: 先实现 `COUNT` + `INFO`（返回已注册命令表，由 `Router.Commands()` 生成）+ `GETKEYS`（复用 `extractKeys`）；`DOCS` 放 Phase 2（需要逐命令文档元数据）。

- [ ] **Step 1:** `conn_test.go` 写红（HELLO 2/3、SELECT 0/1、ECHO、COMMAND COUNT、QUIT/RESET 生存性）
- [ ] **Step 2:** 跑测试确认红
- [ ] **Step 3:** `conn.go` 最小实现；COMMAND INFO 从 `Router.Commands()` + `acl` 元数据拼
- [ ] **Step 4:** 全包绿 + `redis-cli` 手工握手 diff
- [ ] **Step 5:** 四件套检查（meta/arity/keys/write-set 不需要——全是只读/连接类）+ 更新 CONTEXT/READM
- [ ] **Step 6:** 提交 `feat(conn): HELLO/SELECT/QUIT/ECHO/RESET/COMMAND基础`

### Phase 2: 可观测与运维命令（P0，M）

**Goal:** 监控与运维面无断裂；`OBJECT`/`CONFIG` 补到够用。

**Files:**
- Create: `internal/commands/server.go`（DBSIZE/FLUSHDB/FLUSHALL/LASTSAVE/ROLE/SHUTDOWN 行为）
- Modify: `internal/commands/monitor.go`（CONFIG GET/SET 参数扩展 + RESETSTAT；INFO 补段；OBJECT IDLETIME/REFCOUNT/FREQ）
- Modify: `internal/commands/expire.go:213-252`（OBJECT 子命令扩展）
- Test: `internal/commands/server_test.go`

**Interfaces:**
- Consumes: `allUserKeys`（keyspace.go 已有）、`network.StatsView`
- Produces: `RegisterServer(r)`

**Decisions:**
- `FLUSHDB`/`FLUSHALL` 必须走 Hub 进复制流（防主从分叉）；实现为特殊 marker，经 `WriteCommandSet` 登记。
- `BGSAVE`/`BGREWRITEAOF`：本引擎无 RDB/AOF——回 `+OK` 空实现还是报错？选报错 `ERR not supported`（诚实失败优于静默谎言），并在 README 写明。真 RDB 互操作整体推迟到 Phase 10。
- `SHUTDOWN`：优雅关机（停 Expirer、刷 Pebble、关 listener）；`NOSAVE/SAVE` 参数接受但 SAVE 为 no-op（Pebble 常驻持久）。
- `CONFIG`：补 `RESETSTAT`、`GET notify-keyspace-events`（Phase 7 的读口，先占位返回空串）、`maxmemory-policy` 扩展到 Phase 9 的新策略名（先接受、后生效）。
- `COMMAND DOCS`：从各 `acl.Meta` + 手写 usage 表生成； asks `COMMAND INFO` 复用 Phase 1 的表。
- `MEMORY USAGE/DOCTOR`：Pebble 无精确单 key 内存——`MEMORY USAGE` 返回估算（payload 长度 + 常数开销）并文档注明；`MEMORY STATS/DOCTOR/MALLOC-STATS` 回 `ERR not supported`。
- `ROLE`：主返回 `master` + offset（backlog offset 已有？无则回 0 并注明）；副本返回 `slave` + 主地址。

- [ ] **Step 1-6:** TDD 同 Phase 1 节奏；验收 `INFO` 全段 + `redis-cli` 对照；提交拆两笔（server 新文件一笔、monitor 扩展一笔）

### Phase 3: 复制协议面（P0，M）

**Goal:** 真 `redis-cli` 做副本、外部副本能挂上来；`WAIT` 语义可用。

**Files:**
- Modify: `internal/commands/replication.go`（REPLCONF 子命令：LISTENING-PORT/CAPA/ACK；PSYNC 握手补齐）
- Create/Modify: `WAIT` 实现（读 backlog ack 水位；超时回实际数）
- Test: `internal/commands/replication_test.go` 扩展 + `repl_live_test.go` 加 WAIT 用例

**Interfaces:**
- Consumes: backlog offset/ack 跟踪（replication 包内已有 Hub；缺 ack 回传则补）
- Produces: 副本 ACK 上报链路（Phase 7/8 不依赖，但测试可复用 live 拓扑）

**Decisions:**
- `SYNC`（legacy）：明确不支持，回错并文档化（真机已废弃，无需兼容）。
- `REPLICAOF` 已有；补 `SLAVEOF` 别名（一行）。
- `FAILOVER`（复制级，`CLUSTER FAILOVER` stub 已有）：本阶段只做"无副本时明确报错"，真切换逻辑归集群 Phase（Phase 9 之后视需求）。

### Phase 4: 散装命令补齐（P0→P1 过渡，M，可拆两笔提交）

**Goal:** 数据命令面与 7.x 拉平，无单点缺口。

| 命令 | 落点文件 | 备注 |
|---|---|---|
| `GEOHASH` | `geo.go` | 纯读，geohash 编码复用现有 |
| `HRANDFIELD`（含 COUNT/ WITHVALUES） | `hash_multi.go` | 随机采样，注意负 COUNT 语义（可重复） |
| `HSTRLEN` | `hash.go` | |
| `SINTERCARD`（LIMIT） | `set_ops.go` | 不存只算，上限提前终止 |
| `ZINTERCARD`（LIMIT） | `zset_store.go` | 同上 |
| `LMPOP` | `list_block.go` | 复用 BLMPOP 非阻塞路径 |
| `ZMPOP` | `zset_block.go` | 复用 BZMPOP 非阻塞路径 |
| `XSETID` | `stream.go` | 改 last-id；登记 WriteCommandSet |
| `SORT_RO` | `generic.go` | SORT 只读别名（只读副本可放行） |
| `BITFIELD_RO` | `bitfield.go` | 只读变体 |
| `LCS`（LEN/IDX/MINMATCHLEN） | 新建 `lcs.go` | 唯一算法活（DP）；大串截断保护 |
| `EXPIRETIME`/`PEXPIRETIME` | `expire.go` | 读 expiry 字段 |
| `MOVE`/`RANDOMKEY` | `keyspace.go` | 单库下 MOVE 恒失败回 0（语义诚实化+文档） |

附带：`SENTINEL GET-MASTER-ADDR-BY-NAME`（客户端故障转移第一入口，P0 级单点）、`MONITOR`/`MYID`/`REPLICAS`/`CKQUORUM`（sentinel.go 补 case）。

### Phase 5: Pub/Sub 补齐（P1，S-M）

- `PUBSUB CHANNELS/NUMSUB/NUMPAT`：读现有订阅表（pubsub.go 内已有结构）。
- 分片消息：`SSUBSCRIBE`/`SUNSUBSCRIBE`/`SPUBLISH` + `PUBSUB SHARDCHANNELS/SHARDNUMSUB`。注意 cluster 槽校验（分片频道要求同槽多 key 语义；静态拓扑下先做单节点语义+文档注明集群限制）。

### Phase 6: Functions（P1，L）—— 唯一大件功能

**Architecture:** 复用 Lua 沙箱与 `lua.go` 的禁用表/超时/只读化；`FUNCTION LOAD/DUMP/RESTORE/FLUSH/LIST/STATS` + `FCALL/FCALL_RO` 走同一执行器，仅入口与持久化（函数库存 Pebble 独立前缀 `fn:`）不同。

**Files:** 新建 `internal/commands/function.go` + 前缀 `fn:`（datastruct 或直写 KV，参考 `hll:` 前缀惯例）；ACL 类别 `scripting` 复用。

先做：LOAD/FLUSH/LIST/FCALL/FCALL_RO/STATS；DUMP/RESTORE（序列化格式自定+文档）可拆第二笔。

**二批 ✅ 已交付 2026-10-10**：FUNCTION KILL（复用 fnReg kill 三态）+ STATS running_script（在飞追踪）+ DUMP（自定帧 magic `GDISFN01`，库名排序）+ RESTORE（两阶段 FLUSH/APPEND/REPLACE），真机 7.2.6 探针逐字对齐；跨引擎 payload 不互通（文档 + CONTEXT 偏差记录注明）。

### Phase 7: Keyspace 通知（P1，M，跨切面）—— ✅ 已交付 2026-10-10（commit 5a783c5..f45c2dc）

- `CONFIG SET notify-keyspace-events <string>` 生效（Phase 2 已占位）。**已交付**：TEA 支持，KEA 全量生效。
- 发布点：在 Hub/写路径统一出口挂钩（先审计全部写命令的落盘点，列清单再动手——漏一个就是静默丢事件）。**已交付**：generic/string/hash/list/set/zset/stream/expired/evicted 全类发布点。
- `__keyspace@0__` / `__keyevent@0__` 频道复用现有 PubSub 通道。**已交付**：K/E 双频道，三重门。
- 遗留：d/m/n/o/c 配置字母接受但无对应事件；MOVE 事件单库不可达；ZPOPMIN/ZPOPMAX/BZPOP\* 等事件表外命令不发事件（见 CONTEXT 偏差记录）。

### Phase 8: CLIENT TRACKING（P1，M；依赖 Phase 1 的 RESP3 标记）—— ✅ 已交付 2026-10-10（batch2 commits cba654a/cc51c85/70b59ef）

- `CLIENT TRACKING ON/OFF [BCAST] [PREFIX] [OPTIN/OPTOUT] [NOLOOP]`；失效消息走 RESP3 push（RESP3 连接）/ RESP2 兼容降级（`__redis__:invalidate` 通道）。**已交付**。
- 先做 NOLOOP 默认语义 + BCAST；OPTIN/OPTOUT 的 `CLIENT CACHING yes/no` 配合。**已交付（BCAST/PREFIX 可重复/NOLOOP/CACHING 全量，off/断连/写错三路清理收敛，见 CONTEXT 偏差记录）**。
- 依赖 Phase 7 的失效事件源（key 修改事件复用通知管线，只发给 tracking 表）。**实现调整：失效不复用 notify 通路，走独立存储 change hook（Set/Delete/WriteBatch + 逐出 evictHook）→ 跟踪表反查（S7 硬约束：不受 notify-keyspace-events 门控）**。

### Phase 9: 逐出策略补齐（P1，S-M；LFU 依赖 Phase 2 的 OBJECT FREQ 字段位）—— ✅ 已交付 2026-10-10（commit 365c950）

- 易：`noeviction`、`allkeys-random`、`volatile-random`、`volatile-ttl`（CONFIG 接入 + sampling 复用 LRU 路径）。**已交付**。
- 中：LFU（`allkeys-lfu`/`volatile-lfu`）：每个 key 存 24bit 计数器（参考 Redis `object.freq`），`OBJECT FREQ`/`SET`/`GET` 更新衰减；存储前缀内加字段（注意 Pebble 编码版本兼容——走 datastruct 版本位或独立 `freq:` 前缀）。**已交付（简化 LFU：+1 饱和 255，无概率增量/时间衰减，见 CONTEXT 偏差记录）**。
- 遗留：LFU 未实现概率增量（counter 抽样）与时间衰减；OBJECT IDLETIME/FREQ 读取会刷新统计。

### Phase 10: RDB/AOF 互操作（P1→P2，XL；可长期挂起）

- 最小可用：RDB 只读加载（启动 `--load-rdb` 把真机 RDB 转入 Pebble；自研解析器，以真机 dump 为 fixtures 回归）。
- 完整 AOF/RDB 双写是存储引擎级改造，不在 P1 承诺内；本路线交付到"可迁入"，不承诺"可迁出"。

---

## 阶段依赖图

```
Phase 0 ──→ Phase 1 ──→ Phase 2 ──→ Phase 3
  (docs)    (conn)      (server)     (repl)
               │            │
               ▼            ▼
            Phase 8      Phase 9(LFU)
         (tracking)      Phase 7 ──→ Phase 8
              需要 Phase 7 事件源     (tracking)
Phase 4/5/6 独立（任意顺序），建议 4 → 5 → 6（由小到大）。
Phase 10 独立，可挂起。
```

## 工作量总览

| Phase | 规模 | 价值 |
|---|---|---|
| 0 文档 | S | 消除误导 |
| 1 连接层 | M | 客户端兼容分水岭，**先做** |
| 2 运维 | M | 可观测性 |
| 3 复制协议 | M | 外部副本可挂载 |
| 4 散装命令+SENTINEL单点 | M | 命令面拉平 |
| 5 PubSub | S-M | |
| 6 Functions | L | 唯一大件 |
| 7 通知 | M | 跨切面，审计先行 |
| 8 Tracking | M | 需 1+7 |
| 9 逐出 | S-M | |
| 10 RDB | XL | 可挂起，先只读加载 |
