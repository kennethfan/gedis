# Redis 缺口补齐 batch2 —— 缺口命令 / MONITOR / 事件遗留 / CLIENT TRACKING

> For agentic workers: REQUIRED SUB-SKILL: subagent-driven-development. Read the whole plan first; execute tasks in order; work tasks sequentially (none are independent enough to parallelize; T4→T5→T6 are strictly ordered). Maintain checkbox tracking in your session.

## Goal

在 batch1（Phase 7 逐出/事件基础设施 + 命令批，已交付）基础上，补齐 README Scope 剩余缺口命令、MONITOR、Phase 7 事件表外遗留命令的事件发布，交付 Phase 8 CLIENT TRACKING。Phase 10 RDB 明确不在本批（batch3）。

## Architecture

- **命令面**：全部落 `internal/commands/`，遵守命令四件套（r.Register + acl.Meta + arity + cluster/keys.go key-spec 登记；meta_complete_test/arity_test 强制）；写命令进 `WriteCommandSet()`。
- **MONITOR**：`Router.Dispatch` 既有观测点（name 解析、auth 通过、handler 执行前）旁挂广播 hook；订阅态注册在命令包内（per-conn 出站管道，串行化写）。
- **CLIENT TRACKING**：状态挂 `ConnRegistry.ConnState`（conn 生命周期绑定）；跟踪表（userKey → conn 集合）在命令包内维护；读路径注册经 Router 可选 read-hook（复用 `extractKeys`）；失效源 = 存储层 `Pebble.Set/Delete` + 既有 `SetEvictHook` 三 hook（**不受 notify-keyspace-events 门控**），ctx 随 storage.Set/Delete 透传以支持 NOLOOP 排除写者。
- **推送**：RESP3（`ConnRegistry.ProtoOf==3`）走 `protocol.KindPush`（encode.go:48 已有 case）`["invalidate", [key]]`；RESP2 降级发布 pubsub 通道 `__redis__:invalidate`（M5 pubsub 既有）。

## Tech Stack

Go 1.x + pebble；测试 `testing` + testify require；无新增依赖。

## Spec

| # | 交付 | 验收 |
|---|------|------|
| S1 | HEXPIRETIME/HPEXPIRETIME、SUBSTR、SWAPDB、COMMAND LIST、CONFIG REWRITE | 真机语义可核处对齐；不可核处 report 记偏差 |
| S2 | MONITOR：`+OK` 后实时流 `<ts> [db addr] "cmd" "args"` | 双连接订阅、退出清理、AUTH 红action、admin 不记录 |
| S3 | 事件遗留：以 Redis 官方事件表为唯一权威补发 ZPOPMIN/LMPOP/ZREMRANGEBYLEX/GEOSEARCHSTORE/GETEX 等 | 表内条目发事件；表内无条目不发（记录） |
| S4 | CLIENT TRACKING on/off + 全 flag 解析 + 跟踪表 + 读注册 | RED→GREEN |
| S5 | 失效推送：存储 hook → 跟踪表 → RESP3 push / RESP2 invalidate 通道；NOLOOP；evict 失效 | 不受 notify 配置门控 |
| S6 | BCAST+PREFIX 匹配、OPTIN/CACHING、TRACKING off/断连清理 | 边界测试全绿 |
| S7 | README/README.zh-CN/CONTEXT/roadmap 文档汇总 | 一致性 grep 零过期措辞 |

## Global Constraints

1. 每任务先写 RED 测试再实现（TDD 红→绿）；任务结束 `go build ./... && go test ./... -count=1` 全绿。
2. 命令四件套 + 写命令进 WriteCommandSet（本批仅 SWAPDB 判定为写，见 T1 决策）。
3. 每任务开始前向子代理派发一次（`task`，预期 Insufficient account funds 失败）后转内联；会话续用：ses_edb417c6dffe57OZo3BvdrhCf7 / ses_edb329ebcffexuJfMTVms3evPO / ses_edb26ea24ffeFtl0xYIC05KYzA。
4. 每任务完成后 commit 到 dev（沿用 batch1 已获批准的自动 commit 惯例）；**不 push**；不动 git config。
5. 注释纪律：新注释须按类别答辩（既有/BDD given-when-then/必要——复杂算法·安全·并发·格式协议）。
6. 不新增第三方依赖；`.superpowers/` 归档保持 gitignore。
7. **tracking 失效管道独立于 notify-keyspace-events 配置**（S5 硬约束）。
8. 不触 Phase 10（RDB）任何文件。

## Review Focus

- **R1 并发写**：MONITOR/tracking 推送与连接正常 reply 写并发——T2 Step 1 必须先查 server 连接写路径是否已有 per-conn 写锁；无锁则先补（单一写点加锁），再接推送，否则 net.Conn 写竞争。写锁方案进 report。
- **R2 proto 判定**：`ConnRegistry.ProtoOf(conn)`（conn.go:109，默认 2，hello→SetProto:95）；仅 Proto==3 发 KindPush，RESP2 发 pubsub。
- **R3 失效覆盖面**：`Pebble.Set`（pebble.go:128）/`Delete`（:146）/`SetEvictHook`（memory.go:81）三处 hook；hook 签名带 ctx（storage.Set/Delete 首参即 ctx）→ NOLOOP 靠 `ConnFromContext(ctx)`。
- **R4 真机不可核处**（SWAPDB 单库 0 0、COMMAND LIST FILTERBY 错误措辞、OPTIN/OPTOUT 冲突措辞、CONFIG REWRITE）：选定合理文案，report 记"未逐字核对"。
- **R5 MONITOR 线格式**：RESP 协议文档明言 MONITOR push 协议"未指定"（redis.io protocol-spec）→ 本实现按 `+<line>\r\n` 简单字符串行；行内格式 `<unix-ts.%06d> [<db> <RemoteAddr>] "cmd" "arg"...`（redis.io/docs/latest/commands/monitor/ 示例格式）；admin 不记录 + AUTH 红action 按显式名单（report 记与真机 admin 全集的差异）。
- **R6 事件权威**：S3 只认官方事件表 https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/（实现时 webfetch 拉取核对）；表内无条目 → 不发事件、report 记录，不臆造。

---

### Task 1: 缺口命令批 A —— HEXPIRETIME/HPEXPIRETIME + SUBSTR + SWAPDB + COMMAND LIST + CONFIG REWRITE

**Files:**
- Modify: `internal/commands/hash_expire.go`（meta/注册/handler，镜像 HTTL/HPTTL:20-21/:41-42/:227 模式）
- Modify: `internal/commands/string_extra.go`（SUBSTR meta:17 旁 + 注册:27 旁）
- Modify: `internal/commands/conn.go`（swapdb handler 依 select_:268 先例；command():342 补 LIST 子命令）
- Modify: `internal/commands/server.go`（CONFIG handler 补 REWRITE case，定位实现时查）
- Modify: `internal/commands/arity.go`（HEXPIRETIME/HPEXPIRETIME:4、SUBSTR:4、SWAPDB:3）
- Modify: `internal/commands/write.go`（仅 +SWAPDB）
- Modify: `internal/cluster/keys.go`（key-spec 登记；SWAPDB 无 key → 照 DBSIZE 风格 First:-1）
- Test: `internal/commands/substr_swapdb_test.go`（新建）、`hash_expire_time_test.go`（新建）、conn/server 既有测试文件补 case

**Interfaces:**
- Consumes: `hfieldttl`（hash_expire.go:227，理解 -2/-1 语义）、`s.getrange`（string_extra.go:27）、`select_`（conn.go:268 单库先例）、`command()`（conn.go:342）、SAVE 的 honest-error 文案风格（server.go:407）
- Produces: `HEXPIRETIME/HPEXPIRETIME` → integer：-2（无 field/key）、-1（无过期）、否则 **Unix 秒/毫秒绝对时间戳**；`SUBSTR` 行为 = GETRANGE 完全一致；`SWAPDB 0 0` → `+OK`（单库无操作），任一索引非 0 → `ERR DB index is out of range`（与 SELECT 同文案）；`COMMAND LIST` → bulk-string 数组（全部注册命令名，大写）；`CONFIG REWRITE` → `ERR not supported on this engine: config rewrite not implemented`

**决策（进 report）**：
1. SWAPDB 进 WriteCommandSet（真机 SWAPDB 为写命令，副本拒写语义对齐；单库下恒 no-op 但名单口径按命令本性）。
2. `COMMAND LIST` 带 `FILTERBY` 等参数 → `ERR unknown argument '%s' for 'command|list' command`（措辞未逐字核对真机）。
3. HEXPIRETIME/HPEXPIRETIME/SUBSTR/COMMAND LIST 为只读，不入 WriteCommandSet。
4. SUBSTR 与 GETRANGE 共用 handler（别名注册），arity 4 同 GETRANGE。

**行为规格：**

| 输入 | 输出 |
|---|---|
| HEXPIRETIME h nofield | : -2 |
| HEXPIRETIME h f（无过期） | : -1 |
| HSET h f v; HEXPIRE h f 100; HEXPIRETIME h f | : ∈ [now+99, now+100] 秒 |
| HPEXPIRETIME（有毫秒过期） | 毫秒时间戳 |
| SUBSTR k 0 -1 | 与 GETRANGE k 0 -1 逐字节相同（含负索引边界） |
| SWAPDB 0 0 | +OK |
| SWAPDB 0 1 / 1 0 | -ERR DB index is out of range |
| COMMAND LIST | 数组含 "SET"/"HEXPIRETIME"，元素为大写命令名 |
| COMMAND LIST FILTERBY PATTERN x* | -ERR unknown argument ... |
| CONFIG REWRITE | -ERR not supported on this engine: config rewrite not implemented |

**Steps:**

1. **RED**：新建 `substr_swapdb_test.go`（BDD 注释）：`Test_Substr_BehavesLikeGetrange`（正/负/越界三组对照）、`Test_Swapdb_SingleDbNoop`（0 0 OK + 越界错）、`Test_CommandList_ListsRegistered`、`Test_ConfigRewrite_HonestError`；`hash_expire_time_test.go`：`Test_Hexpiretime_ReturnsUnixTimestamp`（-2/-1/绝对戳三分支，HEXPIRETIME+HPEXPIRETIME 各一遍）。
   ```bash
   go test ./internal/commands/ -run 'Test_Substr|Test_Swapdb|Test_CommandList|Test_ConfigRewrite|Test_Hexpiretime' -count=1
   ```
   Expected: FAIL（unknown command / 断言失败）
2. 实现四件套：meta（SUBSTR ReadOnly+string、HEXPIRETIME/HPEXPIRETIME ReadOnly+hash、SWAPDB server 类 First:-1、REWRITE 走 CONFIG 子命令不新增 meta）；arity 三/四件；cluster/keys.go 登记（跑 `meta_complete_test`/`arity_test` 验证）。
3. handler：`hfieldttl` 模式改造出 `hfieldExpireAt`（毫秒/秒参数化）或复制实现；SWAPDB 依 `select_`；COMMAND LIST 从 router 已注册集合取名（查 ConnRegistry/router 是否有命令名枚举 API——`r.Handlers()` 类，无则在 network 包补只读枚举）；CONFIG REWRITE case。
4. `write.go` +SWAPDB；跑 WriteCommandSet 既有测试。
   ```bash
   go test ./internal/commands/ -count=1
   go test ./... -count=1
   ```
   Expected: GREEN 全仓
5. **Commit:**
   ```bash
   git add internal/commands/ internal/cluster/keys.go
   git commit -m "feat(cmd) HEXPIRETIME/HPEXPIRETIME/SUBSTR/SWAPDB/COMMAND LIST/CONFIG REWRITE"
   ```

---

### Task 2: MONITOR —— 实时命令流

**Files:**
- Create: `internal/commands/monitor_stream.go`（MonitorRegistry + 行格式化 + 广播）
- Create: `internal/commands/connpipe.go`（per-conn 串行化出站管道：注册 chan + 单 writer goroutine，写错自动摘除；T5 复用）
- Modify: `internal/network/router.go`（Dispatch 观测点：auth 通过且 handler 命中后、执行前，调用可选 `SetMonitorHook(func(ctx, name string, args []protocol.Value))`——仿 `SetNotifyPublisher` 模式，mu 保护）
- Modify: `internal/commands/server.go`（`h.client` 内 MONITOR case，或独立注册——定位 CLIENT 子命令结构后定）
- Test: `internal/commands/monitor_stream_test.go`（新建）

**Interfaces:**
- Consumes: `ConnFromContext`（network/context.go:18）、`ConnRegistry.IDOf/RemoteAddr`（conn.go:61+）、`Router.Dispatch`（router.go:151）、bulkString 取参惯例（router.go:271 slowlog 同法）
- Produces:
  - `func SetMonitorHook` on Router；`func RegisterMonitorStream(r, conns *ConnRegistry)`（或挂 server.go CLIENT 流程）返回注册并接 hook
  - `func (p *ConnPipe) Enqueue(line string) bool`（满/死 → false 且自动关闭，幂等）
  - 行格式：`<sec>.<06d> [<db> <addr>] "<cmd-lower>" "<arg>"...`，db 恒 0（单库）；无 conn 的 ctx（lua/直调）addr 记 `lua` 近似 → **decision: addr 取 RemoteAddr，取不到记 `local`，report 记 lua 口径近似**
  - 跳过名单（显式集合）：`MONITOR`、`QUIT`、`RESET`；`AUTH` 参数红action 为 `"***"`；admin 显式名单（CONFIG/ACL/DEBUG/SHUTDOWN/SAVE/BGSAVE/BGREWRITEAOF/LATENCY/SLOWLOG/SCRIPT/CLIENT/HELLO）不记录——report 记与真机 admin 全集差异

**行为规格：**

| 步骤 | 结果 |
|---|---|
| connA 发 MONITOR | +OK；A 进入订阅集 |
| connB 发 SET k v（A 已订阅） | A 收到 `+<ts> [0 <B地址>] "set" "k" "v"`（简单字符串行） |
| A 自身后续 GET | A 也收到自己的行（Redis 同：监控连接回流） |
| connA 发 MONITOR 再次 | +OK（幂等） |
| AUTH admin/QUIT | 不出现在流中；AUTH 行参数红action |
| 管道缓冲满（cap 1024） | 丢弃该条、计数，不阻塞 Dispatch；死连接自动摘除 |
| 订阅连接断开（写错） | 下次写失败 → 自动 Unmonitor |

**Steps:**

1. **Step 1（R1 前置）**：查 server 连接写路径（`cmd/gedis/main.go` + network 连接循环）：reply 是否经单一写点/已有 per-conn 锁。若无锁 → 先在该单一写点补 per-conn mutex（最小改动，report 记），**再**继续。结论进 report。
2. **RED**：`monitor_stream_test.go`（BDD 注释）：`Test_Monitor_StreamsCommands`（capture conn：订阅→dispatch SET→断言行 regex `^\d+\.\d{6} \[0 .+\] "set" "k" "v"$`）、`Test_Monitor_SkipsAndRedacts`（QUIT/AUTH/admin 不流、AUTH 参数不出现）、`Test_Monitor_IdempotentAndCleanup`（重复 +OK；模拟写错摘除）。
   ```bash
   go test ./internal/commands/ -run 'Test_Monitor' -count=1
   ```
   Expected: FAIL
3. 实现 ConnPipe（cap 1024、writer goroutine、Enqueue 幂等关闭）、MonitorRegistry（map[net.Conn]*ConnPipe + mu）、Router hook（Dispatch 内 name/args 已取、auth 通过后、`h(...)` 前调用，非阻塞）、CLIENT MONITOR case（+OK + 注册）。
4. 广播含订阅者自己；`hook` 拿不到 conn 时（ctx 无）addr 记 `local`。
   ```bash
   go test ./internal/commands/ -count=1 && go test ./... -count=1
   ```
   Expected: GREEN 全仓
5. **Commit:**
   ```bash
   git add internal/commands/ internal/network/router.go
   git commit -m "feat(cmd) MONITOR 实时命令流+per-conn 出站管道"
   ```

---

### Task 3: Phase 7 事件遗留 —— 官方事件表补发

**Files:**
- Modify: zset/list/geo 相关 handler 文件（ZPOPMIN/ZPOPMAX/BZPOPMIN/BZPOPMAX、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE、GETEX、TOUCH 的发布点，实现时按 handler 定位）
- Modify: `internal/commands/notify.go`（如需补类字母/事件名常量）
- Test: `internal/commands/notify_leftover_test.go`（新建）

**Interfaces:**
- Consumes: `Notify(class, event, key)`（notify.go:111）、batch1 T4 的 z/l 类字母与 mask（KEA 覆盖已建）
- Produces: 每个**官方表内**条目一条 Notify 调用；表外条目不发

**Steps:**

1. **权威核对（Step 1，强制）**：webfetch `https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/`，抄录相关命令的 class+event 名（例：zpop 系列 event 名、lmop/zmop 归属、zremrangebylex、geosearchstore、getex、touch 是否在表）。结果表贴进 report。
2. **RED**：`notify_leftover_test.go` —— 照 batch1 事件测试模式（**显式 Register 被测命令**，openMonitorSetup 不含 conn/list/generic 的教训；mask 测试含类位 KEA/KEg）：每个表内命令一断言（class+event+key）；表内无条目者断言**不发**（捕获 publisher 为空）。
   ```bash
   go test ./internal/commands/ -run 'Test_NotifyLeftover' -count=1
   ```
   Expected: FAIL
3. 实现：各 handler 成功分支加 `Notify(...)`；不动 batch1 已有点。
4. **决策**：touch/GETEX 等若官方表无条目 → 不发，report 记录"表核对结论"。
   ```bash
   go test ./internal/commands/ -count=1 && go test ./... -count=1
   ```
   Expected: GREEN
5. **Commit:**
   ```bash
   git add internal/commands/
   git commit -m "feat(events) 事件表外遗留命令补发事件（官方表核对）"
   ```

---

### Task 4: CLIENT TRACKING —— 命令面 + 跟踪表 + 读注册

**Files:**
- Modify: `internal/commands/conn.go`（ConnState 加 tracking 字段——注意 ConnRegistry 既有锁模式：state() 返回指针、调用点持锁；加带锁 accessor）
- Modify: `internal/commands/server.go`（`h.client` 补 TRACKING/CACHING 子命令 case）
- Modify: `internal/network/router.go`（可选 read-hook：`SetReadHook(func(ctx, name string, keys []string))`，在 handler 执行成功后调；复用 `extractKeys(name, args)` 结果——写命令/keys 空不触发）
- Create: `internal/commands/tracking.go`（TrackTable + OPTIN/OPTOUT 过滤 + 注册逻辑）
- Test: `internal/commands/tracking_test.go`（新建）

**Interfaces:**
- Consumes: `ConnRegistry.ProtoOf/SetProto`（conn.go:95/109）、`extractKeys`（network 包既有，auth 在用）、`ConnFromContext`
- Produces:
  - `CLIENT TRACKING on|off [BCAST] [PREFIX p]... [OPTIN] [OPTOUT] [NOLOOP]` → `+OK`；同时 OPTIN+OPTOUT → `ERR OPTIN and OPTOUT are not compatible`（措辞 report 记未核对）；缺 on/off 参数 → `ERR syntax error`
  - `CLIENT CACHING yes|no` → `+OK`；tracking 未开 → `ERR CLIENT CACHING can be called only when tracking is enabled`（report 记）
  - ConnState 字段：`Tracking, BCast, OptIn, OptOut, NoLoop bool; Prefixes []string; CachingYes bool`（CachingYes 被下一条带 key 命令消费）
  - `TrackTable`：`map[string]map[int64]struct{}`（userKey → connID 集合）+ 反向 `map[int64]map[string]struct{}`（清理用）；测试专用只读访问器
  - 读注册规则：read-hook 触发且 `!writeCmds[name]` 且 keys 非空：OPTIN→仅 CachingYes==true（消费后复位）、OPTOUT→跳过、默认→注册

**行为规格：**

| 步骤 | 结果 |
|---|---|
| CLIENT TRACKING on | +OK，ConnState.Tracking=true |
| 同前缀重复 flag / 未知 flag | -ERR syntax error |
| TRACKING on OPTIN + TRACKING off OPTOUT 同时 | -ERR 冲突文案 |
| TRACKING on 后 GET foo | TrackTable 含 foo→connID |
| OPTIN 模式下 GET（未先 CACHING yes） | 不注册 |
| OPTIN：CLIENT CACHING yes; GET foo | 注册且 CachingYes 复位 |
| 写命令（SET foo） | 不注册（writeCmds 过滤） |
| TRACKING off | 该 conn 全部表项清除 |

**Steps:**

1. **RED**：`tracking_test.go`（BDD 注释）：`Test_ClientTracking_OnOffAndFlags`（含冲突/语法错）、`Test_Tracking_RegistersReadKeys`（GET 注册、SET 不注册、OPTIN 两态、off 清理）。
   ```bash
   go test ./internal/commands/ -run 'Test_ClientTracking|Test_Tracking' -count=1
   ```
   Expected: FAIL
2. 实现 ConnState 字段+accessor、TrackTable、CLIENT 子命令、router read-hook（hook 调用点在 reply 返回前、仿 stats 时序；hook 内不得阻塞）。
3. **注意（R1）**：读 hook 只做表维护，不写 conn，无并发写问题。
   ```bash
   go test ./internal/commands/ -count=1 && go test ./... -count=1
   ```
   Expected: GREEN
4. **Commit:**
   ```bash
   git add internal/commands/ internal/network/router.go
   git commit -m "feat(tracking) CLIENT TRACKING 命令面+跟踪表+读注册"
   ```

---

### Task 5: 失效推送 —— 存储 hook → RESP3 push / RESP2 invalidate

**Files:**
- Modify: `internal/storage/pebble.go`（Set:128/Delete:146 成功后调 change hook；签名 `SetChangeHook(func(ctx context.Context, op byte, rawKey []byte))`，op 's'/'d'；SetEvictHook 同风格，memory.go:81 模式）
- Modify: `internal/storage/memory.go`（如 SetEvictHook 所在层需透传——实现时确认 evict 路径）
- Modify: `internal/commands/notify.go` 或新建 `internal/commands/tracking_push.go`（sink：userKeyFromRaw → TrackTable → 按 proto 分发）
- Modify: `cmd/gedis/main.go`（装配 change hook；测试装配在 helpers）
- Modify: `internal/commands/tracking.go`（INVALIDATE 通道发布 + ConnPipe 复用）
- Test: `internal/commands/tracking_push_test.go` + `internal/storage/change_hook_test.go`

**Interfaces:**
- Consumes: T2 的 `ConnPipe.Enqueue`、T4 的 TrackTable、`userKeyFromRaw`（notify.go:131）、`SetEvictHook`（memory.go:81）、`ConnRegistry.ProtoOf`、pubsub 发布（M5，发布到 `__redis__:invalidate`，订阅者收到 payload `["invalidate", [key]]` 或通道原生格式——按 pubsub 既有 API 定，测试钉实际形态）、`protocol.KindPush`（value.go:23）
- Produces:
  - `SetChangeSink(func(ctx context.Context, op byte, rawKey []byte))`（commands 包导出，装配用）
  - RESP3 行为：`protocol.Value{Kind: KindPush, Elems: [Bulk"invalidate", Array[Bulk key]]}` → 经该 conn 写出（经 R1 补的写点）
  - RESP2 行为：pubsub 发布 `__redis__:invalidate`，payload `Array[Bulk key]`
  - NOLOOP：`ConnFromContext(ctx)` == 观察者 conn → 跳过该 conn
  - evict：`SetEvictHook` 回调 → 同样走 sink（op 'e'），保证逐出也失效
  - **不读、不受 `NotifyString/notify-keyspace-events` 门控**（独立函数，硬约束 S7）

**行为规格：**

| 步骤 | 结果 |
|---|---|
| connA TRACKING on + GET foo；connB SET foo 1 | A 收 invalidate foo（RESP3）/ RESP2 订阅者收通道消息 |
| connB 自己 NOLOOP on | B 不收，A 收 |
| DEL foo / 过期惰性删 / evict | 均失效 |
| 无 tracking 观察者 | 零开销直通（map 查空） |
| notify-keyspace-events 为空（全关） | 失效照发（与 notify 无关） |
| storage.Set ctx 无 conn（replica apply/lua） | NOLOOP 无从排除 → 照发 |

**Steps:**

1. **RED**：`change_hook_test.go`（Set/Delete 触发、ctx 透传）+ `tracking_push_test.go`（BDD 注释）：`Test_Tracking_InvalidatesOnWrite`（RESP3 mock conn 断言 push value 形态）、`Test_Tracking_Resp2Fallback_PublishesInvalidate`、`Test_Tracking_NoloopExcludesWriter`、`Test_Tracking_InvalidatesOnEvictAndDelete`、`Test_Tracking_WorksWithoutNotifyConfig`（notify 全关仍发）。
   ```bash
   go test ./internal/storage/ -run 'Test_SetChangeHook' -count=1 && go test ./internal/commands/ -run 'Test_Tracking_' -count=1
   ```
   Expected: FAIL
2. 实现 storage hook（Set/Delete 成功后、锁外调用；panic 不外溢——recover 或文档约定 sink 不 panic）、sink、RESP3/RESP2 分发（**先确认 R1 写锁已就位**）、evict 二路接线、main.go 装配。
3. 覆盖边界自检：raw key 多类型前缀（s:/h:/l:/z:）经 userKeyFromRaw 归一；内部键（PEL 等）以 test 断言实际归一结果，异常者 report 记。
   ```bash
   go test ./internal/storage/ ./internal/commands/ -count=1 && go test ./... -count=1
   ```
   Expected: GREEN 全仓
4. **Commit:**
   ```bash
   git add internal/storage/ internal/commands/ internal/network/ cmd/gedis/
   git commit -m "feat(tracking) 存储变更hook→失效推送 RESP3 push/RESP2 invalidate"
   ```

---

### Task 6: BCAST/PREFIX + OPTIN/CACHING 完整语义 + 生命周期清理

**Files:**
- Modify: `internal/commands/tracking.go`（BCAST 前缀表：`map[int64][]prefix`；匹配=任一前缀 `strings.HasPrefix`；invalidation 分发加 BCAST 分支）
- Modify: `internal/commands/tracking_push.go`（分发器补 BCAST：对写入 userKey 前缀匹配的全部订阅 conn——T5 已留分发口）
- Modify: `internal/commands/conn.go`（ConnRegistry 若无 Remove → 补 `Remove(conn)` 并在连接关闭路径调用——定位 server 关闭点；清理该 conn 跟踪态与表项）
- Test: `internal/commands/tracking_bcast_test.go`（新建）

**Interfaces:**
- Consumes: T4/T5 全部产出
- Produces:
  - BCAST 语义：不依赖读注册；任何变更 key 前缀命中 → 通知该前缀下所有 BCAST 订阅者（含其自身除外按 NOLOOP）
  - `CLIENT TRACKING off` → 清 conn 全部态（正反向表 + BCAST 前缀）
  - 断连 → ConnRegistry.Remove → 同样清理（若 batch1/既有已有 Remove，复用）
  - OPTIN CACHING 跨命令消费语义回归（T4 已建，T6 补边界：CachingYes 不跨 conn、off 后残留复位）

**行为规格：**

| 步骤 | 结果 |
|---|---|
| connA TRACKING on BCAST PREFIX user: | connB SET user:1 → A 收（未 GET 过） |
| BCAST PREFIX user:；connB SET other:1 | A 不收 |
| 多 PREFIX 命中其一 | 收一次（去重，不重复发） |
| connA off / 断连 | 表项全清，后续写零通知 |
| OPTIN CachingYes 在 off 后 | 残留复位，不再生效 |

**Steps:**

1. **RED**：`tracking_bcast_test.go`（BDD 注释）：`Test_Bcast_InvalidatesByPrefix`（命中/不命中/多前缀去重）、`Test_Tracking_CleanupOnOffAndDisconnect`（off 清、Remove 清、残留复位）。
   ```bash
   go test ./internal/commands/ -run 'Test_Bcast|Test_Tracking_Cleanup' -count=1
   ```
   Expected: FAIL
2. 实现 BCAST 分发表+匹配、Cleanup 收敛到单一 `untrackConn(conn)`（off/Remove/写错三路复用）。
3. 回归 T4/T5 全部测试（不回退）。
   ```bash
   go test ./internal/commands/ -count=1 && go test ./... -count=1
   ```
   Expected: GREEN 全仓
4. **Commit:**
   ```bash
   git add internal/commands/
   git commit -m "feat(tracking) BCAST/PREFIX 失效匹配+跟踪态生命周期清理"
   ```

---

### Task 7: 文档汇总 —— README / CONTEXT / 路线图

**Files:**
- Modify: `README.md` + `README.zh-CN.md`（Scope 缺口命令段移除已交付项；功能清单加 MONITOR/CLIENT TRACKING/HEXPIRETIME 等；Phase 8 标记）
- Modify: `CONTEXT.md`（"2026-10-10 batch2" 交付段 + 全任务偏差清单汇总）
- Modify: `docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md`（Phase 8 节头 ✅ 2026-10-10 + 相关命令项；Phase 10 保持未勾）

**Interfaces:**
- Consumes: `.superpowers/sdd/2026-10-10-redis-gap-batch2/task-1..6-report.md` 全部偏差
- Produces: 文档与实际一致；git 无"缺口命令含 SUBSTR/SWAPDB..."类过期措辞

**Steps:**

1. 读全部 report，汇总偏差清单（逐条进 CONTEXT）。
2. 改 README/zh-CN（镜像同步）、CONTEXT、roadmap（Phase 8 整段 ✅；Phase 10 不动）。
3. 一致性自查：
   ```bash
   rg -n "SUBSTR|SWAPDB|MONITOR|CLIENT TRACKING" README.md README.zh-CN.md docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md
   ```
   Expected: 无"未实现/缺口"类过期措辞；Phase 10 段仍为未交付口径。
4. **Commit:**
   ```bash
   git add README.md README.zh-CN.md CONTEXT.md docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md
   git commit -m "docs(Phase8) batch2 交付汇总+缺口清单收敛"
   ```

---

## 任务依赖与顺序

T1（独立）、T2（独立，产出 ConnPipe）、T3（独立，只依赖 batch1 Notify）→ T4 → T5（依赖 T2 ConnPipe + T4 表）→ T6（依赖 T5）→ T7（最后汇总）。
**顺序不可调换**：T2 在 T5 前，因推送复用其出站管道与写锁结论；T4 在 T5 前，因失效分发查 T4 的表；T3 的事件核对独立于 tracking，可穿插但须在 T7 前完成。
