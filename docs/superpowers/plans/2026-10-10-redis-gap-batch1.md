# Redis 缺口补齐批次一（Keyspace 通知 / 逐出策略 / 高频命令）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 交付 Redis 缺口路线图 Phase 7（Keyspace 通知）与 Phase 9（逐出策略全家），补齐高频缺失命令与 WriteCommandSet 漏收，并修正过期文档。

**Architecture:** 事件通知走包级 `keyNotifier`（CONFIG 解析 → 位掩码 → 经 PubSubRegistry 发布到 `__keyspace@0__:*` / `__keyevent@0__:*` 两频道），发布点散布在各写命令 handler 内（跨切面审计）；逐出策略扩展 `storage/memory.go` 的 `lruEntry`（+expAt/freq 字段）与 `evictOne` 分支，OBJECT IDLETIME/FREQ 消费同一份数据；新命令沿用既有"四件套"注册模式。

**Tech Stack:** Go、Pebble、RESP2/RESP3、`testing`（进程内 router 装配 + PubSubRegistry 订阅断言）

**Spec:**
- `docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md`（Phase 7 / Phase 9 / 命令面部分）
- Redis 官方事件权威表：https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/ （正文逐条已固化到 Task 3/4/5 的事件表，以表为准；表外交叉规则见 Global Constraints §4）
- 真机 7.2.6 措辞对齐惯例（参照 `internal/commands/acl_accept_test.go` 注释风格）

## Global Constraints

1. **命令四件套**：新命令必须同时完成 `r.Register` + `acl.RegisterMeta` + `internal/commands/arity.go` 条目 + `internal/cluster/keys.go` 分类（**先查表**：GETSET/SETNX/SETEX/PSETEX/TOUCH/LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH 的 keys.go 条目**已存在**，勿重复；READONLY/READWRITE/SAVE 需确认）。
2. **写命令进 WriteCommandSet**：`internal/commands/write.go` 的 `WriteCommandSet()` 必须收录全部真写命令。本批新增应收录：GETSET、SETEX、PSETEX、SETNX、LMOVE、BLMOVE、RPOPLPUSH、BRPOPLPUSH；**不收录**：TOUCH、READONLY、READWRITE（ReadOnly meta）。Task 8 另行全量审计补漏。
3. **TDD 红→绿**：每个行为先写失败测试再实现；断言的期望值必须来自独立来源（Redis 官方事件表、真机 7.2.6 文案、本计划写死的字面量），禁止"期望值与实现同源"的同义反复测试。测试经公共接缝：router.Dispatch / handlers / CONFIG / registry 订阅，不测私有函数。
4. **事件语义权威** = 上方 Redis 官方表：**仅当目标 key 真正被修改才发事件**（SREM 删不存在元素、SET NX 失败、EXPIRE 目标不存在 → 零事件）。表已列命令严格照表；表外命令分两类——(a) 语义为已列表命令的直接变体（GETDEL→`del`、UNLINK→`del`、RENAMENX 成功→`rename_from`+`rename_to`）照镜像发并在 report 注明；(b) 表外且无法从表推导者（ZPOPMIN/ZPOPMAX/BZPOP\*、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE、GETEX、TOUCH）**本批不发事件**，Task 9 记入 CONTEXT.md 剩余偏差。
5. **配置类事件字母 `d/m/n/o/c`**：接受并解析（不报错），但本批不产生 module/key-miss/new/overwritten/type_changed 事件；`A` 展开为 `g$lshztdxea`，`a` 展开为 `lshzt`；**必须含 `K` 或 `E` 才投递**，空串禁用。非法字母报错（文案 `ERR invalid notify-keyspace-events parameter`，真机精确措辞未核对——report 注明）。
6. **单库约束不动**：SELECT 非 0 仍报错；通知频道固定 db 0（`@0__`）。HLL dense-only、RESP3 push 未接线等既有边界不在本批。
7. **不引入新第三方依赖；每任务完成后 commit 到 dev 分支（用户已批准自动 commit），全程不 push、不改 git 远端**。
8. **文档集中 Task 9**：Task 1-8 不改 README/CONTEXT/roadmap（避免任务间文档冲突）；所有必须记录的偏差（LFU 简化、READONLY/READWRITE 无行为差异、事件表外命令、OBJECT 自触达副作用、非法配置文案未核对）在各 task 的 report 里写明，Task 9 汇总落档。
9. **禁吞错与假实现**：不得用 `_ = err` 吞掉本批新引入的错误；不得用占位常量冒充真值（OBJECT REFCOUNT=1 例外——已在 spec 外，保留占位但更新注释）。

## Review Focus

1. **零修改不发事件**：SREM 删不存在元素 / SET NX 失败 / EXPIRE 目标不存在必须零投递 —— T3、T4 的"no-op 负例"测试钉住。
2. **配置门与失败原子性**：未启用类别或缺 K/E 时零投递；`CONFIG SET notify-keyspace-events` 非法字母必须报错且**旧 mask 不被破坏** —— T2 测试钉住。
3. **逐出正确性**：noeviction 满内存写入必须回 `ErrOOM` 且不删任何 key；volatile-\* 系绝不能逐出无 TTL 的 key；volatile-ttl 必须选 TTL 最小者 —— T1 测试钉住。
4. **OBJECT 真值不被自身读取污染**：IDLETIME/FREQ 必须先读统计再走会触达 LRU/LFU 的 `getAny`，否则 IDLETIME 恒 0 —— T1 测试钉住（先读后查顺序断言）。
5. **事件顺序契约**：LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH 的 push 事件必须先于 pop 事件送达；SETEX 先 `set` 后 `expire` —— T6、T7 测试按到达序断言。
6. **驱逐路径一致性**：evictOne 删除成功后 evicted 计数与 `e` 类事件同时发生；删除仍走完整 `p.Delete`（复本同步）—— T5 测试钉住（hook 只在成功删除后触发）。
7. **只读副本门补漏**：补 WriteCommandSet 后，readonly 模式下 HEXPIRE/HPERSIST/XREADGROUP/XACK/XCLAIM/XAUTOCLAIM 必须被拒 —— T8 测试钉住。

---

### Task 1: Phase 9 逐出策略全家 + OBJECT IDLETIME/FREQ 真值

**Files:**
- Modify: `internal/storage/memory.go`（`lruEntry`、`trackSet`、`trackGet`、`SetPolicy`、`evictOne`）
- Modify: `internal/commands/string.go:18-24`（KV 接口加 `ObjectStats`）
- Modify: `internal/commands/expire.go:250-295`（OBJECT IDLETIME/FREQ 分支改真值）
- Test: `internal/storage/evict_test.go`、`internal/storage/memory_test.go`（扩展现有）
- Test: `internal/commands/object_test.go`（新建）
- Modify: 其他实现 `commands.KV` 的测试 fake（编译驱动定位，返回零值或测试可控值）

**Interfaces:**
- Consumes: 既有 `lruEntry{at,size,expires}`、`evictForDelta`、`ErrOOM`、`Policy()/SetPolicy()`（memory.go）
- Produces:
  - `KV.ObjectStats(ctx context.Context, rawKey []byte) (idleSec uint64, freq uint8, ok bool)` —— **不经读路径**地查单个 raw key 的访问统计（不刷新 at/freq）；Task 无后续消费者，仅供 OBJECT 用
  - 接受的策略名单（精确字符串）：`noeviction`、`allkeys-random`、`volatile-random`、`volatile-ttl`、`allkeys-lru`、`volatile-lru`、`allkeys-lfu`、`volatile-lfu`
  - `lruEntry` 新字段：`expAt int64`（UnixNano 过期时刻，0=无）、`freq uint8`

- [ ] **Step 1: 写失败测试——SetPolicy 接受全部 8 种策略、拒绝未知策略**

```go
// internal/storage/memory_test.go 追加
func TestSetPolicyAcceptsRedisEvictionPolicies(t *testing.T) {
	p := newTestPebble(t) // 复用本文件既有构造 helper
	for _, pol := range []string{"noeviction", "allkeys-random", "volatile-random",
		"volatile-ttl", "allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu"} {
		if err := p.SetPolicy(pol); err != nil {
			t.Fatalf("SetPolicy(%q) = %v, want nil", pol, err)
		}
		if got := p.Policy(); got != pol {
			t.Fatalf("Policy() = %q, want %q", got, pol)
		}
	}
	if err := p.SetPolicy("bogus-policy"); err == nil {
		t.Fatal("SetPolicy(bogus-policy) = nil, want error")
	}
	if got := p.Policy(); got != "volatile-lfu" {
		t.Fatalf("失败的 SetPolicy 不得改变旧策略, Policy() = %q", got)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/storage/ -run TestSetPolicyAcceptsRedisEvictionPolicies -v`
Expected: FAIL（`noeviction` 等报 `unknown eviction policy`）

- [ ] **Step 3: 扩展 SetPolicy 与 lruEntry 字段**

```go
// memory.go：lruEntry 扩字段
type lruEntry struct {
	at      uint32
	size    int64
	expires bool
	expAt   int64 // 过期时刻 UnixNano；expires=false 时为 0
	freq    uint8 // LFU 简化计数：创建=1，每次访问 +1，饱和 255，无时间衰减
}

// SetPolicy 接受 Redis 全部 8 种策略名
func (p *Pebble) SetPolicy(s string) error {
	switch s {
	case "noeviction", "allkeys-random", "volatile-random", "volatile-ttl",
		"allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu":
		// 沿用既有赋值/加锁方式
		return nil
	}
	return fmt.Errorf("storage: unknown eviction policy %q", s)
}
```

`trackSet` 改造要点：Decode 出 `e.Expiry` 同时写 `expires` 与 `expAt`；**已存在条目保留并递增 freq**（饱和 255），新建条目 `freq=1`：

```go
func (p *Pebble) trackSet(key, value []byte) {
	expires, expAt := false, int64(0)
	if e, err := datastruct.Decode(value); err == nil {
		expires, expAt = e.Expiry != 0, e.Expiry
	}
	size := int64(len(key) + len(value))
	p.lruMu.Lock()
	old, ok := p.lru[string(key)]
	freq := uint8(1)
	if ok {
		p.usedBytes.Add(size - old.size)
		freq = old.freq
		if freq < 255 {
			freq++
		}
	} else {
		p.usedBytes.Add(size)
	}
	if p.lru == nil {
		p.lru = make(map[string]lruEntry)
	}
	p.lru[string(key)] = lruEntry{at: nowSec(), size: size, expires: expires, expAt: expAt, freq: freq}
	p.lruMu.Unlock()
}

func (p *Pebble) trackGet(key []byte) {
	p.lruMu.Lock()
	if e, ok := p.lru[string(key)]; ok {
		e.at = nowSec()
		if e.freq < 255 {
			e.freq++
		}
		p.lru[string(key)] = e
	}
	p.lruMu.Unlock()
}
```

- [ ] **Step 4: evictOne 按策略分支**

保留既有"采样 evictSampleN=5、候选 struct、解锁后 `p.Delete` + `evicted.Add(1)`"骨架，候选选择改为 switch。volatile-\* 系（`volatile-lru/volatile-random/volatile-ttl`）一律先过滤 `!e.expires`：

```go
switch policy {
case "noeviction":
	return false
case "allkeys-lru", "volatile-lru":
	// 既有逻辑：采样5取 at 最小（volatile 版带过滤）
case "allkeys-lfu", "volatile-lfu":
	// 采样5取 freq 最小；freq 相同取 at 较小者（volatile 版带过滤）
case "allkeys-random", "volatile-random":
	// 对过滤后的全表做水库抽样随机取1（volatile 版仅 expires 条目；无候选返回 false）
case "volatile-ttl":
	// 全表扫描取 expAt>0 中最小者；无带 TTL 的 key 返回 false（→ ErrOOM，符合 Redis）
}
```

- [ ] **Step 5: 写失败测试——逐出行为**

```go
// internal/storage/evict_test.go 追加（构造/触发写超限沿用本文件既有模式）
func TestEvictNoEvictionReturnsOOM(t *testing.T) {
	p := newTestPebbleWithMax(t, 128) // 沿用既有 helper；无则自建：SetMaxBytes(128)
	_ = p.SetPolicy("noeviction")
	mustSet(t, p, []byte("s:k1"), bytes.Repeat([]byte("x"), 100))
	err := p.Set(context.Background(), []byte("s:k2"), bytes.Repeat([]byte("y"), 100)) // 超限
	if !errors.Is(err, ErrOOM) {
		t.Fatalf("noeviction 超限 = %v, want ErrOOM", err)
	}
	// k1 必须原样存在
	if _, err := p.Get(context.Background(), []byte("s:k1")); err != nil {
		t.Fatalf("noeviction 不得删除既有 key: %v", err)
	}
}

func TestEvictVolatileTTLKeepsNonExpiringKey(t *testing.T) {
	// 写入：无 TTL key、短 TTL key、长 TTL key；maxbytes 压到只容一个；
	// 触发超限写 → 断言无 TTL key 存活、短 TTL key 先被删（EvictedCount>=1）
}

func TestEvictLFULowestFreqFirst(t *testing.T) {
	// max=250；写 A(100B)、B(100B)（都装得下）；GET A 十次（freq 升）；
	// 再写 C(100B) 触发超限（300>250）→ freq 最低的 B 被逐（200<=250 停止）
	// 断言：B 不存在、A 与 C 存活、EvictedCount==1
}
```

- [ ] **Step 6: 跑逐出测试确认失败 → 实现 evictOne 分支 → 确认通过**

Run: `go test ./internal/storage/ -run 'TestEvict' -v`
Expected: 先 FAIL（unknown policy / 分支缺失），实现后 PASS

- [ ] **Step 7: OBJECT 真值——先写失败测试**

```go
// internal/commands/object_test.go 新建；装配方式沿用 meta_complete_test.go 的 wireMainRouter
// 或直接构造 stringHandler + 真 Pebble（TempDir），与既有 commands 测试模式一致
func TestObjectIdleTimeAndFreqRealValues(t *testing.T) {
	r, cleanup := newTestRouter(t) // helper：wire + SetNotify 不涉及；此处只需 router
	defer cleanup()
	// 写 key → freq=1
	dispatch(t, r, "SET", "oidle", "v")
	// 连读 5 次：freq = 1 + 5 = 6（先断 FREQ——OBJECT 自身的 getAny 会再 +1，
	// 但 stats 读取先于 getAny，故本条断言不受影响；顺序不可颠倒）
	for i := 0; i < 5; i++ {
		dispatch(t, r, "GET", "oidle")
	}
	if got := dispatchInt(t, r, "OBJECT", "FREQ", "oidle"); got != 6 {
		t.Fatalf("FREQ = %d, want 6", got)
	}
	// IDLETIME：at 刚被 GET 刷新 → 0（stats 先于 getAny 读取）
	if got := dispatchInt(t, r, "OBJECT", "IDLETIME", "oidle"); got != 0 {
		t.Fatalf("IDLETIME = %d, want 0", got)
	}
	// 不存在 key：IDLETIME 回 nil bulk
	if v := dispatchVal(t, r, "OBJECT", "IDLETIME", "nokey"); v.Kind != protocol.KindBulkString || len(v.Bulk) != 0 {
		t.Fatalf("missing key IDLETIME = %+v, want nil bulk", v)
	}
}
```

注意：`OBJECT IDLETIME` 读取前不得因 handler 自身的 `getAny` 提前刷新 `at`——实现顺序见 Step 8。

- [ ] **Step 8: 实现 ObjectStats + OBJECT 分支**

KV 接口追加：

```go
ObjectStats(ctx context.Context, rawKey []byte) (idleSec uint64, freq uint8, ok bool)
```

storage 实现：lruMu 下查 map；`ok=false` 若无条目；`idleSec = uint64(nowSec() - e.at)`。**该方法不得调用 trackGet**。

expire.go OBJECT 分支实现顺序（关键）：

```go
// 1) 先按 typePrefixes 逐前缀查 ObjectStats（不触达），拿到 haveStats/idle/freq
// 2) 再 s.getAny(ctx, key) 做存在性/惰性过期检查
// 3) not found → nil bulk；REFCOUNT → 1；IDLETIME → idle（无统计则 0）；FREQ → freq（无统计则 0）
// 已知偏差：getAny 会刷新 at/freq（OBJECT 调用自触达，Redis 为 LOOKUP_NOTOUCH）——report 注明，Task 9 落档
```

同时把该分支上方注释"LFU 真值等 Phase 9 补齐"改为"REFCOUNT 无引用计数模型，恒 1（文档注明）"。

- [ ] **Step 9: 全量测试 + 编译修复 fake**

Run: `go build ./... && go test ./internal/storage/ ./internal/commands/ -count=1`
Expected: PASS。KV 接口变更导致的 fake 编译错误逐一补齐（返回零值或测试可控值）。

- [ ] **Step 10: Commit**

```bash
git add internal/storage/ internal/commands/
git commit -m "feat(eviction) Phase9 逐出策略全家+OBJECT IDLETIME/FREQ 真值"
```

---

### Task 2: Phase 7 通知基础设施（解析 / CONFIG 挂接 / 发布者注入 / SET+DEL 试点）

**Files:**
- Create: `internal/commands/notify.go`
- Modify: `internal/commands/monitor.go:194-257`（CONFIG GET/SET notify-keyspace-events 接线）
- Modify: `internal/commands/string.go`（SET、DEL/UNLINK 试点发布）
- Modify: `cmd/gedis/main.go:143` 附近（`SetNotifyPublisher` 注入）
- Modify: `internal/commands/meta_complete_test.go:42`（wireMainRouter 同步注入，保持装配顺序复刻）
- Test: `internal/commands/notify_test.go`（新建）

**Interfaces:**
- Consumes: `commands.RegisterPubSub(router) *PubSubRegistry`（pubsub.go）；`PubSubRegistry.Publish(channel string, payload protocol.Value) int64`（pubsub.go:615）；monitorHandler 既有 getNotify/setNotify
- Produces（Task 3-7 全部依赖，**签名精确**）:
  - `func Notify(class, event, key string)` —— class ∈ `"g"|$"|"l"|"s"|"h"|"z"|"t"|"x"|"e"`；未配置 K/E、类别未启用、publisher 未注入时全部 no-op
  - `func SetNotifyString(s string) error` —— 解析并原子替换 mask+raw；报错时旧值不变
  - `func NotifyString() string` —— 返回原始配置串（CONFIG GET 回显）
  - `func SetNotifyPublisher(pub func(channel string, payload protocol.Value) int64)` —— nil 允许（测试复位）
  - `func NotifyEvicted(rawKey string)` —— Task 5 消费（本 task 可先建骨架文件占位，或留到 Task 5 一起加；**本 task 不接线 evicted**）

- [ ] **Step 1: 写失败测试——解析与配置原子性**

```go
// internal/commands/notify_test.go 新建
func TestSetNotifyStringParsesFlags(t *testing.T) {
	t.Cleanup(func() { _ = SetNotifyString("") })
	if err := SetNotifyString("KEA"); err != nil {
		t.Fatalf("SetNotifyString(KEA) = %v", err)
	}
	if got := NotifyString(); got != "KEA" {
		t.Fatalf("NotifyString() = %q, want KEA", got)
	}
	if err := SetNotifyString("q"); err == nil {
		t.Fatal("非法字母必须报错")
	}
	if got := NotifyString(); got != "KEA" {
		t.Fatalf("失败的 Set 不得破坏旧配置, got %q", got)
	}
	if err := SetNotifyString(""); err != nil {
		t.Fatalf("空串合法: %v", err)
	}
}

func TestNotifyRespectsClassAndChannelGates(t *testing.T) {
	// 用真实 PubSubRegistry 收集发布：SetNotifyPublisher(reg.Publish)
	// 1) mask="K"（无 E）→ 只收到 __keyspace@0__:k，收不到 __keyevent@0__
	// 2) mask="KE"（无 $）→ Notify("$","set","k") 零投递
	// 3) mask="KE$" → 两频道各 1 条：channel=__keyspace@0__:k payload=bulk("set")；
	//                 channel=__keyevent@0__:set payload=bulk("k")
	// 4) SetNotifyPublisher(nil) → 零投递不 panic
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `go test ./internal/commands/ -run 'TestSetNotifyString|TestNotifyRespects' -v`
Expected: FAIL（符号不存在）

- [ ] **Step 3: 实现 notify.go**

```go
package commands

import (
	"fmt"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/protocol"
)

// 位定义（与 Global Constraints §5 字母表一致）
const (
	notifyK = uint32(1 << iota) // K
	notifyE                     // E
	notifyG                     // g generic
	notifyStr                   // $ string
	notifyL                     // l list
	notifyS                     // s set
	notifyH                     // h hash
	notifyZ                     // z zset
	notifyT                     // t stream
	notifyX                     // x expired
	notifyEv                    // e evicted
)

var notifyMu sync.RWMutex
var notifyMask uint32
var notifyRaw string
var notifyPub func(string, protocol.Value) int64

func classBit(class string) uint32 {
	switch class {
	case "g": return notifyG
	case "$": return notifyStr
	case "l": return notifyL
	case "s": return notifyS
	case "h": return notifyH
	case "z": return notifyZ
	case "t": return notifyT
	case "x": return notifyX
	case "e": return notifyEv
	}
	return 0
}

// SetNotifyString 解析 notify-keyspace-events 并原子生效；报错时旧值不变。
// 接受字母：KEg$lshztxed 及别名 A(=g$lshztdxea)、a(=lshzt)；d/m/n/o/c 接受但不产生事件。
func SetNotifyString(s string) error {
	var mask uint32
	for _, r := range s {
		switch r {
		case 'K': mask |= notifyK
		case 'E': mask |= notifyE
		case 'g': mask |= notifyG
		case '$': mask |= notifyStr
		case 'l': mask |= notifyL
		case 's': mask |= notifyS
		case 'h': mask |= notifyH
		case 'z': mask |= notifyZ
		case 't': mask |= notifyT
		case 'x': mask |= notifyX
		case 'e': mask |= notifyEv
		case 'A': mask |= notifyG | notifyStr | notifyL | notifyS | notifyH | notifyZ | notifyT | notifyX | notifyEv
		case 'a': mask |= notifyL | notifyS | notifyH | notifyZ | notifyT
		case 'd', 'm', 'n', 'o', 'c': // 接受但本批不产生对应事件
		default:
			return fmt.Errorf("ERR invalid notify-keyspace-events parameter")
		}
	}
	notifyMu.Lock()
	notifyMask, notifyRaw = mask, s
	notifyMu.Unlock()
	return nil
}

func NotifyString() string {
	notifyMu.RLock()
	defer notifyMu.RUnlock()
	return notifyRaw
}

func SetNotifyPublisher(pub func(string, protocol.Value) int64) {
	notifyMu.Lock()
	notifyPub = pub
	notifyMu.Unlock()
}

// Notify 发布一条键空间/键事件通知（db 固定 0）。
func Notify(class, event, key string) {
	notifyMu.RLock()
	mask, pub := notifyMask, notifyPub
	notifyMu.RUnlock()
	if pub == nil || mask == 0 {
		return
	}
	cb := classBit(class)
	if cb == 0 || mask&cb == 0 {
		return
	}
	if mask&notifyK != 0 {
		pub("__keyspace@0__:"+key, protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(event)})
	}
	if mask&notifyE != 0 {
		pub("__keyevent@0__:"+event, protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(key)})
	}
}
```

- [ ] **Step 4: CONFIG 接线（monitor.go）**

`getNotify()` 改为 `return NotifyString()`；`setNotify(s string) error` 改为 `return SetNotifyString(s)`（CONFIG SET 的错误上抛路径**照抄同文件 SetPolicy 的既有处理**）。CONFIG GET :195 保持现状（值来自 getNotify 即自动生效）。先在 notify_test 加一条经 router 的集成断言：

```go
func TestConfigNotifyRoundTrip(t *testing.T) {
	// CONFIG SET notify-keyspace-events KEA → +OK；CONFIG GET → bulk("KEA")
	// CONFIG SET notify-keyspace-events ZQ → Error；CONFIG GET 仍 "KEA"
}
```

- [ ] **Step 5: 发布者注入**

`cmd/gedis/main.go`：`pubsubReg := commands.RegisterPubSub(router)` 后加 `commands.SetNotifyPublisher(pubsubReg.Publish)`。**注意 main.go:222 还有 sentinel 的 RegisterPubSub——不得覆盖主 router 的注入**（SetNotifyPublisher 只在主注册点调用一次）。
`meta_complete_test.go` wireMainRouter：line 42 `RegisterPubSub(r)` 改为接住返回值并同样注入（保持"测试装配 = main 装配"惯例）。

- [ ] **Step 6: SET/DEL 试点发布 + 端到端测试**

string.go `h.set` 成功路径末尾（含 NX/XX 成功分支）：`Notify("$", "set", key)`——**NX/XX 失败路径零事件**。`h.del`（DEL/UNLINK 共用）对**每个确实被删除的 key**：`Notify("g", "del", key)`。

```go
func TestSetDelEmitNotifications(t *testing.T) {
	// 装配 router + SetNotifyPublisher + PSUBSCRIBE __key*__:*（经 registry 或 SUBSCRIBE 命令）
	// CONFIG SET notify-keyspace-events KEA
	// SET foo v            → 收到 __keyspace@0__:foo "set" + __keyevent@0__:set "foo"
	// SET foo v NX（已存在）→ 零新事件
	// DEL foo              → __keyspace@0__:foo "del" + __keyevent@0__:del "foo"
	// DEL nokey            → 零事件
}
```

订阅收数方式：沿用 `pubsub_test.go` 既有的订阅/断言模式（读 registry 的订阅通道，带超时）。

- [ ] **Step 7: 全量测试**

Run: `go test ./internal/commands/ ./cmd/gedis/ -count=1`
Expected: PASS（含 meta_complete_test 装配回归）

- [ ] **Step 8: Commit**

```bash
git add internal/commands/notify.go internal/commands/monitor.go internal/commands/string.go internal/commands/notify_test.go internal/commands/meta_complete_test.go cmd/gedis/main.go
git commit -m "feat(notify) Phase7 通知基础设施+SET/DEL 试点"
```

---

### Task 3: 事件发布点审计 A——generic + string

**Files:**
- Modify: `internal/commands/string.go`（APPEND/SETRANGE/INCR 族/MSET/GETDEL）
- Modify: `internal/commands/generic.go`（COPY/RENAME/RENAMENX/SORT STORE）
- Modify: `internal/commands/keyspace.go`（MOVE、DEL 已在 T2 做，核对）
- Modify: `internal/commands/expire.go`（EXPIRE 族/PERSIST）
- Modify: `internal/commands/dump_restore.go`（RESTORE）、`internal/commands/migrate.go`（MIGRATE）
- Test: `internal/commands/notify_string_test.go`（新建）

**Interfaces:**
- Consumes: `Notify(class, event, key)`、`SetNotifyString`、`SetNotifyPublisher`（Task 2）
- Produces: 无新接口（纯发布点接入）

**事件表（权威，Redis 官方表原文固化；class 为 Notify 第一参）：**

| 命令 | 事件（按此序） | class | 条件 |
|---|---|---|---|
| APPEND | `append` | `$` | 成功追加 |
| SETRANGE | `setrange` | `$` | 成功 |
| INCR/DECR/INCRBY/DECRBY | `incrby` | `$` | 成功 |
| INCRBYFLOAT | `incrbyfloat` | `$` | 成功 |
| MSET | 每 key 一条 `set` | `$` | 每个 key |
| SET | `set` | `$` | 真写成功（NX/XX 失败零事件；T2 已接） |
| GETDEL | `del` | `g` | key 被删除（表外镜像） |
| DEL/UNLINK | 每 key 一条 `del` | `g` | 该 key 确实存在并被删（T2 已接） |
| COPY | `copy_to` | `g` | 复制成功 |
| RENAME | `rename_from`(旧) + `rename_to`(新) | `g` | 按此序 |
| RENAMENX | 同 RENAME 两条 | `g` | 仅成功时（表外镜像） |
| MOVE | `move_from`(源) + `move_to`(目标) | `g` | 仅成功移库时（单库下恒不触发——实现仍接线，真机对齐留 TODO 注释） |
| EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT | `expire` | `g` | 正超时/未来时间戳且 key 存在 |
| EXPIRE 族（过去时间戳） | `del` | `g` | 以删除收场时 |
| PERSIST | `persist` | `g` | TTL 确实被清除 |
| SORT STORE | `sortstore` | `g` | STORE 成功 |
| SORT STORE 结果空且旧 key 被删 | `del` | `g` | 特例 |
| RESTORE | `restore` | `g` | 成功 |
| MIGRATE | `del` | `g` | 源 key 被移除时 |

- [ ] **Step 1: 写失败测试（每类挑代表 + 全表冒烟）**

```go
// internal/commands/notify_string_test.go
// helper 复用 notify_test.go 的“订阅+收数”断言（保持 expectEvents(t, r, func(){...}, want []event)）
func TestGenericStringEvents(t *testing.T) {
	// 用例表（每条：动作序列 → 期望事件序列，频道+载荷字节精确断言）：
	// 1. SET k v; APPEND k x            → set, append
	// 2. INCR n                          → incrby
	// 3. SETNX k2 v（新建）; SETNX k2 v2（失败）→ set, （第二步零事件）
	// 4. SET ex 100; EXPIRE ex 50        → expire
	// 5. SET past v; EXPIRE past -100    → （若实现删除 key 则 del；否则零事件——见 Step 3 注）
	// 6. PERSIST ex                      → persist（TTL 已清）; 再 PERSIST ex → 零事件
	// 7. MSET a 1 b 2                    → set(a), set(b)（两条）
	// 8. RENAME r1 r2（r1 存在）          → rename_from(r1), rename_to(r2)
	// 9. GETDEL gd                       → del(gd)
	// 10. COPY c1 c2                     → copy_to(c2)
	// 11. SET s1 v; DEL s1               → set(s1), del(s1)
}
```

每条用例先跑确认 FAIL（零事件），再逐条接线。

- [ ] **Step 2: 按表逐命令接入 Notify**

插入点＝各 handler 确认成功修改数据之后、return 之前。要点：
- **EXPIRE 族**：`expire.go`——正超时成功 `Notify("g","expire",key)`；过去时间戳若实现"删除 key"行为则 `Notify("g","del",key)`（先读现有实现确认语义；若实现为"设为已过期、惰性删"，则不在此发 del，惰性删由 Task 5 的 lookupRaw `expired` 事件覆盖——**report 必须写明所选路径**）
- **PERSIST**：仅 TTL 确实被清除才发
- **SORT STORE**：`generic.go`——STORE 成功发 `sortstore`；结果空且原 key 被删补 `del`
- **RENAME/RENAMENX/COPY/MIGRATE/RESTORE**：按表条件
- **MSET**：循环体内每 key 一条

- [ ] **Step 3: 负例确认**：SET NX 失败、EXPIRE 不存在 key、PERSIST 无 TTL、RENAMENX 目标存在、DEL 不存在 key —— 全部零事件（测试用例覆盖）。

- [ ] **Step 4: 跑测试**

Run: `go test ./internal/commands/ -run 'TestGenericStringEvents|TestNotify|TestSetDel' -count=1 -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/commands/
git commit -m "feat(notify) generic/string 类事件发布点"
```

---

### Task 4: 事件发布点审计 B——hash / list / set / zset

**Files:**
- Modify: `internal/commands/hash.go`、`hash_multi.go`、`hash_incr.go`、`hash_expire.go`
- Modify: `internal/commands/list.go`、`list_mod.go`
- Modify: `internal/commands/set.go`
- Modify: `internal/commands/zset.go`
- Test: `internal/commands/notify_struct_test.go`（新建）

**Interfaces:**
- Consumes: `Notify(class, event, key)`（Task 2）
- Produces: 无新接口

**事件表（权威固化）：**

| 命令 | 事件（按此序） | class | 条件 |
|---|---|---|---|
| HSET/HSETNX/HMSET | 单条 `hset` | `h` | 变参也只发 1 条 |
| HDEL | `hdel`；结果空且删 key 补 `del` | `h`/`g` | 仅真删字段 |
| HINCRBY | `hincrby` | `h` | 成功 |
| HINCRBYFLOAT | `hincrbyfloat` | `h` | 成功 |
| HPERSIST | `hpersist` | `h` | 成功 |
| HEXPIRE/HEXPIREAT/HPEXPIRE/HPEXPIREAT | `hexpired` | `h` | 按官方表：命令调用即发；**字段惰性过期删除时也发 `hexpired`**（hash_expire.go 清理路径） |
| LPUSH/LPUSHX | 单条 `lpush` | `l` | 变参 1 条 |
| RPUSH/RPUSHX | 单条 `rpush` | `l` | 变参 1 条 |
| LPOP | `lpop`；弹空删 key 补 `del` | `l`/`g` | |
| RPOP | `rpop`；同上补 `del` | `l`/`g` | |
| LSET | `lset` | `l` | 成功 |
| LINSERT | `linsert` | `l` | 成功 |
| LREM | `lrem`；空补 `del` | `l`/`g` | |
| LTRIM | `ltrim`；空补 `del` | `l`/`g` | |
| SADD | 单条 `sadd` | `s` | 变参 1 条 |
| SREM | `srem`；空补 `del` | `s`/`g` | 仅真删成员 |
| SPOP | `spop`；空补 `del` | `s`/`g` | |
| SMOVE | 源 `srem` + 目标 `sadd` | `s` | 按此序 |
| SINTERSTORE/SUNIONSTORE/SDIFFSTORE | `sinterstore`/`sunionstore`/`sdiffstore`；结果空且旧 key 被删补 `del` | `s`/`g` | |
| ZADD | 单条 `zadd` | `z` | 变参 1 条 |
| ZINCRBY | `zincr` | `z` | 成功 |
| ZREM | `zrem`；空补 `del` | `z`/`g` | 仅真删成员 |
| ZREMRANGEBYRANK | `zrembyrank`；空补 `del` | `z`/`g` | |
| ZREMRANGEBYSCORE | `zrembyscore`；空补 `del` | `z`/`g` | |
| ZDIFFSTORE/ZINTERSTORE/ZUNIONSTORE | `zdiffstore`/`zinterstore`/`zunionstore`；结果空且旧 key 被删补 `del` | `z`/`g` | |

**表外不接（Global Constraints §4b，Task 9 落档）**：ZPOPMIN/ZPOPMAX/BZPOP\*、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE。

- [ ] **Step 1: 写失败测试**

```go
// internal/commands/notify_struct_test.go
func TestHashListSetZSetEvents(t *testing.T) {
	// 代表用例（全表逐条冒烟，断言频道+载荷+顺序）：
	// HSET h f v（新建）→ hset；HDEL h f（清空）→ hdel, del
	// LPUSH l a b（变参）→ 单条 lpush；LPOP l（弹空）→ lpop, del
	// SADD s m1 m2 → 单条 sadd；SREM s missing（不存在成员）→ 零事件
	// SMOVE s s2 m1 → srem(s), sadd(s2)
	// ZADD z 1 m → zadd；ZINCRBY z 1 m → zincr；ZREM z missing → 零事件
	// HEXPIRE h 10 F 1 f → hexpired
	// SINTERSTORE out a b（结果空且 out 已存在）→ sinterstore, del
	// LPUSH lp x; LREM lp 0 x（清空）→ lpush, lrem, del
}
```

- [ ] **Step 2: 逐文件接线**——每个成功修改点后插 `Notify(...)`；"结果空且旧 key 被删"的 store 类特例在删除旧 key 的分支补 `Notify("g","del",...)`。
- [ ] **Step 3: hash 字段惰性过期**——定位 hash_expire.go 中"读取时删除过期字段/整 key 过期"路径，字段级清理发 `Notify("h","hexpired",key)`；若该路径在 lookupRaw（整 key 过期）则属 Task 5 的 `expired`，勿重复。
- [ ] **Step 4: 跑测试**

Run: `go test ./internal/commands/ -run 'TestHashListSetZSetEvents' -count=1 -v`
Expected: PASS；`go test ./internal/commands/ -count=1` 全绿。

- [ ] **Step 5: Commit**

```bash
git add internal/commands/
git commit -m "feat(notify) hash/list/set/zset 类事件发布点"
```

---

### Task 5: 事件发布点审计 C——stream + expired + evicted

**Files:**
- Modify: `internal/commands/stream.go`、`stream_trim.go`（XADD/XDEL/XTRIM/XSETID/XGROUP）
- Modify: `internal/commands/keyspace.go:64-69`（lookupRaw 惰性过期删除分支）
- Modify: `internal/commands/expiry.go:56-60`（SweepOnce 删除成功后）
- Modify: `internal/storage/memory.go`（evictOne 成功删除后回调）
- Modify: `cmd/gedis/main.go`、`internal/commands/meta_complete_test.go`（hook 注入）
- Test: `internal/commands/notify_stream_test.go`（新建）、`internal/storage/evict_test.go`（扩展）

**Interfaces:**
- Consumes: `Notify`、`SetNotifyPublisher`（Task 2）
- Produces:
  - `func NotifyEvicted(rawKey string)`（notify.go 追加）——按 `commands.typePrefixes`（keyspace.go:39）剥前缀得用户 key 后 `Notify("e","evicted",userKey)`；剥不出前缀则原样用
  - `func (p *Pebble) SetEvictHook(f func(rawKey string))`（memory.go）——evictOne 中 `p.Delete` **成功后**调用（nil 安全）

**事件表（权威固化）：**

| 命令/路径 | 事件（按此序） | class | 条件 |
|---|---|---|---|
| XADD | `xadd`；发生 MAXLEN 裁剪则随后 `xtrim` | `t` | 成功 |
| XTRIM | `xtrim` | `t` | 成功 |
| XDEL | 单条 `xdel` | `t` | 多 entry 也 1 条 |
| XSETID | `xsetid` | `t` | 成功 |
| XGROUP CREATE | `xgroup-create` | `t` | 成功 |
| XGROUP CREATECONSUMER | `xgroup-createconsumer` | `t` | 成功 |
| XGROUP DELCONSUMER | `xgroup-delconsumer` | `t` | 成功 |
| XGROUP DESTROY | `xgroup-destroy` | `t` | 成功 |
| XGROUP SETID | `xgroup-setid` | `t` | 成功 |
| 惰性过期（lookupRaw 发现过期删除） | `expired` | `x` | 每删一条 |
| 后台清扫（SweepOnce 删除） | `expired` | `x` | 每删一条 |
| maxmemory 驱逐（evictOne） | `evicted` | `e` | 每驱逐一条 |

- [ ] **Step 1: 写失败测试——stream 事件**

```go
// internal/commands/notify_stream_test.go
func TestStreamEvents(t *testing.T) {
	// XADD s * f v → xadd；XADD s MAXLEN~ 1 * f v（触发裁剪）→ xadd, xtrim
	// XTRIM s MINID 0-1 → xtrim
	// XDEL s id → xdel；XSETID s <ms>-0 → xsetid
	// XGROUP CREATE s g $ → xgroup-create；XGROUP DESTROY s g → xgroup-destroy
	// （SETID/DELCONSUMER/CREATECONSUMER 按同模式冒烟）
}
```

- [ ] **Step 2: 写失败测试——expired**

```go
func TestExpiredEventsLazyAndSweep(t *testing.T) {
	// a) SET k v EX 1（或 PX 50，等待）；随后 GET k（惰性过期）→
	//    __keyspace@0__:k "expired" + __keyevent@0__:expired "k"
	// b) 写短 TTL key 不访问；手动调 Expirer.SweepOnce() → expired 事件
	//    （装配处参考 expiry_test.go 既有方式）
}
```

- [ ] **Step 3: 写失败测试——evicted（storage 层）**

```go
// internal/storage/evict_test.go 追加
func TestEvictHookFiresOnEviction(t *testing.T) {
	// SetEvictHook 收集 rawKey；触发驱逐 → 断言 hook 收到被删 raw key（如 "s:k1"）
	// noeviction 模式 OOM → hook 零次
}
```

- [ ] **Step 4: 实现三路事件**

1. stream 各成功点插 `Notify("t", ...)`；
2. `lookupRaw` 过期分支（keyspace.go:64-69，`kv.Delete` 后）：`Notify("x", "expired", key)`（此处 key 即用户 key）；
3. `SweepOnce` 删除成功后：剥前缀 → `Notify("x", "expired", userKey)`（剥前缀 helper 放 notify.go，与 NotifyEvicted 共用）；
4. `evictOne` 成功删除后：`p.evictHook(key)`（rawKey）；
5. 装配：`cmd/gedis/main.go` store 创建后 `store.SetEvictHook(commands.NotifyEvicted)`；`meta_complete_test.go:19` store 创建后同序注入。

- [ ] **Step 5: 跑测试**

Run: `go test ./internal/commands/ ./internal/storage/ -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/commands/ internal/storage/ cmd/gedis/main.go
git commit -m "feat(notify) stream/expired/evicted 事件"
```

---

### Task 6: 命令批 A——GETSET / SETEX / PSETEX / SETNX / TOUCH

**Files:**
- Modify: `internal/commands/string.go`（handler + 注册 + meta）
- Modify: `internal/commands/arity.go`
- Modify: `internal/commands/write.go`（WriteCommandSet 收录 GETSET/SETEX/PSETEX/SETNX，不含 TOUCH）
- Test: `internal/commands/string_batch_test.go`（新建）

**Interfaces:**
- Consumes: `Notify`（Task 2）；`typePrefixes`、`lookupRaw`、既有 SET 的 TTL/编码 helper
- Produces: 五个新命令的注册与行为（后续 Task 无依赖）

**行为规格（Redis 7.2 语义）：**

| 命令 | arity | meta（四件套之二） | 行为 | 事件 |
|---|---|---|---|---|
| `GETSET key value` | 3 | `{Name:"GETSET", Category:"string", Keys:{First:0,Last:0}}` | 覆盖写并**清除 TTL**；返回旧值 bulk，无旧值回 nil | `set`(class `$`) |
| `SETEX key seconds value` | 4 | string, {0,0} | seconds≤0 → `ERR invalid expire time in 'setex' command`；设 TTL | `set` 然后 `expire`(class `$`/`g`)——**顺序断言** |
| `PSETEX key milliseconds value` | 4 | string, {0,0} | ms≤0 → `ERR invalid expire time in 'psetex' command` | 同 SETEX |
| `SETNX key value` | 3 | string, {0,0} | 存在回 0 不改；不存在回 1 | 仅成功时 `set`(`$`) |
| `TOUCH key [key ...]` | -2 | `{Name:"TOUCH", Category:"keyspace", ReadOnly:true, Keys:{First:-1}}` | 返回存在的 key 数；存在即刷新访问时间（经 lookupRaw 触达）；**不进 WriteCommandSet** | 无（表外§4b） |

keys.go 五个条目均已存在（T217/T61/T218 区段），**勿改**；若核对发现缺条目再补并在 report 说明。

- [ ] **Step 1: 写失败测试**

```go
// internal/commands/string_batch_test.go
func TestGetSet(t *testing.T) { /* SET k v; GETSET k v2 → "v"；GET k → v2；TTL 语义：
	SET k2 v EX 100; GETSET k2 v2 → "v"；TTL k2 → -1（GETSET 清 TTL）
	GETSET fresh v → nil；GET fresh → v */ }
func TestSetexPsetex(t *testing.T) { /* SETEX k 10 v; TTL k ≈10；
	SETEX k 0 v → Error "ERR invalid expire time in 'setex' command"（真机文案）
	PSETEX k 5000 v; PTTL ≈5000；PSETEX k -1 → psetex 文案 */ }
func TestSetnx(t *testing.T) { /* 新建→1 且值在；已存在→0 且值不变、TTL 不变 */ }
func TestTouch(t *testing.T) { /* SET a; TOUCH a b → 1（b 不存在）；
	TOUCH → wrong number of arguments；readonly 模式下 TOUCH 可执行（非写集合）*/ }
func TestNewStringCommandsEvents(t *testing.T) { /* KEA 下：
	SETNX 新建 → set；SETEX → set 后 expire（按到达序）；GETSET → set */ }
```

- [ ] **Step 2: 跑测试确认失败** → `go test ./internal/commands/ -run 'TestGetSet|TestSetex|TestSetnx|TestTouch|TestNewString' -v` Expected: FAIL（unknown command）

- [ ] **Step 3: 实现**——四件套（meta 追加进 stringMeta；arity 条目 `"GETSET": 3, "SETEX": 4, "PSETEX": 4, "SETNX": 3, "TOUCH": -2`；register 列表追加 5 行）；handler 复用既有 SET/编码 helper；`WriteCommandSet` 加前四个；事件按表接入；错误文案用表中精确串。

- [ ] **Step 4: 跑测试确认通过 + meta 完备性**

Run: `go test ./internal/commands/ -count=1`（含 TestMetaComplete 自动校验新命令有 meta）
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/commands/
git commit -m "feat(cmd) GETSET/SETEX/PSETEX/SETNX/TOUCH"
```

---

### Task 7: 命令批 B——LMOVE / BLMOVE / RPOPLPUSH / BRPOPLPUSH + READONLY / READWRITE

**Files:**
- Modify: `internal/commands/list.go` 或 `list_mod.go`（LMOVE/RPOPLPUSH）、`list_block.go`（BLMOVE/BRPOPLPUSH 阻塞复用）
- Modify: `internal/commands/keyspace.go` 或 `conn.go`（READONLY/READWRITE 注册位置——跟随既有"无 key 命令"归属惯例）
- Modify: `internal/commands/arity.go`、`internal/commands/write.go`（收录四个 list 命令；READONLY/READWRITE 不收录）
- Test: `internal/commands/list_move_test.go`（新建）

**Interfaces:**
- Consumes: `Notify`（Task 2）；`blockPop`（list_block.go:71）的阻塞等待机制；既有 LPOP/RPUSH 原子写 helper
- Produces: LMOVE 家族行为 + READONLY/READWRITE（per-conn 标志仅存 ctx，见下）

**行为规格：**

| 命令 | arity | meta | 行为 | 事件（按此序） |
|---|---|---|---|---|
| `LMOVE source destination <LEFT\|RIGHT> <LEFT\|RIGHT>` | 5 | list, Keys{First:0,Last:1} | 原子弹出+压入；源空回 nil；侧向词法错误 `ERR syntax error` | 按 whereto 发 `lpush` 或 `rpush`，**先于** 按 wherefrom 发的 `lpop` 或 `rpop`；源弹空删 key 补 `del` |
| `BLMOVE source destination <LEFT\|RIGHT> <LEFT\|RIGHT> timeout` | 6 | list, {0,1} | timeout 浮点秒；0=永久；负数 `ERR timeout is negative`；沿用 blockPop 等待机制 | 同 LMOVE |
| `RPOPLPUSH source destination` | 3 | list, {0,1} | ≡ LMOVE source destination RIGHT LEFT | `lpush`(dst) 先，`rpop`(src) 后；源空补 `del` |
| `BRPOPLPUSH source destination timeout` | 4 | list, {0,1} | 阻塞版 RPOPLPUSH（Redis 7.2 仍支持） | 同 RPOPLPUSH |
| `READONLY` | 1 | `{Name:"READONLY", Category:"admin", ReadOnly:true}` | 回 `+OK`；**记录 per-conn 标志（ctx 可变状态）但无行为差异**（见 Ruling） | 无 |
| `READWRITE` | 1 | 同上 | 回 `+OK`；清标志 | 无 |

**Ruling（执行时记 ledger）**：READONLY/READWRITE 在非 cluster 副本场景下 Redis 本身即为 no-op；gedis cluster 无 per-slot 副本读路由，本批**注册命令 + ctx 存标志 + 文档注明"集群副本读路由待后续"**，不做 router 改造。readonly 全局门（router.go:208）不受影响——两命令 ReadOnly:true，副本模式可执行。

- [ ] **Step 1: 写失败测试**

```go
// internal/commands/list_move_test.go
func TestLmove(t *testing.T) { /* RPUSH src a b c; LMOVE src dst RIGHT LEFT → "c"；
	dst=[c]；LMOVE src dst LEFT LEFT → "a"；空源 → nil；
	LMOVE src dst MIDDLE LEFT → ERR syntax error */ }
func TestRpoplpush(t *testing.T) { /* RPUSH s a b; RPOPLPUSH s d → "b"；d=[b] */ }
func TestBlmoveBlockingAndTimeout(t *testing.T) { /* 后台 goroutine 延迟 RPUSH；
	BLMOVE 等到值；BLMOVE timeout 0.05 于空源 → nil（超时）；负数 → ERR timeout is negative */ }
func TestLmoveEventsOrder(t *testing.T) { /* KEA：LMOVE src dst RIGHT LEFT →
	先 __keyspace@0__:dst "rpush"，后 __keyspace@0__:src "rpop"（按到达序断言）；
	源弹空 → 追加 __keyspace@0__:src "del" */ }
func TestReadonlyReadwrite(t *testing.T) { /* READWRITE→OK；READONLY→OK；
	wrong number of arguments → 各报错；readonly 副本模式（SetReadOnly(true)）下 READONLY 仍 OK */ }
```

- [ ] **Step 2: 跑测试确认失败**
- [ ] **Step 3: 实现**——四件套 + WriteCommandSet 收录四个 list 命令 + 事件接线；阻塞版复用 `blockPop` 的等待通道（等 source 可用 → 执行 LMOVE 主体；超时语义与 BLPOP 既有实现一致——先读 list_block.go 与 list_block_test.go 对齐文案）。READONLY/READWRITE：ctx 可变标志参照 `network/context.go` 的 `ContextWithUser` 模式（network 包新增或 commands 内实现，取改动最小者），仅存不读。
- [ ] **Step 4: 跑测试确认通过** → `go test ./internal/commands/ -count=1`
- [ ] **Step 5: Commit**

```bash
git add internal/commands/ internal/network/
git commit -m "feat(cmd) LMOVE/BLMOVE/RPOPLPUSH/BRPOPLPUSH/READONLY/READWRITE"
```

---

### Task 8: 修复批——WriteCommandSet 全量审计补漏 + SAVE

**Files:**
- Modify: `internal/commands/write.go`（WriteCommandSet）
- Modify: `internal/commands/server.go:42-43` 附近（注册 SAVE）+ meta + `internal/commands/arity.go`
- Test: `internal/commands/write_set_test.go`（新建或扩展既有 write 相关测试）

**Interfaces:**
- Consumes: 本批与历史全部已注册命令清单（`router.Commands()`）
- Produces: readonly 门完整语义

**已知漏收（rg 验证）**：`HEXPIRE`、`HEXPIREAT`、`HPEXPIRE`、`HPEXPIREAT`、`HPERSIST`、`XREADGROUP`、`XACK`、`XCLAIM`、`XAUTOCLAIM`。**另需全量审计**：遍历 `WriteCommandSet()` 对照全部已注册命令，任何"成功执行会改数据"的命令缺收都补上（含检查 XSETID、GEORADIUS\*\_STORE、BITOP\_NOT、RESTORE、SORT STORE 目的等边界），report 列出审计结论。

**SAVE**：注册为既有 `h.unsupportedPersistence`（server.go:407，与 BGSAVE 同档诚实报错），arity `"SAVE": 1`，meta 照抄 BGSAVE 条目改名。

- [ ] **Step 1: 写失败测试**

```go
// internal/commands/write_set_test.go
func TestWriteCommandSetCoversAllMutatingCommands(t *testing.T) {
	// 期望名单（字面量硬编码——独立来源）：
	want := []string{"HEXPIRE","HEXPIREAT","HPEXPIRE","HPEXPIREAT","HPERSIST",
		"XREADGROUP","XACK","XCLAIM","XAUTOCLAIM",
		// 本批新增（若 Task6/7 已收则为回归钉）：
		"GETSET","SETEX","PSETEX","SETNX","LMOVE","BLMOVE","RPOPLPUSH","BRPOPLPUSH"}
	for _, c := range want {
		if !WriteCommandSet()[c] { t.Errorf("WriteCommandSet 缺 %s", c) }
	}
	// 反向钉：只读命令不得进集合
	for _, c := range []string{"TOUCH","READONLY","READWRITE","GET","LRANGE"} {
		if WriteCommandSet()[c] { t.Errorf("只读命令 %s 不得进 WriteCommandSet", c) }
	}
}

func TestReadonlyReplicaRejectsHexpires(t *testing.T) {
	// r.SetReadOnly(true) + SetWriteCommands(WriteCommandSet())
	// dispatch HPERSIST h f → Error "READONLY You can't write against a read only replica."
	// dispatch READONLY → OK（Task7 meta ReadOnly 放行）
}

func TestSaveFailsHonestly(t *testing.T) { /* SAVE → ERR not supported on this engine: no RDB/AOF persistence（与 BGSAVE 同文案）；SAVE extra-arg → wrong number */ }
```

- [ ] **Step 2: 跑测试确认失败** → `go test ./internal/commands/ -run 'TestWriteCommandSet|TestReadonlyReplica|TestSave' -v`
- [ ] **Step 3: 补 write.go 名单（含审计发现的其他漏收）+ 注册 SAVE 四件套**
- [ ] **Step 4: 跑测试确认通过** → `go test ./internal/commands/ ./internal/network/ -count=1`
- [ ] **Step 5: Commit**

```bash
git add internal/commands/
git commit -m "fix(write-set) 补漏 HEXPIRE/XREADGROUP 等写命令+SAVE 注册"
```

---

### Task 9: 文档交付——README / CONTEXT / 路线图勾选

**Files:**
- Modify: `README.md`（:120-122 服务端 TLS 段；功能清单补 Phase 7/9 与新命令）
- Modify: `CONTEXT.md`（:9 TLS 过期措辞；交付清单与偏差记录）
- Modify: `docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md`（勾选 Phase 7、Phase 9 及本批完成的命令项；Phase 8/10 保持未勾）

**Interfaces:**
- Consumes: Task 1-8 的 report 文件（`.superpowers/sdd/` 下）里记录的全部偏差
- Produces: 文档与实际一致

**必须落档的偏差清单（从各 report 汇总，逐条写入 CONTEXT.md）：**
1. 服务端 TLS **已实现**（cmd/gedis/main.go 监听口 TLS 包装 + 自签测试）——修正 README:120-122 与 CONTEXT:9 的"未实现"旧措辞（git 历史 87bda4f 可证）。
2. LFU 为简化实现：访问 +1 饱和 255、无 Redis 概率增量与时间衰减；OBJECT FREQ 同源。
3. OBJECT IDLETIME/FREQ：OBJECT 自身读取会刷新统计（非 LOOKUP_NOTOUCH）；未被 track 的 key（重启后仅被读过）回 0。
4. READONLY/READWRITE 为注册级支持（+OK + per-conn 标志），集群副本读路由未接线。
5. 事件表外命令不发事件：ZPOPMIN/ZPOPMAX/BZPOP\*、LMPOP/BLMPOP/ZMPOP/BZMPOP、ZREMRANGEBYLEX、GEOSEARCHSTORE、GETEX、TOUCH；配置字母 d/m/n/o/c 接受但无事件；MOVE 事件在单库下不可达。
6. `CONFIG SET notify-keyspace-events` 非法字母文案未与真机核对。
7. 交付声明：Phase 7（keyspace 通知 KEA 全量发布点）、Phase 9（8 种逐出策略）完成；Phase 8（CLIENT TRACKING）、Phase 10（RDB/AOF）仍未做；剩余缺口命令：HEXPIRETIME/HPEXPIRETIME、MONITOR、SUBSTR、SWAPDB、COMMAND LIST、CONFIG REWRITE 等（报告沿用缺口清单口径）。

- [ ] **Step 1: 读 Task 1-8 全部 report，核对偏差清单完整性**
- [ ] **Step 2: 改 README.md**——TLS 移入已支持；功能列表加"Keyspace 通知（notify-keyspace-events KEA）"、"逐出策略 8 种"、新命令清单
- [ ] **Step 3: 改 CONTEXT.md**——修正 TLS 措辞；加"2026-10-10 批次"交付段与上述偏差清单
- [ ] **Step 4: 勾选 roadmap**——Phase 7、Phase 9 整段勾选；对应命令项勾选；Phase 8/10 与未交付命令保持 `- [ ]`
- [ ] **Step 5: 文档一致性自查**——rg 确认仓库内不再有"服务端 TLS 未实现"类措辞
- [ ] **Step 6: Commit**

```bash
git add README.md CONTEXT.md docs/superpowers/plans/2026-10-08-redis-gap-roadmap.md
git commit -m "docs(Phase7/9) 交付更新+TLS 过期措辞修正"
```

---

## 任务依赖与顺序

T1 →（独立）T2 → T3 → T4 → T5 → T6 → T7 → T8 → T9。
T3-T7 均依赖 T2 的 `Notify` 接口；T6/T7 的新命令事件在各自 task 内接入（接口签名见 T2 Interfaces）；T8 依赖 T6/T7 已收录的名单（审计不回退）；T9 最后汇总。**顺序不可调换**：先命令后审计会漏新命令的发布点（T3-T5 在前即为此设计）。
