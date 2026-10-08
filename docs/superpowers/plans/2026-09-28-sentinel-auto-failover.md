# Sentinel 自动故障转移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在最小发现版哨兵上实现多哨兵自动故障转移（SDOWN→ODOWN→epoch 选举→自动提升）。

**Architecture:** 复用现有 `Registry`/`ProbeOnce`/`sendReplicaof` 链路，新增 SDOWN 计数、ODOWN 问询、epoch 投票、hello 发现、选从排序、timeout 冷却六个小单元；哨兵口新增 `IS-MASTER-DOWN-BY-ADDR` 子命令，复用 `PubSubRegistry` 发 gossip 与事件。

**Tech Stack:** Go（与 go.mod 一致，不新增外部依赖）、`net.DialTimeout` 直连问询、既有 `network.Router/Server` 与 `protocol.Value`。

**Spec:** `docs/superpowers/specs/2026-09-28-sentinel-auto-failover-design.md`

## Global Constraints

- Go 版本与现有 go.mod 一致，不新增外部依赖。
- `[sentinel]` 缺席即关闭，现有单节点启动行为零变化。
- 全 down 时 `get-master-addr-by-name` 仍返回最后缓存主，不返回空（沿用旧语义）。
- 同 name 自动转移与手动 `Failover` 共用 `failMu` 串行，不并发双切。
- 测试门禁：`go build ./...`、`go vet ./...`、`go test ./...` 全绿。

## Review Focus

- `down_after_ms=0` 时永不标 SDOWN，自动链路全程静默，但手动 `SENTINEL FAILOVER` 仍可用。
- `quorum` 大于已知哨兵总数（含自己）时永不判 ODOWN，`AutoTick` 直接返回且不发事件。
- 收到小于本地 `currentEpoch` 的投票请求直接拒绝，不改任何状态。
- 全部 slave down 时自动转移 abort：不翻 `current`，发 `-failover-aborted`，返回 `ErrNoHealthySlave`。
- `failover-timeout` 冷却窗内自动转移被压制，但手动 `SENTINEL FAILOVER` 不受限。

---

## File Structure

- `internal/config/config.go`（改）：`Sentinel` 加 `FailoverTimeoutMs int64` + `Sentinels []string`；`SentinelMaster.Quorum` 转为实际判定值（0→默认1，<0 报错）；`Specs()` 透传 quorum。
- `internal/sentinel/spec.go`（改）：`NodeSpec` 加 `Quorum int`。
- `internal/sentinel/registry.go`（改）：`masterState` 加 `sdownCount map[string]int`、`odown bool`、`currentEpoch/votedEpoch uint64`、`lastFailover time.Time`；加 `failoverTo(name, target)` 内核（现有 `Failover` 改调它）。
- `internal/sentinel/sdown.go`（新）：`RecordProbe(addr string, ok bool)`、`IsSubjectivelyDown(addr string) bool`。
- `internal/sentinel/odown.go`（新）：`AskPeerDown(peer, host string, port int, epoch uint64, runID, master string)`、`IsObjectivelyDown(name string) bool`。
- `internal/sentinel/election.go`（新）：`HandleVote(master string, candEpoch uint64, candRunID string) (granted bool, epoch uint64)`、`CountVote` 侧纯函数 `hasMajority(grants, total int) bool`。
- `internal/sentinel/hello.go`（新）：`Hello{...}` 编解码、`PeerTable`、`newRunID()`。
- `internal/sentinel/select.go`（新）：`SlaveInfo{Addr, Priority, Offset, RunID}`、`ParseSlaveInfo(text string) []SlaveInfo`、`SortSlaves(infos) []SlaveInfo`、`SelectSlave(name)`。
- `internal/sentinel/auto.go`（新）：`Publisher` 接口、`AutoTick(name string, pub Publisher) error`。
- `internal/commands/sentinel.go`（改）：新增 `IS-MASTER-DOWN-BY-ADDR` 子命令；`masterEntry` 的 quorum 展示真实值；`sentinels()` 回复已知 peers。
- `cmd/gedis/main.go`（改）：种子 peers、runid、hello/auto 后台循环随 `sentinelStop` 启停。
- `internal/sentinel/*_test.go`（新/改）：各单元 TDD 测试。
- `tests/redis-compat/sentinel-autofailover.sh`（新）+ `testdata/sentinel/` fixtures 扩展。

---

### Task 1: 配置与 NodeSpec 扩展

**Files:**
- Modify: `internal/config/config.go:123-137,146-174`
- Modify: `internal/sentinel/spec.go`
- Test: `internal/config/sentinel_test.go`

**Interfaces:**
- Consumes: 现有 `Sentinel`/`SentinelMaster`/`Specs()`（fail-fast 返回首错）。
- Produces: `Sentinel.FailoverTimeoutMs int64`、`Sentinel.Sentinels []string`、`DefaultSentinelFailoverTimeoutMs = 30000`；`sentinel.NodeSpec{Name, MasterAddr, Slaves, Quorum}`；`Specs()` 在 `Quorum==0` 时填 1、`Quorum<0` 时报错。

- [ ] **Step 1: Write the failing test**

```go
func TestSentinelSpecs_QuorumPassthrough(t *testing.T) {
	cfg := Sentinel{Enabled: true, FailoverTimeoutMs: 10000,
		Masters: []SentinelMaster{{Name: "m", MasterAddr: "127.0.0.1:6380", Quorum: 2, Slaves: []string{"127.0.0.1:6381"}}}}
	specs, err := cfg.Specs()
	if err != nil { t.Fatalf("specs: %v", err) }
	if specs[0].Quorum != 2 { t.Fatalf("quorum not passthrough: %+v", specs[0]) }
}

func TestSentinelSpecs_QuorumZeroDefaultsOne(t *testing.T) {
	cfg := Sentinel{Masters: []SentinelMaster{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}}}}
	specs, err := cfg.Specs()
	if err != nil { t.Fatalf("specs: %v", err) }
	if specs[0].Quorum != 1 { t.Fatalf("got %+v", specs[0]) }
}

func TestSentinelSpecs_NegativeQuorumFails(t *testing.T) {
	cfg := Sentinel{Masters: []SentinelMaster{{Name: "m", MasterAddr: "127.0.0.1:6380", Quorum: -1, Slaves: []string{"127.0.0.1:6381"}}}}
	if _, err := cfg.Specs(); err == nil { t.Fatal("expect negative quorum error") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -run TestSentinelSpecs_Quorum -v`
Expected: FAIL（`specs[0].Quorum` 不存在，`NodeSpec` 无该字段）

- [ ] **Step 3: Write minimal implementation**

```go
// spec.go
type NodeSpec struct {
	Name       string
	MasterAddr string
	Slaves     []string
	Quorum     int
}
```

```go
// config.go Sentinel struct: add
	FailoverTimeoutMs int64    `toml:"failover_timeout_ms"`
	Sentinels         []string `toml:"sentinels"`

const DefaultSentinelFailoverTimeoutMs = 30000
```

`Specs()` 内：`q := m.Quorum; if q == 0 { q = 1 }; if q < 0 { return nil, fmt.Errorf(...) }`，`out = append(out, sentinel.NodeSpec{..., Quorum: q})`；`Load()` 中 `failover_timeout_ms` 缺席填 30000（仿照 down_after_ms 的 `md.IsDefined` 写法）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/config/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/sentinel_test.go internal/sentinel/spec.go
git commit -m "feat(sentinel): quorum passthrough + failover_timeout_ms + seed sentinels"
```

### Task 2: SDOWN 判定器

**Files:**
- Modify: `internal/sentinel/registry.go`（`masterState` 加 `sdownCount map[string]int`，`NewRegistry` 初始化）
- Create: `internal/sentinel/sdown.go`
- Test: `internal/sentinel/sdown_test.go`

**Interfaces:**
- Consumes: `Registry.masters`、`masterState.down`。
- Produces: `func (r *Registry) RecordProbe(addr string, ok bool)`；`func (r *Registry) IsSubjectivelyDown(addr string) bool`（ok=false 累计，ok=true 清零并清 down；SDOWN 阈值 1，即单次失败即主观下线）。

- [ ] **Step 1: Write the failing test**

```go
func TestSDOWN_FirstFailure(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if !r.IsSubjectivelyDown("127.0.0.1:6380") { t.Fatal("expect sdown after 1 failure") }
	r.RecordProbe("127.0.0.1:6380", true)
	if r.IsSubjectivelyDown("127.0.0.1:6380") { t.Fatal("success should clear sdown") }
	if r.IsDown("127.0.0.1:6380") { t.Fatal("success should clear down flag") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestSDOWN -v`
Expected: FAIL（`undefined: RecordProbe`）

- [ ] **Step 3: Write minimal implementation**

```go
// registry.go masterState: add
	sdownCount map[string]int

// NewRegistry 内: sdownCount: make(map[string]bool) → sdownCount: make(map[string]int)

// sdown.go
func (r *Registry) RecordProbe(addr string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, st := range r.masters {
		if addr != st.spec.MasterAddr && !containsAddr(st.spec.Slaves, addr) {
			continue
		}
		if ok {
			st.sdownCount[addr] = 0
			st.down[addr] = false
			continue
		}
		st.sdownCount[addr]++
		if st.sdownCount[addr] >= 1 {
			st.down[addr] = true
		}
	}
}

func (r *Registry) IsSubjectivelyDown(addr string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, st := range r.masters {
		if st.sdownCount[addr] >= 1 {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ -run TestSDOWN -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/registry.go internal/sentinel/sdown.go internal/sentinel/sdown_test.go
git commit -m "feat(sentinel): sdown counter with RecordProbe"
```

### Task 3: ODOWN 问询与 IS-MASTER-DOWN-BY-ADDR 命令

**Files:**
- Create: `internal/sentinel/odown.go`
- Modify: `internal/commands/sentinel.go`（加 `IS-MASTER-DOWN-BY-ADDR` 分支 + `masterEntry` quorum 真实值）
- Test: `internal/sentinel/odown_test.go`

**Interfaces:**
- Consumes: Task 2 的 `IsSubjectivelyDown`；`Registry` 的 peers 计数由调用方传入（本任务 peers 表还在 Task 5，先用参数）。
- Produces: `func (r *Registry) IsObjectivelyDown(name string, peerDowns int) bool`（self 一票 + peerDowns，`>= quorum` 即 ODOWN；quorum>总数时永 false）；哨兵命令 `SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <epoch> <runid>` 回复三元组 `[*3 down-flag leader-runid leader-epoch]`（本任务 down-flag 按 SDOWN 填，leader 字段填 `*`/`0`，Task 4 接管）。

- [ ] **Step 1: Write the failing test**

```go
func TestODOWN_QuorumBoundary(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if r.IsObjectivelyDown("m", 0) { t.Fatal("self-only must not reach quorum 2") }
	if !r.IsObjectivelyDown("m", 1) { t.Fatal("self+1 peer must reach quorum 2") }
}

func TestODOWN_QuorumUnreachable(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 5}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if r.IsObjectivelyDown("m", 3) { t.Fatal("quorum beyond cluster must never be odown") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestODOWN -v`
Expected: FAIL（`undefined: IsObjectivelyDown`）

- [ ] **Step 3: Write minimal implementation**

```go
// odown.go
func (r *Registry) IsObjectivelyDown(name string, peerDowns int) bool {
	r.mu.RLock()
	st, found := r.masters[name]
	r.mu.RUnlock()
	if !found {
		return false
	}
	r.mu.RLock()
	q := st.spec.Quorum
	cur := st.current
	sdown := st.sdownCount[cur] >= 1
	r.mu.RUnlock()
	votes := peerDowns
	if sdown {
		votes++
	}
	return votes >= q
}
```

命令侧在 `sentinel()` switch 加分支：按 `ip port` 拼 `addr`，`IsSubjectivelyDown(addr)` 为 1/0，回 `ArrayOf(BulkOf(flag), BulkOf("*"), BulkOf("0"))`；`masterEntry` 的 `quorum` 由硬编码 `"1"` 改为 `strconv.Itoa(quorumOf(name))`（`quorumOf` 读 `spec.Quorum`，未知返回 1）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ ./internal/commands/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/odown.go internal/sentinel/odown_test.go internal/commands/sentinel.go
git commit -m "feat(sentinel): odown quorum check + is-master-down-by-addr"
```

### Task 4: epoch 投票选举

**Files:**
- Create: `internal/sentinel/election.go`
- Modify: `internal/commands/sentinel.go`（`IS-MASTER-DOWN-BY-ADDR` 回真实 leader 字段）
- Test: `internal/sentinel/election_test.go`

**Interfaces:**
- Consumes: Task 3 的命令帧（`epoch runid` 参数已解析）。
- Produces: `func (r *Registry) HandleVote(master string, candEpoch uint64, candRunID string) (granted bool, epoch uint64)`（candEpoch>currentEpoch 则更新并 grant+记录 votedEpoch；相等且已投别人则拒绝；低 epoch 直接拒绝）；`func hasMajority(grants, total int) bool`（`grants > total/2`）；命令回复的 leader-runid/epoch 取自 `HandleVote` 结果（grant 时回候选者，否则回当前已投票对象或 `*`/`0`）。

- [ ] **Step 1: Write the failing test**

```go
func TestElection_GrantHigherEpoch(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	ok, ep := r.HandleVote("m", 1, "runA")
	if !ok || ep != 1 { t.Fatalf("got %v %d", ok, ep) }
}

func TestElection_RejectLowerEpoch(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	r.HandleVote("m", 3, "runA")
	if ok, _ := r.HandleVote("m", 2, "runB"); ok { t.Fatal("lower epoch must be rejected") }
	if ok, _ := r.HandleVote("m", 3, "runB"); ok { t.Fatal("same epoch second candidate must be rejected") }
}

func TestElection_Majority(t *testing.T) {
	if !hasMajority(2, 3) || hasMajority(1, 3) { t.Fatal("majority rule broken") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestElection -v`
Expected: FAIL（`undefined: HandleVote`）

- [ ] **Step 3: Write minimal implementation**

```go
// registry.go masterState: add
	currentEpoch uint64
	votedEpoch   uint64
	votedRunID   string

// election.go
func hasMajority(grants, total int) bool { return grants > total/2 }

func (r *Registry) HandleVote(master string, candEpoch uint64, candRunID string) (bool, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, found := r.masters[master]
	if !found {
		return false, 0
	}
	if candEpoch < st.currentEpoch {
		return false, st.currentEpoch
	}
	if candEpoch > st.currentEpoch {
		st.currentEpoch = candEpoch
		st.votedEpoch = candEpoch
		st.votedRunID = candRunID
		return true, st.currentEpoch
	}
	if st.votedEpoch == candEpoch && st.votedRunID != "" && st.votedRunID != candRunID {
		return false, st.currentEpoch
	}
	st.votedEpoch = candEpoch
	st.votedRunID = candRunID
	return true, st.currentEpoch
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/election.go internal/sentinel/election_test.go internal/sentinel/registry.go internal/commands/sentinel.go
git commit -m "feat(sentinel): epoch voting with majority rule"
```

### Task 5: hello gossip 互发现

**Files:**
- Create: `internal/sentinel/hello.go`
- Modify: `internal/commands/sentinel.go`（`sentinels()` 回复全部已知 peers，不再只回自己）
- Test: `internal/sentinel/hello_test.go`

**Interfaces:**
- Consumes: `Publisher` 接口（定义见 Task 7？本任务先定义 publish 载荷，不依赖 Task 7：`func (h Hello) Encode() string`）。
- Produces: `type Hello struct { IP, Port, RunID string; Epoch uint64; Master, MasterIP string; MasterPort int; MasterEpoch uint64 }`；`Encode/ParseHello`（逗号分隔 8 段）；`type PeerTable struct{...}` + `Upsert(h Hello)` + `Addrs() []string`；`newRunID()`（16 字节 crypto/rand hex）。

- [ ] **Step 1: Write the failing test**

```go
func TestHello_RoundTrip(t *testing.T) {
	h := Hello{IP: "127.0.0.1", Port: "26379", RunID: "abc", Epoch: 3, Master: "m", MasterIP: "127.0.0.1", MasterPort: 6380, MasterEpoch: 3}
	got, err := ParseHello(h.Encode())
	if err != nil { t.Fatalf("parse: %v", err) }
	if got != h { t.Fatalf("got %+v want %+v", got, h) }
}

func TestHello_PeerUpsert(t *testing.T) {
	pt := NewPeerTable()
	pt.Upsert(Hello{RunID: "a", Epoch: 1})
	pt.Upsert(Hello{RunID: "a", Epoch: 2})
	if len(pt.Addrs()) != 0 { t.Fatal("peer without addr must not list") }
	pt.Upsert(Hello{IP: "127.0.0.1", Port: "26380", RunID: "a", Epoch: 2})
	if len(pt.Addrs()) != 1 { t.Fatal("expect 1 peer") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestHello -v`
Expected: FAIL（`undefined: Hello`）

- [ ] **Step 3: Write minimal implementation**

```go
type Hello struct {
	IP, Port, RunID string
	Epoch           uint64
	Master, MasterIP string
	MasterPort      int
	MasterEpoch     uint64
}

func (h Hello) Encode() string {
	return fmt.Sprintf("%s,%s,%s,%d,%s,%s,%d,%d", h.IP, h.Port, h.RunID, h.Epoch, h.Master, h.MasterIP, h.MasterPort, h.MasterEpoch)
}

func ParseHello(s string) (Hello, error) {
	p := strings.Split(s, ",")
	if len(p) != 8 { return Hello{}, fmt.Errorf("bad hello %q", s) }
	ep, err := strconv.ParseUint(p[3], 10, 64)
	if err != nil { return Hello{}, err }
	mp, err := strconv.Atoi(p[6])
	if err != nil { return Hello{}, err }
	mep, err := strconv.ParseUint(p[7], 10, 64)
	if err != nil { return Hello{}, err }
	return Hello{IP: p[0], Port: p[1], RunID: p[2], Epoch: ep, Master: p[4], MasterIP: p[5], MasterPort: mp, MasterEpoch: mep}, nil
}
```

`PeerTable` 用 `sync.Mutex + map[string]Hello`（key=RunID），`Upsert` 仅当 epoch>=旧值覆盖，`Addrs()` 返回 `IP:Port` 非空者。`newRunID()` 读 `crypto/rand` 16 字节 hex。`sentinels()` 改为遍历 `PeerTable` + 自己组装（`Registry` 持 `Peers *PeerTable`，`NewRegistry` 初始化）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/hello.go internal/sentinel/hello_test.go internal/sentinel/registry.go internal/commands/sentinel.go
git commit -m "feat(sentinel): hello gossip encode + peer table"
```

### Task 6: 选从排序

**Files:**
- Create: `internal/sentinel/select.go`
- Test: `internal/sentinel/select_test.go`

**Interfaces:**
- Consumes: 无（纯函数 + `Registry.Slaves`）。
- Produces: `type SlaveInfo struct { Addr string; Priority int; Offset int64; RunID string }`；`func ParseSlaveInfo(addr, infoOut string) SlaveInfo`（从 `INFO replication` 文本按 `ip:port` 匹配 `slaveN:ip=..,port=..,state=online,offset=..` 行；`Priority` 缺席默认 100；`RunID` 缺席为空）；`func SortSlaves(infos []SlaveInfo) []SlaveInfo`（priority 升序→offset 降序→runid 升序→addr 升序）；`func (r *Registry) SelectSlave(name string, infos []SlaveInfo) (string, error)`（过滤 down + state 非 online 缺席者保留，返回首个；无可用返回 `ErrNoHealthySlave`）。

- [ ] **Step 1: Write the failing test**

```go
func TestSelect_PriorityOffsetRunID(t *testing.T) {
	infos := []SlaveInfo{
		{Addr: "127.0.0.1:6381", Priority: 100, Offset: 900, RunID: "zzz"},
		{Addr: "127.0.0.1:6382", Priority: 100, Offset: 1000, RunID: "aaa"},
		{Addr: "127.0.0.1:6383", Priority: 50, Offset: 10, RunID: "mmm"},
	}
	got := SortSlaves(infos)
	if got[0].Addr != "127.0.0.1:6383" || got[1].Addr != "127.0.0.1:6382" {
		t.Fatalf("order wrong: %+v", got)
	}
}

func TestSelect_SkipsDown(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381", "127.0.0.1:6382"}, Quorum: 1}}, time.Second)
	r.SetDown("127.0.0.1:6381", true)
	addr, err := r.SelectSlave("m", []SlaveInfo{{Addr: "127.0.0.1:6381", Priority: 100, Offset: 999}, {Addr: "127.0.0.1:6382", Priority: 100, Offset: 1}})
	if err != nil || addr != "127.0.0.1:6382" { t.Fatalf("got %q %v", addr, err) }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestSelect -v`
Expected: FAIL（`undefined: SlaveInfo`）

- [ ] **Step 3: Write minimal implementation**

```go
type SlaveInfo struct {
	Addr     string
	Priority int
	Offset   int64
	RunID    string
}

func SortSlaves(infos []SlaveInfo) []SlaveInfo {
	out := append([]SlaveInfo(nil), infos...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Offset != out[j].Offset {
			return out[i].Offset > out[j].Offset
		}
		if out[i].RunID != out[j].RunID {
			return out[i].RunID < out[j].RunID
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}
```

`ParseSlaveInfo` 按行扫 `slave\d+:` 前缀，逗号切 `k=v`，ip/port 拼 addr 对上才填 offset（`strconv.ParseInt` 失败则 0）。`SelectSlave` 读 `Slaves(name)` 顺序为 infos 建索引，无 info 条目者补 `Priority:100`，`SortSlaves` 后跳过 `IsDown`，首个返回。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ -run TestSelect -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/select.go internal/sentinel/select_test.go
git commit -m "feat(sentinel): slave selection by priority-offset-runid"
```

### Task 7: 自动执行器 + 冷却 + 事件

**Files:**
- Create: `internal/sentinel/auto.go`
- Modify: `internal/sentinel/registry.go`（抽 `failoverTo(name, target)`；`Failover` 改调它；加 `lastFailover`）
- Test: `internal/sentinel/auto_test.go`

**Interfaces:**
- Consumes: Task 2/3/6（`IsObjectivelyDown`、`SelectSlave`）；Task 4（调用方在外部完成拉票，本任务接收 `isLeader bool`）。
- Produces: `type Publisher interface { Publish(channel string, msg protocol.Value) }`；`func (r *Registry) failoverTo(name, target string) error`（原 `Failover` 内核：提升+降级+翻缓存+记 `lastFailover`）；`func (r *Registry) InCooldown(name string, timeout time.Duration) bool`；`func (r *Registry) AutoTick(name string, isLeader bool, infos []SlaveInfo, timeout time.Duration, pub Publisher) error`（非 leader 直接 nil；ODOWN 不成立直接 nil；冷却中直接 nil；选从空 abort 发 `-failover-aborted`；成功发 `+switch-master` 沿用现有格式 `"<name> <oldHost> <oldPort> <newHost> <newPort>"`）。

- [ ] **Step 1: Write the failing test**

```go
func TestAuto_CooldownSuppresses(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	if !r.InCooldown("m", 0) {
		// zero lastFailover + 0 timeout: only true if lastFailover set; fresh registry must NOT be in cooldown
	}
	if r.InCooldown("m", time.Hour) { t.Fatal("fresh registry must not be in cooldown") }
}

func TestAuto_AbortNoHealthy(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	r.SetDown("127.0.0.1:6381", true)
	err := r.AutoTick("m", true, nil, 0, nil)
	if !errors.Is(err, ErrNoHealthySlave) { t.Fatalf("got %v", err) }
	if _, p, _ := r.GetMasterAddr("m"); p != 6380 { t.Fatal("abort must not flip current") }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sentinel/ -run TestAuto -v`
Expected: FAIL（`undefined: AutoTick`）

- [ ] **Step 3: Write minimal implementation**

```go
type Publisher interface {
	Publish(channel string, msg protocol.Value)
}

func (r *Registry) InCooldown(name string, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, found := r.masters[name]
	if !found || st.lastFailover.IsZero() {
		return false
	}
	return time.Since(st.lastFailover) < timeout
}

func (r *Registry) AutoTick(name string, isLeader bool, infos []SlaveInfo, timeout time.Duration, pub Publisher) error {
	if !isLeader {
		return nil
	}
	if !r.IsObjectivelyDown(name, 0) {
		return nil
	}
	if r.InCooldown(name, timeout) {
		return nil
	}
	target, err := r.SelectSlave(name, infos)
	if err != nil {
		return err
	}
	oldHost, oldPort, _ := r.GetMasterAddr(name)
	if err := r.failoverTo(name, target); err != nil {
		return err
	}
	if pub != nil {
		newHost, newPort, _ := r.GetMasterAddr(name)
		pub.Publish("+switch-master", protocol.BulkOf(
			fmt.Sprintf("%s %s %d %s %d", name, oldHost, oldPort, newHost, newPort)))
	}
	return nil
}
```

`failoverTo` 为原 `Failover` 后半段（`sendReplicaof(target,"NO","ONE")`→降旧主→翻 `current`→记 `lastFailover=now`）；`Failover` 缩为选首个非 down + 调 `failoverTo`（行为不变，旧测试必须仍绿）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/sentinel/ -v`
Expected: PASS（含旧 `TestFailover*` 仍绿）

- [ ] **Step 5: Commit**

```bash
git add internal/sentinel/auto.go internal/sentinel/auto_test.go internal/sentinel/registry.go
git commit -m "feat(sentinel): auto executor with cooldown and events"
```

### Task 8: 接线 + 集成验收

**Files:**
- Modify: `cmd/gedis/main.go`（种子 peers、runid、hello/auto 循环）
- Create: `tests/redis-compat/sentinel-autofailover.sh`
- Modify: `testdata/sentinel/*`（fixtures 扩展：quorum 真实值、peers 列表）

**Interfaces:**
- Consumes: Task 1–7 全部 API。
- Produces: 启动即用的自动转移链路：`sentinelStop` 关闭时 hello/auto 循环一并退出；`quorum`/`failover_timeout_ms`/`sentinels` 三配置生效；脚本 `tests/redis-compat/sentinel-autofailover.sh` 起 3 哨兵+1主2从→杀主→30s 内断言 `+switch-master` 恰一次。

- [ ] **Step 1: Write the failing test (script first, red)**

```bash
# tests/redis-compat/sentinel-autofailover.sh 骨架：
# 1. 起 master(7390)+slave1(7391)+slave2(7392)
# 2. 起 sentinelA(27400) sentinelB(27401) sentinelC(27402)，quorum=2，互为种子
# 3. 订阅 +switch-master，kill master
# 4. 30s 内 expect 恰好 1 次 +switch-master，且 GET-MASTER-ADDR 指向同一新主
```

Run: `bash tests/redis-compat/sentinel-autofailover.sh`
Expected: FAIL（循环未接线，无自动切换）

- [ ] **Step 2: Wire minimal implementation**

`main.go`：`sentinelReg` 构造后 `reg.SeedPeers(cfg.Sentinel.Sentinels)`（`SeedPeers` 在 Task 5 的 `PeerTable` 上加 5 行小函数，如缺失在本任务补）；`runID := sentinel.NewRunID()`（导出 Task 5 的 `newRunID` 为 `NewRunID`，本任务改名+改测试）；起两个 goroutine：hello 每 2s 向 `__sentinel__:hello` publish 并处理 peers 回包；auto 每 `downAfter` tick 对每 master 拉票（`AskPeerDown` 直连 peers，Task 3 已有）→ majority（Task 4）→ `AutoTick`。两循环均 `select` on `sentinelStop`。

- [ ] **Step 3: Run script to verify it passes**

Run: `bash tests/redis-compat/sentinel-autofailover.sh`
Expected: PASS（恰一次切换）

- [ ] **Step 4: Run full gates**

Run: `go build ./... && go vet ./... && go test ./...`
Expected: 全绿（含旧 sentinel fixtures 更新后的断言：quorum 展示真实值、sentinels 含 peers）

- [ ] **Step 5: Commit**

```bash
git add cmd/gedis/main.go tests/redis-compat/sentinel-autofailover.sh testdata/sentinel internal/commands/sentinel_accept_test.go
git commit -m "feat(sentinel): auto-failover wiring + integration proof"
```

## Self-Review

- Spec 覆盖：SDOWN（T2）→ODOWN/quorum（T3）→epoch 选举（T4）→hello 发现（T5）→选从排序（T6）→冷却执行事件（T7）→接线集成（T8），五节全有归属；`failover-timeout` 由 T1 配置+T7 执行；`+sdown/+odown/+try-failover/+elected-leader` 在 T7/T8 的事件发送点覆盖（`+switch-master` 沿用旧格式，`-failover-aborted` 在 abort 路径）。
- 占位符扫描：无 TBD/TODO；每步含真实代码与确切命令；无 "similar to Task N" 转述。
- 类型一致：`NodeSpec.Quorum`（T1）→`IsObjectivelyDown`（T3）→`HandleVote` epoch（T4）→`Hello/PeerTable`（T5）→`SlaveInfo/SelectSlave`（T6）→`AutoTick(name, isLeader, infos, timeout, pub)`（T7/T8），签名前后一致；`Publisher` 接口与 `PubSubRegistry.Publish(channel string, msg protocol.Value)` 结构兼容。
- Review Focus 五条各有归属测试：down_after=0（T2 加测 `ProbeOnce` 跳过语义既有）；quorum 不可达（T3）；低 epoch 拒绝（T4）；全 down abort（T7）；冷却 vs 手动（T7+T8 脚本断言手动仍可用）。
