# Cluster Slot 迁移 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 Redis 7.2.6 一致的单 key MIGRATE + SETSLOT 三态迁移（含落盘恢复与 ASK/MOVED 按存在性分流）。

**Architecture:** Topology 新增 migrating/importing 双态 map + epoch，SETSLOT 写内存即落盘快照、启动重放；intercept 按存在性分流 ASK/MOVED；MIGRATE 复用 lookupRaw(DUMP 语义)→TCP 直连目标 RESTORE→成功非 COPY 则 DEL。

**Tech Stack:** Go（现有 go.mod 工具链），Pebble 存储（storage.Pebble），TCP 直连（net.DialTimeout + RESP），无新依赖。

**Spec:** docs/superpowers/specs/2026-09-27-cluster-resharding-design.md

## Global Constraints

- 对标 Redis 7.2.6 真机措辞与流程，单 key 语义一致。
- MIGRATE 单 key 全参：COPY / REPLACE / AUTH password / AUTH2 user pass / timeout。
- SETSLOT 写内存即落盘（nodes.conf 式快照：slots 归属 + 双态 + epoch），启动重放恢复。
- 不做：多 key KEYS 扩展、在线批量快照自创协议、MEET/RESET/REPLICATE 从角色（仍拒绝）。
- 测试：TDD 单测 + testdata/cluster/ 真机 7.2.6 fixtures + 双节点 redis-cli 验收 + -race 干净。

## Review Focus

- STABLE 在对端仍有 IMPORTING 残留时，源端清槽后目标残留是否导致 ASK 死循环 — 期望目标 IMPORTING 无源 MIGRATING 时按普通 owned 槽处理（MOVED/本地执行，不发 ASK）。
- MIGRATE 超时杀连接后源端 key 必须保留 — 期望超时/IO 错误一律不 DEL，返回 IOERR 系措辞。
- REPLACE 缺席且目标已存在时必须报错且不覆盖 — 期望返回目标已存在错误，原值不变。
- COPY 带超时成功后源必须留、目标必须有 — 期望 COPY 永不 DEL。
- 重启恰在 NODE 切换后崩溃，落盘 epoch 是否单调 — 期望重放后归属为 NODE 目标、双态清空、epoch 不回退。

## File Structure

- Modify: `internal/cluster/topo.go` — Topology 新增 migrating/importing/epoch + RWMutex，SetMigrating/SetImporting/SetStable/SetNode + Snapshot/LoadSnapshot + MigratingTo/ImportingFrom 查询。
- Create: `internal/cluster/topo_migrate_test.go` — 双态状态机 + 快照重放单测。
- Modify: `internal/commands/cluster.go` — clusterHandler 新增 persistPath/persist 回调，注册 CLUSTER SETSLOT，intercept 按存在性分流 ASK/MOVED。
- Create: `internal/commands/migrate.go` — MIGRATE 解析（host/port/key/db/timeout/COPY/REPLACE/AUTH/AUTH2）+ DUMP→TCP RESTORE→DEL 流程。
- Create: `internal/commands/migrate_test.go`、`internal/commands/setslot_test.go` — SETSLOT 三态 + MIGRATE 全参单测（含 fake 目标 TCP server）。
- Modify: `cmd/gedis/main.go` — cluster 快照路径为 `<dataDir>/nodes.conf`，启动 LoadSnapshot 重放，SETSLOT 回调落盘。
- Create: `tests/redis-compat/cluster-migrate.sh` — 真机 7.2.6 互证（ASK/MOVED/MIGRATE 全参/COPY/REPLACE/超时）。

---

### Task 1: Topology 迁移双态 + epoch + 快照重放

**Files:**
- Modify: `internal/cluster/topo.go:11-17`
- Create: `internal/cluster/topo_migrate_test.go`
- Test: `internal/cluster/topo_migrate_test.go`

**Interfaces:**
- Consumes: 现有 `Topology{owned, owner, self, nodes}`、`NumSlots`、`Build`。
- Produces: `func (t *Topology) SetMigrating(slot int, targetID string) error`、`func (t *Topology) SetImporting(slot int, sourceID string) error`、`func (t *Topology) SetStable(slot int)`、`func (t *Topology) SetNode(slot int, nodeID string) error`、`func (t *Topology) MigratingTo(slot int) (string, bool)`、`func (t *Topology) ImportingFrom(slot int) (string, bool)`、`func (t *Topology) Epoch() uint64`、`func (t *Topology) Snapshot() Snapshot`、`func (t *Topology) LoadSnapshot(Snapshot) error`，其中 `type Snapshot struct { Owner [NumSlots]int; Migrating map[int]string; Importing map[int]string; Epoch uint64 }`。Task 2/3 依赖这些签名，不可改名。

- [ ] **Step 1: Write the failing test**

```go
package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Topology_when_MigratingState(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	slot := 100
	require.NoError(t, topo.SetMigrating(slot, "nodeB"))
	got, ok := topo.MigratingTo(slot)
	require.True(t, ok)
	require.Equal(t, "nodeB", got)
	topo.SetStable(slot)
	_, ok = topo.MigratingTo(slot)
	require.False(t, ok)
}

func Test_Topology_when_SnapshotRoundTrip(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	require.NoError(t, topo.SetMigrating(100, "nodeB"))
	snap := topo.Snapshot()
	require.Equal(t, uint64(1), snap.Epoch)
	topo2, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	require.NoError(t, topo2.LoadSnapshot(snap))
	got, ok := topo2.MigratingTo(100)
	require.True(t, ok)
	require.Equal(t, "nodeB", got)
	stale := snap
	stale.Epoch = 0
	require.Error(t, topo2.LoadSnapshot(stale))
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/cluster/ -run 'Test_Topology_when_MigratingState|Test_Topology_when_SnapshotRoundTrip' -v`
Expected: FAIL with "undefined: SetMigrating / MigratingTo / Snapshot"

- [ ] **Step 3: Write minimal implementation**

在 `internal/cluster/topo.go` 的 Topology struct 新增字段（RWMutex 保护），全部方法加锁；校验规则：slot 越界报错，Migrating 仅源端 owner 自检（owner[slot]==self 否则报错），Importing 仅目标端自检（owner[slot]!=self 否则报错），幂等成功（重复设相同 target 直接返回 nil），SetNode 做归属原子切换 + 清本槽双态 + epoch++，SetStable 清本槽双态（不碰归属，epoch 不变）：

```go
type Snapshot struct {
	Owner     [NumSlots]int
	Migrating map[int]string
	Importing map[int]string
	Epoch     uint64
}
```

```go
func (t *Topology) SetMigrating(slot int, targetID string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if slot < 0 || slot >= NumSlots {
		return fmt.Errorf("slot out of range")
	}
	if t.owner[slot] != t.self {
		return fmt.Errorf("not owner")
	}
	if cur, ok := t.migrating[slot]; ok && cur == targetID {
		return nil
	}
	if t.migrating == nil {
		t.migrating = map[int]string{}
	}
	t.migrating[slot] = targetID
	t.epoch++
	return nil
}
```

Importing/SetStable/SetNode/MigratingTo/ImportingFrom/Epoch/Snapshot/LoadSnapshot 同构实现（LoadSnapshot 全量覆盖 owner+migrating+importing+epoch，epoch 不回退：若 snap.Epoch < t.epoch 则报错）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/cluster/ -v -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/cluster/topo.go internal/cluster/topo_migrate_test.go
git commit -m "feat(cluster): topology migrating/importing dual-state with epoch snapshot"
```

### Task 2: CLUSTER SETSLOT 三态机 + 落盘重放接线

**Files:**
- Modify: `internal/commands/cluster.go:1-60`
- Create: `internal/commands/setslot_test.go`
- Modify: `cmd/gedis/main.go:106-121`
- Test: `internal/commands/setslot_test.go`

**Interfaces:**
- Consumes: Task 1 的 `SetMigrating/SetImporting/SetStable/SetNode/MigratingTo/ImportingFrom`。
- Produces: `CLUSTER SETSLOT <slot> MIGRATING <node-id> | IMPORTING <node-id> | STABLE | NODE <node-id>` 注册；`clusterHandler.persist func(Snapshot) error` 回调字段；`LoadSnapshot` 在 main.go 启动时重放。Task 3 依赖 SETSLOT 已落盘。

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Setslot_when_MigratingThenStable(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	slot := cluster.Slot("mkey1")
	_ = slot
	got := dispatch(r, "CLUSTER", "SETSLOT", "100", "MIGRATING", "nodeB")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	got = dispatch(r, "CLUSTER", "SETSLOT", "100", "STABLE")
	require.Equal(t, "OK", got.S)
}

func Test_Setslot_when_NotOwnerFails(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	got := dispatch(r, "CLUSTER", "SETSLOT", "6000", "MIGRATING", "nodeC")
	require.Equal(t, protocol.KindError, got.Kind)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/commands/ -run 'Test_Setslot_when' -v`
Expected: FAIL（SETSLOT 未注册 → unknown command 或 wrong number of args）

- [ ] **Step 3: Write minimal implementation**

在 `internal/commands/cluster.go` 的 clusterCommands 表追加 `{"SETSLOT", c.setslot}`（acl.Meta Name SETSLOT、Category cluster、ReadOnly false），实现 setslot：参数个数校验（3 或 4），slot 整型 0-16383 校验，四个分支分别调 topo.SetMigrating/SetImporting/SetStable/SetNode，成功后调 c.persist（nil 则跳过，单测不落盘），失败返回 `ERR ...`（沿用现有 cluster.go 的 errReply 风格，措辞与 7.2.6 对齐：slot out of range / not owner / unknown node）；NODE 分支需将 nodeID 解析为 topo 节点（遍历 Nodes() 比对 ID，找不到报错）。

`cmd/gedis/main.go` 接线：clusterTopo 构建成功后，若 `cfg.Storage.DataDir != ""` 则 `snapPath := filepath.Join(cfg.Storage.DataDir, "nodes.conf")`，先 `LoadSnapshot(readFile)`（文件缺席跳过），再 `clusterH.SetPersist(func(snap) error { return os.WriteFile(snapPath, encode(snap), 0644) })`；encode 为 `slot ownerID migrating targetID importing sourceID epoch` 文本行（复用 ParseSlotRanges 逆操作，不引入新依赖）。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/commands/ -run 'Test_Setslot_when|Test_Cluster_when' -v -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/commands/cluster.go internal/commands/setslot_test.go cmd/gedis/main.go
git commit -m "feat(cluster): CLUSTER SETSLOT state machine with snapshot persist"
```

### Task 3: intercept ASK/MOVED 按存在性分流 + CheckExec 同槽

**Files:**
- Modify: `internal/commands/cluster.go:150-260`
- Modify: `internal/commands/setslot_test.go`
- Test: `internal/commands/setslot_test.go`

**Interfaces:**
- Consumes: Task 1 双态查询 + 现有 `keysInSlotNames`、`allUserKeys`、`lookupKey`、`AskRegistry`。
- Produces: intercept 新分支行为（源 MIGRATING 有 key 放行/无 key 发 ASK；目标 IMPORTING 需 ASKING+keyPresent 放行否则 MOVED）。无新导出符号，Task 4 的 MIGRATE 直接受益（迁移中读写不误杀）。

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Intercept_when_MigratingKeyPresentPasses(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	key := "askkey-present"
	slot := cluster.Slot(key)
	require.Less(t, slot, 5461)
	require.NoError(t, dispatchSetSlot(r, slot, "MIGRATING", "nodeB"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", key, "v"))
	require.Equal(t, "v", dispatch(r, "GET", key).BulkString())
}

func Test_Intercept_when_MigratingKeyMissingAsks(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	slot := cluster.Slot("askkey-missing-xyz")
	require.Less(t, slot, 5461)
	require.NoError(t, dispatchSetSlot(r, slot, "MIGRATING", "nodeB"))
	got := dispatch(r, "GET", "askkey-missing-xyz")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "ASK ")
}
```

其中 `dispatchSetSlot` 为测试 helper：`func dispatchSetSlot(r *network.Router, slot int, args ...string) error`，内部 `dispatch(r, append([]string{"CLUSTER","SETSLOT",strconv.Itoa(slot)}, args...)...)` 并断言 OK（与 cluster_test.go 的 dispatchConn/dispatch 风格一致）。TRYAGAIN 本计划不触发（无后台搬槽任务；迁移由 MIGRATE 同步完成），措辞位预留。

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/commands/ -run 'Test_Intercept_when_Migrating' -v`
Expected: FAIL（MIGRATING 有 key 被 MOVED 误杀，或无 key 未发 ASK）

- [ ] **Step 3: Write minimal implementation**

在 `intercept` 的 owned-缺席分支前插入双态分支（伪码，变量名与现有 cluster.go 一致）：

```go
if target, ok := c.topo.MigratingTo(slot); ok {
	if _, err := lookupKey(ctx, c.kv, key); err == nil {
		return false, protocol.Value{} // 本地有 key：放行执行
	}
	return true, protocol.Value{Kind: protocol.KindError, S: "ASK " + strconv.Itoa(slot) + " " + targetAddr}
}
if src, ok := c.topo.ImportingFrom(slot); ok {
	_ = src
	if c.asking.Consume(ctx) {
		if _, err := lookupKey(ctx, c.kv, key); err == nil {
			return false, protocol.Value{}
		}
	}
	// 无 ASKING 或本地无 key：沿用现有 MOVED 逻辑（owner addr）
}
```

targetAddr 由 targetID 查 Nodes() 换 Addr（找不到则回退现有 MOVED）。CheckExec 保持 CROSSSLOT（多 key 不同槽拒绝），不因迁移态放宽。Review Focus 第一行在此步用例覆盖：STABLE 清源后目标残留 IMPORTING 时，`ImportingFrom` 分支要求 ASKING+keyPresent，否则走 MOVED，不死循环。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/commands/ -run 'Test_Intercept_when|Test_Setslot_when|Test_Cluster_when' -v -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/commands/cluster.go internal/commands/setslot_test.go
git commit -m "feat(cluster): intercept ASK/MOVED by key presence during migration"
```

### Task 4: MIGRATE 单 key 全参（DUMP→TCP RESTORE→DEL）

**Files:**
- Create: `internal/commands/migrate.go`
- Create: `internal/commands/migrate_test.go`
- Test: `internal/commands/migrate_test.go`

**Interfaces:**
- Consumes: `lookupKey`（DUMP 语义：返回 datastruct.Entry{Type,Expiry,Payload}）、`KV.Delete`、`cluster.Slot`、`protocol`、`network.Router`。
- Produces: `func RegisterMigrate(r *network.Router, kv KV)`，在 main.go 与 openClusterSetup 中与 RegisterCluster 并列调用；`MIGRATE host port key db timeout [COPY] [REPLACE] [AUTH password | AUTH2 user pass]` 全参解析。Task 5 依赖此命令做真机互证。

- [ ] **Step 1: Write the failing test**

```go
package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Migrate_when_CopyKeepsSource(t *testing.T) {
	r, store := openTestSetup(t)
	_ = store
	fake := startFakeRestoreServer(t, nil)
	require.Equal(t, "OK", dispatch(r, "SET", "mg1", "v1").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg1", "0", "1000", "COPY")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	require.Equal(t, "v1", dispatch(r, "GET", "mg1").BulkString())
	require.True(t, fake.GotRestore)
}

func Test_Migrate_when_NoKeyReportsNoKey(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServer(t, nil)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg-missing", "0", "1000")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOKEY")
}

func Test_Migrate_when_TargetExistsWithoutReplaceFails(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServer(t, []string{"BUSYKEY"})
	require.Equal(t, "OK", dispatch(r, "SET", "mg2", "v2").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg2", "0", "1000")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "BUSYKEY")
}
```

`startFakeRestoreServer(t, existing []string)` 为本文件测试 helper：net.Listen 127.0.0.1:0，RESP 解析 RESTORE key ttl payload [REPLACE]，existing 命中且无 REPLACE 回 `BUSYKEY Target key name already exists.`，否则回 `+OK` 并置 GotRestore；Port 为实际端口字符串。

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/commands/ -run 'Test_Migrate_when' -v`
Expected: FAIL with "unknown command 'migrate'"

- [ ] **Step 3: Write minimal implementation**

`internal/commands/migrate.go`（120 行内）：parse 参数（host/port/key/db/timeout 必选，db 必须为 0 否则 `ERR ...`，timeout 整型>0 否则报错；COPY/REPLACE/AUTH/AUTH2 大小写不敏感，AUTH2 需两个参数，AUTH 与 AUTH2 互斥，多余参数报错）；`lookupKey` 取 Entry（notFound → `NOKEY No such key`，7.2.6 措辞）；TTL 由 Entry.Expiry 折算毫秒（无过期则 0）；`net.DialTimeout(tcp, host:port, timeout)`，先发 AUTH（`AUTH pass` 或 `AUTH user pass`），再发 `RESTORE key ttl payload [REPLACE]`（payload 为 Entry.Payload 原字节，RESP bulk 发送，不做二次编码）；读目标回复：BUSYKEY → 返回错误且不 DEL；+OK → 非 COPY 则 `kv.Delete(prefix+key)`（prefix 由 lookupRaw 返回的实际 raw key 推导，不假设 s:），返回 `+OK`；超时/IO → `IOERR ...` 且不 DEL。Review Focus 第 2/3/4 行由本任务三单测覆盖。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./internal/commands/ -run 'Test_Migrate_when' -v -race`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/commands/migrate.go internal/commands/migrate_test.go
git commit -m "feat(cluster): MIGRATE single-key with COPY REPLACE AUTH timeout"
```

### Task 5: 主链路接线 + 真机互证 + 全量回归

**Files:**
- Modify: `cmd/gedis/main.go:84-121`
- Modify: `internal/commands/cluster_test.go:16-24`（openClusterSetup 并调 RegisterMigrate）
- Create: `tests/redis-compat/cluster-migrate.sh`
- Test: 全量 `go test ./... -race`

**Interfaces:**
- Consumes: Task 2 的 persist 回调、Task 4 的 RegisterMigrate。
- Produces: 可验收的双节点迁移链路；无新接口。

- [ ] **Step 1: Write the failing test（真机脚本先行）**

```bash
#!/usr/bin/env bash
# tests/redis-compat/cluster-migrate.sh — 与 redis-server 7.2.6 互证
set -euo pipefail
REDIS=${REDIS:-redis-server}
CLI=${CLI:-redis-cli}
GEDIS=${GEDIS:-./gedis}
# 1. 起 7.2.6 双节点 cluster（7000/7001），CLUSTER SETSLOT <slot> MIGRATING/IMPORTING
# 2. 对同一 key 序列执行 ASK（无 key）/ 本地读写（有 key）/ MIGRATE COPY / MIGRATE / MIGRATE REPLACE
# 3. 断言 gedis 与真机回复逐行一致（ASK/MOVED/NOKEY/BUSYKEY/+OK）
# 4. kill -9 源端后重启，断言迁移态按 nodes.conf 恢复
```

先写脚本骨架使 `./tests/redis-compat/cluster-migrate.sh` 退出非零（缺 gedis 接线时失败）。

- [ ] **Step 2: Run test to verify it fails**

Run: `go build -o /tmp/gedis ./cmd/gedis && bash tests/redis-compat/cluster-migrate.sh`
Expected: FAIL（MIGRATE unknown command / SETSLOT 未落盘导致重启恢复断言失败）

- [ ] **Step 3: Write minimal implementation**

`cmd/gedis/main.go`：在 `RegisterCluster` 之前加 `commands.RegisterMigrate(router, store)`；cluster 启用时完成 Task 2 的快照重放+persist 接线。`openClusterSetup`（cluster_test.go:16）内加 `RegisterMigrate(r, store)` 使单测链路与线上一致。

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -race`
Expected: PASS；再跑 `bash tests/redis-compat/cluster-migrate.sh` PASS（需本机有 redis-server 7.2.6，否则记录跳过原因）。

- [ ] **Step 5: Commit**

```bash
git add cmd/gedis/main.go internal/commands/cluster_test.go tests/redis-compat/cluster-migrate.sh
git commit -m "feat(cluster): wire migrate handler and compat verification"
```
