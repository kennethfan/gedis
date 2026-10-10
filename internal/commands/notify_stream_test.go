package commands

import (
	"net"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: KEA 开启，A 订阅 __key*__:*，B 执行 stream 类事件表用例
// When: XADD/XTRIM/XDEL/XSETID/XGROUP 族逐条执行
// Then: t 类事件双频道序列精确匹配；失败路径（BUSYGROUP、I:0）零事件
func TestStreamEvents(t *testing.T) {
	r, store, stats := openMonitorSetup(t)
	RegisterStream(r, store, stats)
	reg := RegisterPubSub(r)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	run := func(args ...string) {
		t.Helper()
		dispatchPub(r, srvB, args...)
	}

	// XADD 每条消息 1 条 xadd
	run("XADD", "s1", "100-1", "f", "v")
	expectEvents(t, cliA, wantEvent("s1", "xadd"))
	run("XADD", "s1", "100-2", "f", "v2")
	expectEvents(t, cliA, wantEvent("s1", "xadd"))

	// XTRIM 成功即发（真删 1 条）
	run("XTRIM", "s1", "MAXLEN", "=", "1")
	expectEvents(t, cliA, wantEvent("s1", "xtrim"))

	// XTRIM deleted=0 仍发（条件=成功）
	run("XTRIM", "s1", "MAXLEN", "=", "1")
	expectEvents(t, cliA, wantEvent("s1", "xtrim"))

	// XDEL 真删 → xdel；重复删 → 零事件
	run("XDEL", "s1", "100-2")
	expectEvents(t, cliA, wantEvent("s1", "xdel"))
	run("XDEL", "s1", "100-2")
	expectEvents(t, cliA, nil)

	// XSETID 成功 → xsetid
	run("XSETID", "s1", "200-0")
	expectEvents(t, cliA, wantEvent("s1", "xsetid"))

	// XGROUP CREATE 已有 group → BUSYGROUP 零事件
	run("XGROUP", "CREATE", "s1", "g1", "0-0")
	expectEvents(t, cliA, wantEvent("s1", "xgroup-create"))
	run("XGROUP", "CREATE", "s1", "g1", "0-0")
	expectEvents(t, cliA, nil)

	// XGROUP CREATE + MKSTREAM（流不存在）→ xgroup-create
	run("XGROUP", "CREATE", "s2", "g1", "0-0", "MKSTREAM")
	expectEvents(t, cliA, wantEvent("s2", "xgroup-create"))

	// CREATECONSUMER 成功 I:1 → 事件；重复 I:0 → 零事件
	run("XGROUP", "CREATECONSUMER", "s1", "g1", "c1")
	expectEvents(t, cliA, wantEvent("s1", "xgroup-createconsumer"))
	run("XGROUP", "CREATECONSUMER", "s1", "g1", "c1")
	expectEvents(t, cliA, nil)

	// DELCONSUMER found（dropped 可 0）→ 事件；不存在 consumer I:0 → 零事件
	run("XGROUP", "DELCONSUMER", "s1", "g1", "c1")
	expectEvents(t, cliA, wantEvent("s1", "xgroup-delconsumer"))
	run("XGROUP", "DELCONSUMER", "s1", "g1", "c9")
	expectEvents(t, cliA, nil)

	// XGROUP SETID 成功 → xgroup-setid
	run("XGROUP", "SETID", "s1", "g1", "5-0")
	expectEvents(t, cliA, wantEvent("s1", "xgroup-setid"))

	// DESTROY 成功 I:1 → 事件；不存在 I:0 → 零事件
	run("XGROUP", "DESTROY", "s1", "g1")
	expectEvents(t, cliA, wantEvent("s1", "xgroup-destroy"))
	run("XGROUP", "DESTROY", "s1", "g1")
	expectEvents(t, cliA, nil)
}

// Given: KEA 开启，订阅在位；k1/k2 各带 50ms TTL
// When: k1 走 GET 触发惰性过期；k2 不访问由 SweepOnce 主动清扫
// Then: 两条 expired 事件分别由两条路径发出
func TestExpiredEvents(t *testing.T) {
	r, store, stats := openMonitorSetup(t)
	reg := RegisterPubSub(r)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	run := func(args ...string) {
		t.Helper()
		dispatchPub(r, srvB, args...)
	}

	// 惰性过期：GET 触发 lookupRaw 删除
	run("SET", "k1", "v", "PX", "50")
	expectEvents(t, cliA, wantEvent("k1", "set"))
	time.Sleep(120 * time.Millisecond)
	run("GET", "k1")
	expectEvents(t, cliA, wantEvent("k1", "expired"))

	// 主动清扫：不访问，由 SweepOnce 扫到
	run("SET", "k2", "v", "PX", "50")
	expectEvents(t, cliA, wantEvent("k2", "set"))
	time.Sleep(120 * time.Millisecond)
	exp := NewExpirer(store, stats)
	n, err := exp.SweepOnce()
	require.NoError(t, err)
	require.GreaterOrEqual(t, n, 1, "SweepOnce 应删到 k2")
	expectEvents(t, cliA, wantEvent("k2", "expired"))
}
