package commands

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// wantEvent 组装单个事件的双频道推送（K 先 E 后，KEA 下每事件两条）。
func wantEvent(key, event string) []protocol.Value {
	return []protocol.Value{
		notifyPMsg("__keyspace@0__:"+key, event),
		notifyPMsg("__keyevent@0__:"+event, key),
	}
}

// expectEvents 断言 conn 上恰好收到 want 序列：多一条少一条都 FAIL；
// 读带超时，缺事件时快速失败不挂死。
func expectEvents(t testing.TB, conn net.Conn, want []protocol.Value) {
	t.Helper()
	for i, w := range want {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)), "event[%d] 读超时", i)
		got, err := protocol.Decode(bufio.NewReader(conn))
		require.NoError(t, err, "event[%d] 读失败", i)
		require.Equal(t, w, got, "event[%d]", i)
	}
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(300*time.Millisecond)))
	_, err := protocol.Decode(bufio.NewReader(conn))
	require.Error(t, err, "零多余事件")
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
}

// Given: KEA 开启，A 订阅 __key*__:*，B 执行事件表用例
// When: 逐条执行动作序列
// Then: 双频道事件序列精确匹配，零多余；负例零事件
func TestGenericStringEvents(t *testing.T) {
	r, store, _ := openMonitorSetup(t)
	RegisterGeneric(r, store)
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
	seq := func(events ...[]protocol.Value) []protocol.Value {
		var out []protocol.Value
		for _, e := range events {
			out = append(out, e...)
		}
		return out
	}

	// 1. SET k v; APPEND k x → set, append
	run("SET", "k", "v")
	run("APPEND", "k", "x")
	expectEvents(t, cliA, seq(wantEvent("k", "set"), wantEvent("k", "append")))

	// 2. INCR n → incrby
	run("INCR", "n")
	expectEvents(t, cliA, seq(wantEvent("n", "incrby")))

	// 3. SETNX 替代为 SET NX：新建成功 → set；已存在失败 → 零事件
	run("SET", "k2", "v", "NX")
	run("SET", "k2", "v2", "NX")
	expectEvents(t, cliA, seq(wantEvent("k2", "set")))

	// 4. SET ex 100; EXPIRE ex 50 → set, expire（完整序列，含准备写）
	run("SET", "ex", "100")
	run("EXPIRE", "ex", "50")
	expectEvents(t, cliA, seq(wantEvent("ex", "set"), wantEvent("ex", "expire")))

	// 5. EXPIRE 过去时间戳 → 实现走立即删除路径 → del（读实现后定）
	run("SET", "past", "v")
	run("EXPIRE", "past", "-100")
	expectEvents(t, cliA, seq(wantEvent("past", "set"), wantEvent("past", "del")))

	// 6. PERSIST 清 TTL → persist；再 PERSIST（已无 TTL）→ 零事件
	run("PERSIST", "ex")
	expectEvents(t, cliA, seq(wantEvent("ex", "persist")))
	run("PERSIST", "ex")
	expectEvents(t, cliA, nil)

	// 7. MSET a 1 b 2 → set(a), set(b)
	run("MSET", "a", "1", "b", "2")
	expectEvents(t, cliA, seq(wantEvent("a", "set"), wantEvent("b", "set")))

	// 8. RENAME r1 r2 → rename_from(r1), rename_to(r2)
	run("SET", "r1", "v")
	run("RENAME", "r1", "r2")
	expectEvents(t, cliA, seq(wantEvent("r1", "set"), wantEvent("r1", "rename_from"), wantEvent("r2", "rename_to")))

	// 9. GETDEL gd → set(gd), del(gd)
	run("SET", "gd", "v")
	run("GETDEL", "gd")
	expectEvents(t, cliA, seq(wantEvent("gd", "set"), wantEvent("gd", "del")))

	// 10. COPY c1 c2 → set(c1), copy_to(c2)
	run("SET", "c1", "v")
	run("COPY", "c1", "c2")
	expectEvents(t, cliA, seq(wantEvent("c1", "set"), wantEvent("c2", "copy_to")))

	// 11. DEL → set(s1), del(s1)
	run("SET", "s1", "v")
	run("DEL", "s1")
	expectEvents(t, cliA, seq(wantEvent("s1", "set"), wantEvent("s1", "del")))

	// 负例：EXPIRE 不存在 key → 零事件
	run("EXPIRE", "nokey", "100")
	expectEvents(t, cliA, nil)

	// 负例：DEL 不存在 key → 零事件
	run("DEL", "nokey2")
	expectEvents(t, cliA, nil)

	// 负例：RENAMENX 目标已存在 → 零事件（准备写照常计数）
	run("SET", "rx1", "v")
	run("SET", "rx2", "v")
	run("RENAMENX", "rx1", "rx2")
	expectEvents(t, cliA, seq(wantEvent("rx1", "set"), wantEvent("rx2", "set")))
}
