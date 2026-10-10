package commands

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// openPushSetup 仿 openTrackingSetup：额外返回 store 与 PubSubRegistry，
// 并在 helper 内完成存储侧装配（SetChangeHook/SetEvictHook，仿 main.go
// 的装配点）。
func openPushSetup(t testing.TB) (*network.Router, *storage.Pebble, *serverHandler, *PubSubRegistry) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: true, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	s := network.NewStats()
	r.AttachStats(s)
	RegisterWriteCommands(r)
	RegisterStrings(r, store)
	st := acl.NewStore()
	authReg := RegisterAuth(r, st)
	connReg := RegisterConn(r, st, authReg)
	h := RegisterServer(r, store, s, hub, connReg, ServerDeps{StartUnix: 1700000000})
	reg := RegisterPubSub(r)
	store.SetChangeHook(InvalidateChange)
	store.SetEvictHook(OnEvicted)
	t.Cleanup(func() {
		store.SetChangeHook(nil)
		store.SetEvictHook(nil)
	})
	return r, store, h, reg
}

// invalidateValue 组装期望的失效推送帧：>2 invalidate [key]。
func invalidateValue(key string) protocol.Value {
	return protocol.Value{Kind: protocol.KindPush, Elems: []protocol.Value{
		protocol.BulkOf("invalidate"),
		protocol.ArrayOf(protocol.BulkOf(key)),
	}}
}

// expectNoFrames 断言 conn 在窗口内零帧（多一条即 FAIL）。
func expectNoFrames(t testing.TB, conn net.Conn, d time.Duration) {
	t.Helper()
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(d)))
	_, err := protocol.Decode(bufio.NewReader(conn))
	require.Error(t, err, "零多余帧")
	require.NoError(t, conn.SetReadDeadline(time.Time{}))
}

func pushOK(t testing.TB, v protocol.Value) {
	t.Helper()
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, v)
}

// Given: A（RESP3）TRACKING on 并 GET foo 完成读注册；notify 配置全关
// When: B 执行 SET foo 1
// Then: A 收到 KindPush invalidate foo 且零多余；B 零帧（RESP3 直推只给观察者）
func Test_Tracking_InvalidatesOnWrite(t *testing.T) {
	r, _, h, _ := openPushSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	h.conns.SetProto(srvA, 3)

	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA, "GET", "foo")
	pushOK(t, dispatchWithConn(r, srvB, "SET", "foo", "1"))

	require.Equal(t, invalidateValue("foo"), readPushed(t, cliA))
	expectNoFrames(t, cliA, 300*time.Millisecond)
	expectNoFrames(t, cliB, 100*time.Millisecond)
}

// Given: A（RESP2 默认）TRACKING on 并读注册 foo；S 订阅 __redis__:invalidate；
//
//	两个 publisher 场景只装 invalidate 发布器，notify 全关。
// When: B 执行 SET foo 1
// Then: S 收到 message 通道帧，payload 为 [foo]；零多余
func Test_Tracking_Resp2Fallback_PublishesInvalidate(t *testing.T) {
	r, _, _, reg := openPushSetup(t)
	SetInvalidatePublisher(reg.Publish)
	t.Cleanup(func() { SetInvalidatePublisher(nil) })
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	srvS, cliS := net.Pipe()
	t.Cleanup(func() {
		srvA.Close(); cliA.Close()
		srvB.Close(); cliB.Close()
		srvS.Close(); cliS.Close()
	})
	_ = cliA

	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA, "GET", "foo")
	require.Equal(t, confirmKind("__redis__:invalidate", 1, "subscribe"),
		dispatchPub(r, srvS, "SUBSCRIBE", "__redis__:invalidate"))

	pushOK(t, dispatchWithConn(r, srvB, "SET", "foo", "1"))
	want := protocol.ArrayOf(
		protocol.BulkOf("message"),
		protocol.BulkOf("__redis__:invalidate"),
		protocol.ArrayOf(protocol.BulkOf("foo")),
	)
	require.Equal(t, want, readPushed(t, cliS))
	expectNoFrames(t, cliS, 300*time.Millisecond)
}

// Given: A（RESP3）读注册 foo；B（RESP3）TRACKING on NOLOOP 也读注册 foo
// When: B 执行 SET foo 1（写者自身 NOLOOP）
// Then: A 收到 invalidate foo；B 零帧（NOLOOP 排除写者自己）
func Test_Tracking_NoloopExcludesWriter(t *testing.T) {
	r, _, h, _ := openPushSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	h.conns.SetProto(srvA, 3)
	h.conns.SetProto(srvB, 3)

	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA, "GET", "foo")
	pushOK(t, dispatchWithConn(r, srvB, "CLIENT", "TRACKING", "on", "NOLOOP"))
	_ = dispatchWithConn(r, srvB, "GET", "foo")

	pushOK(t, dispatchWithConn(r, srvB, "SET", "foo", "1"))
	require.Equal(t, invalidateValue("foo"), readPushed(t, cliA))
	expectNoFrames(t, cliA, 300*time.Millisecond)
	expectNoFrames(t, cliB, 300*time.Millisecond)
}

// Given: A（RESP3）读注册 k1/k5；库中有 k1/k5；notify 全关
// When: DEL k5；随后压小内存上限持续写新 key 触发驱逐（k1 最冷被逐）
// Then: A 先收 k5 invalidate、后收 k1 invalidate；零多余
func Test_Tracking_InvalidatesOnEvictAndDelete(t *testing.T) {
	r, store, h, _ := openPushSetup(t)
	srvA, cliA := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close() })
	h.conns.SetProto(srvA, 3)

	pushOK(t, dispatch(r, "SET", "k1", "v"))
	pushOK(t, dispatch(r, "SET", "k5", "v"))
	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA, "GET", "k1")
	_ = dispatchWithConn(r, srvA, "GET", "k5")

	// DEL → 'd' 通路
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "DEL", "k5"))
	require.Equal(t, invalidateValue("k5"), readPushed(t, cliA))

	// 逐出 → evictHook op 'e' 通路（k1 最冷，容量压到 100B 后最先被逐）
	store.SetMaxBytes(100)
	rd := bufio.NewReader(cliA)
	var got protocol.Value
	for i := 0; i < 60; i++ {
		pushOK(t, dispatch(r, "SET", "k"+strconv.Itoa(i), strings.Repeat("v", 20)))
		require.NoError(t, cliA.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
		v, err := protocol.Decode(rd)
		if err == nil {
			got = v
			break
		}
	}
	require.NoError(t, cliA.SetReadDeadline(time.Time{}))
	require.Equal(t, invalidateValue("k1"), got, "被逐出的 k1 必须失效推送")
	expectNoFrames(t, cliA, 300*time.Millisecond)
}

// Given: notify-keyspace-events 全关、notify publisher 与 invalidate
// publisher 均 nil
// When: A 读注册 foo 后 B SET foo 1
// Then: A 仍收到 RESP3 失效推送（失效独立于 notify 配置，硬约束 S7）
func Test_Tracking_WorksWithoutNotifyConfig(t *testing.T) {
	r, _, h, _ := openPushSetup(t)
	require.NoError(t, SetNotifyString(""))
	SetNotifyPublisher(nil)
	SetInvalidatePublisher(nil)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		SetInvalidatePublisher(nil)
		_ = SetNotifyString("")
	})
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	h.conns.SetProto(srvA, 3)

	pushOK(t, dispatchWithConn(r, srvA, "CLIENT", "TRACKING", "on"))
	_ = dispatchWithConn(r, srvA, "GET", "foo")
	pushOK(t, dispatchWithConn(r, srvB, "SET", "foo", "1"))
	require.Equal(t, invalidateValue("foo"), readPushed(t, cliA))
	expectNoFrames(t, cliA, 300*time.Millisecond)
}
