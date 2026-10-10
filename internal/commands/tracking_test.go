package commands

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// openTrackingSetup 仿 openServerSetup，但保留 serverHandler（读 h.tracks/h.conns
// 断言）并装写命令集合（SET 走写过滤）。
func openTrackingSetup(t testing.TB) (*network.Router, *serverHandler) {
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
	return r, h
}

// Given: 一条带连接的 ctx
// When: CLIENT TRACKING on / 重复 flag / 未知 flag / OPTIN+OPTOUT 冲突 / 缺 on|off / off
// Then: on 回 OK 且 Tracking=true；重复或未知 flag 回 syntax error；冲突回兼容错误；
//
//	缺 on|off 回 syntax error；off 回 OK 且 Tracking=false。
func Test_ClientTracking_OnOffAndFlags(t *testing.T) {
	r, h := openTrackingSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on"))
	require.True(t, h.conns.TrackingOf(c1).On)

	// 重复 flag → syntax error
	got := dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on", "BCAST", "BCAST")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")

	// 未知 flag → syntax error
	got = dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on", "NOLOOOP")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")

	// OPTIN 与 OPTOUT 同时 → 兼容错误
	got = dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on", "OPTIN", "OPTOUT")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "ERR OPTIN and OPTOUT are not compatible")

	// 缺 on|off → syntax error
	got = dispatchWithConn(r, c1, "CLIENT", "TRACKING")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")
	got = dispatchWithConn(r, c1, "CLIENT", "TRACKING", "banana")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")

	// off → OK 且 Tracking=false
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "off"))
	require.False(t, h.conns.TrackingOf(c1).On)
}

// Given: TRACKING off 与 on 两种状态
// When: CLIENT CACHING yes|no
// Then: off 时报 tracking 未开；on 时回 OK 且 CachingYes 置位/清零。
func Test_ClientTracking_Caching(t *testing.T) {
	r, h := openTrackingSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// tracking 未开 → 明确报错
	got := dispatchWithConn(r, c1, "CLIENT", "CACHING", "yes")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "ERR CLIENT CACHING can be called only when tracking is enabled")

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "CACHING", "yes"))
	require.True(t, h.conns.CachingYesOf(c1))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "CACHING", "no"))
	require.False(t, h.conns.CachingYesOf(c1))

	// 非 yes/no → syntax error
	got = dispatchWithConn(r, c1, "CLIENT", "CACHING", "maybe")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "syntax error")
}

// Given: TRACKING on 的连接与写命令集合
// When: SET（写）→ GET（读）→ TRACKING off；OPTIN 未 CACHING / CACHING yes 后 GET；OPTOUT 下 GET
// Then: 写命令不注册；默认模式 GET 注册 foo→connID；off 清空该 conn 表项；
//
//	OPTIN 未 CACHING 不注册、CACHING yes 后注册且 CachingYes 复位；OPTOUT 跳过。
func Test_Tracking_RegistersReadKeys(t *testing.T) {
	r, h := openTrackingSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on"))
	id := h.conns.IDOf(c1)

	// 写命令 → 不注册
	dispatchWithConn(r, c1, "SET", "foo", "v")
	require.Empty(t, h.tracks.ConnsFor("foo"))

	// 读命令 → 注册 foo→connID
	dispatchWithConn(r, c1, "GET", "foo")
	require.Equal(t, []int64{id}, h.tracks.ConnsFor("foo"))

	// TRACKING off → 该 conn 全部表项清除
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "off"))
	require.Empty(t, h.tracks.ConnsFor("foo"))
	require.Empty(t, h.tracks.KeysFor(id))

	// OPTIN：未先 CACHING yes → 不注册
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on", "OPTIN"))
	dispatchWithConn(r, c1, "GET", "foo")
	require.Empty(t, h.tracks.ConnsFor("foo"))

	// CACHING yes 后 GET → 注册且 CachingYes 被消费复位
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "CACHING", "yes"))
	require.True(t, h.conns.CachingYesOf(c1))
	dispatchWithConn(r, c1, "GET", "foo")
	require.Equal(t, []int64{id}, h.tracks.ConnsFor("foo"))
	require.False(t, h.conns.CachingYesOf(c1))

	// OPTOUT → 跳过注册
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "off"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "TRACKING", "on", "OPTOUT"))
	dispatchWithConn(r, c1, "GET", "foo")
	require.NotContains(t, h.tracks.ConnsFor("foo"), id)
}
