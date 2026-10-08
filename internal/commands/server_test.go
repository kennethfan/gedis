package commands

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openServerSetup(t testing.TB) (*network.Router, *storage.Pebble, *network.Stats, *replication.Hub, *int64) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: true, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	s := network.NewStats()
	r.AttachStats(s)
	RegisterStrings(r, store)
	RegisterMonitor(r, store, s, hub)
	RegisterReplication(r, store, s, hub)
	st := acl.NewStore()
	authReg := RegisterAuth(r, st)
	connReg := RegisterConn(r, st, authReg)
	var shutCalls int64
	RegisterServer(r, store, s, hub, connReg, ServerDeps{
		StartUnix: 1700000000,
		Shutdown:  func() { atomic.AddInt64(&shutCalls, 1) },
	})
	return r, store, s, hub, &shutCalls
}

func dispatchWithConn(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	return r.Dispatch(network.ContextWithConn(context.Background(), conn), cmd(args...))
}

// Given: 库中有 2 个 key
// When: DBSIZE / DEL 后再 DBSIZE
// Then: 依次回 2、1
func Test_Server_when_Dbsize(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	dispatch(r, "SET", "a", "1")
	dispatch(r, "SET", "b", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "DBSIZE"))
	dispatch(r, "DEL", "a")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "DBSIZE"))
}

// Given: 库中有数据且 backlog 有水位
// When: FLUSHDB / FLUSHDB ASYNC / FLUSHALL
// Then: 均回 OK；数据清空；backlog 水位推进（复制标记已进流）
func Test_Server_when_Flush(t *testing.T) {
	r, _, _, hub, _ := openServerSetup(t)
	dispatch(r, "SET", "x", "1")
	before := hub.Backlog().Latest()
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "FLUSHDB"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "GET", "x"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "DBSIZE"))
	require.Greater(t, hub.Backlog().Latest(), before)

	dispatch(r, "SET", "y", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "FLUSHDB", "ASYNC"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "DBSIZE"))

	dispatch(r, "SET", "z", "3")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "FLUSHALL"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "DBSIZE"))

	got := dispatch(r, "FLUSHDB", "BOGUS")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: 服务端已装配
// When: LASTSAVE / ROLE / TIME
// Then: LASTSAVE=装配时刻；ROLE 首元 master；TIME 双整数且秒级合理
func Test_Server_when_LastsaveRoleTime(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1700000000}, dispatch(r, "LASTSAVE"))

	role := dispatch(r, "ROLE")
	require.Equal(t, protocol.KindArray, role.Kind)
	require.GreaterOrEqual(t, len(role.Elems), 1)
	require.Equal(t, "master", string(role.Elems[0].Bulk))

	now := time.Now().Unix()
	tm := dispatch(r, "TIME")
	require.Equal(t, protocol.KindArray, tm.Kind)
	require.Len(t, tm.Elems, 2)
	sec, err := strconv.ParseInt(string(tm.Elems[0].Bulk), 10, 64)
	require.NoError(t, err)
	require.GreaterOrEqual(t, sec, now-60)
	require.LessOrEqual(t, sec, now+60)
}

// Given: 带连接的 ctx
// When: CLIENT SETNAME/GETNAME/LIST/ID
// Then: 命名往返；LIST 含 name=web；ID 为正整数；KILL 明确报错
func Test_Server_when_ClientNames(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t,
		protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchWithConn(r, c1, "CLIENT", "SETNAME", "web"))
	require.Equal(t, protocol.BulkOf("web"), dispatchWithConn(r, c1, "CLIENT", "GETNAME"))

	list := dispatchWithConn(r, c1, "CLIENT", "LIST")
	require.Equal(t, protocol.KindBulkString, list.Kind)
	require.Contains(t, string(list.Bulk), "name=web")

	id := dispatchWithConn(r, c1, "CLIENT", "ID")
	require.Equal(t, protocol.KindInteger, id.Kind)
	require.Greater(t, id.I, int64(0))

	// 无命名的新连接 GETNAME 回空
	c3, c4 := net.Pipe()
	defer c3.Close()
	defer c4.Close()
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchWithConn(r, c3, "CLIENT", "GETNAME"))

	// 从未命名的新连接直接 LIST：必须能看到自己（建号副作用回归）
	fresh1, fresh2 := net.Pipe()
	defer fresh1.Close()
	defer fresh2.Close()
	selfID := dispatchWithConn(r, fresh1, "CLIENT", "ID")
	freshList := dispatchWithConn(r, fresh1, "CLIENT", "LIST")
	require.Contains(t, string(freshList.Bulk), "id="+strconv.FormatInt(selfID.I, 10))

	// KILL 尚未接线：诚实报错而非静默成功
	got := dispatchWithConn(r, c1, "CLIENT", "KILL", "ID", "1")
	require.Equal(t, protocol.KindError, got.Kind)

	// 无连接 ctx 下 CLIENT ID 报错
	got = dispatch(r, "CLIENT", "ID")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: 库中有 string key
// When: MEMORY USAGE / DOCTOR / STATS
// Then: USAGE 回正整数估算；缺 key 回空；DOCTOR/STATS 明确不支持
func Test_Server_when_Memory(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	dispatch(r, "SET", "k", "v")
	usage := dispatch(r, "MEMORY", "USAGE", "k")
	require.Equal(t, protocol.KindInteger, usage.Kind)
	require.Greater(t, usage.I, int64(0))
	usage = dispatch(r, "MEMORY", "USAGE", "k", "SAMPLES", "5")
	require.Equal(t, protocol.KindInteger, usage.Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "MEMORY", "USAGE", "missing"))
	require.Equal(t, protocol.KindError, dispatch(r, "MEMORY", "DOCTOR").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "MEMORY", "STATS").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "MEMORY").Kind)
}

// Given: 无 RDB/AOF 引擎
// When: BGSAVE / BGREWRITEAOF
// Then: 诚实报错而非静默 +OK
func Test_Server_when_BgsaveUnsupported(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	got := dispatch(r, "BGSAVE")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "not supported")
	got = dispatch(r, "BGREWRITEAOF")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "not supported")
}

// Given: Shutdown 钩子为计数 spy
// When: SHUTDOWN NOSAVE / SHUTDOWN
// Then: 回 OK 且钩子被触发；非法参数报错
func Test_Server_when_Shutdown(t *testing.T) {
	r, _, _, _, shutCalls := openServerSetup(t)
	require.Equal(t, protocol.KindError, dispatch(r, "SHUTDOWN", "BOGUS").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SHUTDOWN", "NOSAVE"))
	require.Eventually(t, func() bool { return atomic.LoadInt64(shutCalls) == 1 }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SHUTDOWN"))
	require.Eventually(t, func() bool { return atomic.LoadInt64(shutCalls) == 2 }, 3*time.Second, 10*time.Millisecond)
}

// Given: CONFIG 已注册
// When: RESETSTAT / GET+SET notify-keyspace-events
// Then: RESETSTAT 回 OK；通知配置可写读往返（Phase 7 前为空串占位读口）
func Test_Server_when_ConfigResetstatNotify(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "CONFIG", "RESETSTAT"))

	got := dispatch(r, "CONFIG", "GET", "notify-keyspace-events")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, "notify-keyspace-events", string(got.Elems[0].Bulk))

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	got = dispatch(r, "CONFIG", "GET", "notify-keyspace-events")
	require.Equal(t, "KEA", string(got.Elems[1].Bulk))
}

// Given: 无延迟跟踪
// When: LATENCY LATEST/HISTORY/RESET
// Then: 查询回空数组；RESET 回 OK
func Test_Server_when_Latency(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "LATENCY", "LATEST"))
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}},
		dispatch(r, "LATENCY", "HISTORY", "event"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "LATENCY", "RESET"))
	require.Equal(t, protocol.KindError, dispatch(r, "LATENCY").Kind)
}

// Given: 库中有 key
// When: DEBUG SLEEP 0 / DEBUG OBJECT / DEBUG RELOAD
// Then: SLEEP 回 OK；OBJECT 回编码信息；RELOAD 回 OK；未知子命令报错
func Test_Server_when_Debug(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "DEBUG", "SLEEP", "0"))
	dispatch(r, "SET", "k", "v")
	obj := dispatch(r, "DEBUG", "OBJECT", "k")
	require.Equal(t, protocol.KindBulkString, obj.Kind)
	require.Contains(t, string(obj.Bulk), "encoding:embstr")
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "DEBUG", "OBJECT", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "DEBUG", "RELOAD"))
	require.Equal(t, protocol.KindError, dispatch(r, "DEBUG", "BOGUS").Kind)
}

// Given: 库中有 string key
// When: OBJECT REFCOUNT/IDLETIME/FREQ
// Then: 回 1/0/0（未跟踪的诚实占位）；缺 key 回空；非法子命令报错
func Test_Server_when_ObjectExtended(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "OBJECT", "REFCOUNT", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "OBJECT", "IDLETIME", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "OBJECT", "FREQ", "k"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "OBJECT", "REFCOUNT", "missing"))
	require.Equal(t, protocol.KindError, dispatch(r, "OBJECT", "BOGUS", "k").Kind)
	require.Equal(t, protocol.BulkOf("embstr"), dispatch(r, "OBJECT", "ENCODING", "k"))
}

// Given: 全命令表已注册（含 COMMAND）
// When: COMMAND DOCS
// Then: 回非空数组，首元为 [name, ...] 结构
func Test_Server_when_CommandDocs(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	got := dispatch(r, "COMMAND", "DOCS")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.NotEmpty(t, got.Elems)
	require.Equal(t, protocol.KindArray, got.Elems[0].Kind)
	require.NotEmpty(t, got.Elems[0].Elems)
}

// Given: INFO 已注册
// When: INFO persistence
// Then: 含 rdb_last_save_time 与 aof_enabled:0
func Test_Server_when_InfoPersistence(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	got := dispatch(r, "INFO", "persistence")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Contains(t, string(got.Bulk), "rdb_last_save_time:")
	require.Contains(t, string(got.Bulk), "aof_enabled:0")
}
