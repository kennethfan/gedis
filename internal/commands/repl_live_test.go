package commands

import (
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

type replServer struct {
	router *network.Router
	store  *storage.Pebble
	stats  *network.Stats
	hub    *replication.Hub
	srv    *network.Server
	addr   string
}

func openReplServer(t testing.TB) *replServer {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: true, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterHash(r, store)
	RegisterSet(r, store)
	RegisterWriteCommands(r)
	srv := network.NewServer(r)
	s := srv.Stats()
	RegisterMonitor(r, store, s, hub)
	RegisterReplication(r, store, s, hub)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		hub.Close()
		_ = store.Close()
	})
	return &replServer{router: r, store: store, stats: s, hub: hub, srv: srv, addr: ln.Addr().String()}
}

func replHostPort(t testing.TB, addr string) (string, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	return host, port
}

func requireReplicaHas(t testing.TB, r *network.Router, key, want string) {
	t.Helper()
	require.Eventually(t, func() bool {
		got := r.Dispatch(context.Background(), cmd("GET", key))
		return got.Kind == protocol.KindBulkString && string(got.Bulk) == want
	}, 5*time.Second, 20*time.Millisecond)
}

// Given: 主从双活服务
// When: 从库 REPLICAOF 主库
// Then: 全量 + 增量都同步过来
func Test_ReplLive_when_FullAndIncremental(t *testing.T) {
	master := openReplServer(t)
	replica := openReplServer(t)
	dispatchOn(master.router, "SET", "a", "1")
	dispatchOn(master.router, "SADD", "s", "x")

	host, port := replHostPort(t, master.addr)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		replica.router.Dispatch(context.Background(), cmd("REPLICAOF", host, port)))
	requireReplicaHas(t, replica.router, "a", "1")

	dispatchOn(master.router, "SET", "b", "2")
	requireReplicaHas(t, replica.router, "b", "2")

	got := replica.router.Dispatch(context.Background(), cmd("INFO", "replication"))
	require.Contains(t, string(got.Bulk), "role:slave")
	require.Contains(t, string(got.Bulk), "master_port:"+port)
}

// Given: 已同步的从库
// When: NO ONE 断开 → 主库继续写 → 重新 REPLICAOF
// Then: 断点后数据追平
func Test_ReplLive_when_ReconnectCatchesUp(t *testing.T) {
	master := openReplServer(t)
	replica := openReplServer(t)
	dispatchOn(master.router, "SET", "a", "1")

	host, port := replHostPort(t, master.addr)
	replica.router.Dispatch(context.Background(), cmd("REPLICAOF", host, port))
	requireReplicaHas(t, replica.router, "a", "1")

	replica.router.Dispatch(context.Background(), cmd("REPLICAOF", "NO", "ONE"))
	dispatchOn(master.router, "SET", "after", "cut")
	time.Sleep(300 * time.Millisecond)
	got := replica.router.Dispatch(context.Background(), cmd("GET", "after"))
	require.Nil(t, got.Bulk)

	replica.router.Dispatch(context.Background(), cmd("REPLICAOF", host, port))
	requireReplicaHas(t, replica.router, "after", "cut")
}

// Given: 一主两从
// When: 双从 REPLICAOF 同一主库
// Then: 两从都收敛，INFO 显示 2 slaves
func Test_ReplLive_when_MultiReplica(t *testing.T) {
	master := openReplServer(t)
	r1 := openReplServer(t)
	r2 := openReplServer(t)
	dispatchOn(master.router, "SET", "k", "v")

	host, port := replHostPort(t, master.addr)
	for _, r := range []*replServer{r1, r2} {
		require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
			r.router.Dispatch(context.Background(), cmd("REPLICAOF", host, port)))
	}
	requireReplicaHas(t, r1.router, "k", "v")
	requireReplicaHas(t, r2.router, "k", "v")

	require.Eventually(t, func() bool {
		return master.hub.SubCount() == 2
	}, 5*time.Second, 20*time.Millisecond)
	got := master.router.Dispatch(context.Background(), cmd("INFO", "replication"))
	require.Contains(t, string(got.Bulk), "connected_slaves:2")
}

// Given: 主库 backlog 有操作
// When: PSYNC <replid> <offset>（带连接 ctx）
// Then: 返回 CONTINUE + 遗漏 OP
func Test_ReplLive_when_PsyncContinue(t *testing.T) {
	m := openReplServer(t)
	dispatchOn(m.router, "SET", "a", "1")
	dispatchOn(m.router, "SET", "b", "2")
	latest := m.hub.Backlog().Latest()

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	defer m.hub.Close()
	ctx := network.ContextWithConn(context.Background(), server)
	got := m.router.Dispatch(ctx, cmd("PSYNC", m.stats.ReplID, strconv.FormatInt(latest-1, 10)))
	require.Equal(t, protocol.KindArray, got.Kind)
	require.True(t, strings.HasPrefix(string(got.Elems[0].Bulk), "CONTINUE "))
	require.Len(t, got.Elems[1].Elems, 1)
}

func dispatchOn(r *network.Router, args ...string) protocol.Value {
	return r.Dispatch(context.Background(), cmd(args...))
}
