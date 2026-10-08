package commands

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openReplSetup(t testing.TB) (*network.Router, *storage.Pebble, *network.Stats, *replication.Hub) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: true, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	s := network.NewStats()
	r.AttachStats(s)
	RegisterStrings(r, store)
	RegisterWriteCommands(r)
	RegisterReplication(r, store, s, hub)
	return r, store, s, hub
}

// Given: 主库已有数据
// When: PSYNC ? -1（带 pipe 连接的 ctx）
// Then: 回复为 [FULLRESYNC 标记, RDB bulk] 复合数组，RDB 含全部 key
func Test_Repl_when_PsyncFull(t *testing.T) {
	r, _, s, hub := openReplSetup(t)
	dispatch(r, "SET", "a", "1")
	dispatch(r, "SET", "b", "2")

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	defer hub.Close()
	ctx := network.ContextWithConn(context.Background(), server)
	got := r.Dispatch(ctx, cmd("PSYNC", "?", "-1"))
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.True(t, strings.HasPrefix(string(got.Elems[0].Bulk), "FULLRESYNC "+s.ReplID+" "))
	require.Equal(t, protocol.KindBulkString, got.Elems[1].Kind)
	entries, err := replication.UnmarshalRDB(got.Elems[1].Bulk)
	require.NoError(t, err)
	require.Len(t, entries, 2)
}

// Given: 主库模式
// When: REPLICAOF NO ONE / REPLICAOF 参数错误
// Then: +OK 且保持 master；错误参数报 syntax
func Test_Repl_when_ReplicaofNoOne(t *testing.T) {
	r, _, s, _ := openReplSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "REPLICAOF", "NO", "ONE"))
	require.Equal(t, "master", s.Role)
	got := dispatch(r, "REPLICAOF", "NO")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatch(r, "REPLICAOF", "host", "notaport")
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: 主库模式
// When: REPLCONF 各子命令
// Then: listening-port/capa 回 +OK；未知子命令与坏参数报错；ACK 需连接上下文
func Test_Repl_when_Replconf(t *testing.T) {
	r, _, _, _ := openReplSetup(t)
	ok := protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	require.Equal(t, ok, dispatch(r, "REPLCONF", "listening-port", "6380"))
	require.Equal(t, protocol.KindError, dispatch(r, "REPLCONF", "listening-port", "nope").Kind)
	require.Equal(t, ok, dispatch(r, "REPLCONF", "capa", "psync2"))
	require.Equal(t, ok, dispatch(r, "REPLCONF", "capa", "eof"))
	require.Equal(t, protocol.KindError, dispatch(r, "REPLCONF", "bogus-cmd").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "REPLCONF").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "REPLCONF", "ACK").Kind)
	// 真机措辞对齐（redis 7.2.6 实测）：未知子命令与坏参数面一律 ERR syntax error；
	// capa 接受多个；GETACK 必须带 '*'，裸 GETACK 为 syntax error。
	require.Equal(t, "ERR syntax error", dispatch(r, "REPLCONF", "bogus-cmd").S)
	require.Equal(t, "ERR syntax error", dispatch(r, "REPLCONF", "ACK").S)
	require.Equal(t, "ERR syntax error", dispatch(r, "REPLCONF", "capa").S)
	require.Equal(t, "ERR syntax error", dispatch(r, "REPLCONF", "listening-port").S)
	require.Equal(t, ok, dispatch(r, "REPLCONF", "capa", "eof", "capa", "psync2"))
	require.Equal(t, "ERR syntax error", dispatch(r, "REPLCONF", "GETACK").S)
	got := dispatch(r, "REPLCONF", "GETACK", "*")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, "REPLCONF", string(got.Elems[0].Bulk))
	require.Equal(t, "ACK", string(got.Elems[1].Bulk))
	// ACK 无连接上下文必须报错（无法归属副本）
	require.Equal(t, protocol.KindError, dispatch(r, "REPLCONF", "ACK", "5").Kind)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	ctx := network.ContextWithConn(context.Background(), server)
	require.Equal(t, ok, r.Dispatch(ctx, cmd("REPLCONF", "ACK", "5")))
	require.Equal(t, protocol.KindError, r.Dispatch(ctx, cmd("REPLCONF", "ACK", "nope")).Kind)
}

// Given: 主库模式
// When: SYNC / SLAVEOF / FAILOVER / WAIT 参数面
// Then: SYNC 指路 PSYNC；SLAVEOF NO ONE 回 OK；无副本 FAILOVER 报错；WAIT 零副本回 0
func Test_Repl_when_SyncSlaveofFailoverWait(t *testing.T) {
	r, _, _, _ := openReplSetup(t)
	ok := protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}

	got := dispatch(r, "SYNC")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "PSYNC")

	require.Equal(t, ok, dispatch(r, "SLAVEOF", "NO", "ONE"))
	got = dispatch(r, "SLAVEOF", "host", "notaport")
	require.Equal(t, protocol.KindError, got.Kind)

	got = dispatch(r, "FAILOVER")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "replica")
	require.Equal(t, "ERR FAILOVER requires connected replicas.", got.S)

	got = dispatch(r, "WAIT", "0", "100")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(0), got.I)

	start := time.Now()
	got = dispatch(r, "WAIT", "1", "50")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(0), got.I)
	require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)

	require.Equal(t, protocol.KindError, dispatch(r, "WAIT", "1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "WAIT", "x", "y").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "WAIT", "-1", "10").Kind)
}
// Given: 主库模式
// When: SET / GET
// Then: SET 报 READONLY，GET 正常；关闭只读后 SET 恢复
func Test_Repl_when_ReadOnly(t *testing.T) {
	r, _, _, _ := openReplSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v"))
	r.SetReadOnly(true)
	got := dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "READONLY")
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
	r.SetReadOnly(false)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v"))
}
