package commands

import (
	"context"
	"net"
	"strings"
	"testing"

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
