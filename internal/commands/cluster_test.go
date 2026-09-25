package commands

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func openClusterSetup(t testing.TB, topo *cluster.Topology) (*network.Router, *AskRegistry) {
	t.Helper()
	r, store := openTestSetup(t)
	txnReg := RegisterTxn(r, nil)
	asking := NewAskRegistry()
	h := RegisterCluster(r, store, topo, asking)
	txnReg.PreExec = h.CheckExec
	return r, asking
}

func fullTopo(t testing.TB, selfAddr string) *cluster.Topology {
	t.Helper()
	topo, err := cluster.Build(selfAddr, nil)
	require.NoError(t, err)
	return topo
}

func splitTopo(t testing.TB, selfAddr string) *cluster.Topology {
	t.Helper()
	topo, err := cluster.Build(selfAddr, []cluster.NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 10922}}},
		{ID: "nodeC", Addr: "127.0.0.1:7002", Ranges: [][2]int{{10923, 16383}}},
	})
	require.NoError(t, err)
	return topo
}

func dispatchConn(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

// Given: topo 为 nil（集群关闭）
// When: CLUSTER INFO / ASKING
// Then: CLUSTER 报 disabled；ASKING 未注册→unknown-command（7.2.6 standalone 实测纠正）
func Test_Cluster_when_Disabled(t *testing.T) {
	r, _ := openClusterSetup(t, nil)
	got := dispatch(r, "CLUSTER", "INFO")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR This instance has cluster support disabled", got.S)
	got = dispatch(r, "asking")
	require.Equal(t, protocol.KindError, got.Kind)
	require.True(t, strings.HasPrefix(got.S, "ERR unknown command 'asking'"))
}

// Given: 全槽拓扑开启
// When: CLUSTER KEYSLOT foo / MYID
// Then: 12182（真机互证）；MYID 40 位 hex
func Test_Cluster_when_Keyslot(t *testing.T) {
	r, _ := openClusterSetup(t, fullTopo(t, "127.0.0.1:7777"))
	got := dispatch(r, "CLUSTER", "KEYSLOT", "foo")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(12182), got.I)
	got = dispatch(r, "CLUSTER", "MYID")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Len(t, string(got.Bulk), 40)
}

// Given: 全槽拓扑开启
// When: 有连接上下文时 ASKING
// Then: +OK；标志一次性，Consume 后清除；ConnClosed 丢弃
func Test_Cluster_when_AskingLifecycle(t *testing.T) {
	_, asking := openClusterSetup(t, fullTopo(t, "127.0.0.1:7777"))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	ctx := network.ContextWithConn(context.Background(), c1)
	require.True(t, asking.Set(ctx))
	require.True(t, asking.Consume(ctx))
	require.False(t, asking.Consume(ctx))
	require.True(t, asking.Set(ctx))
	asking.ConnClosed(c1)
	require.False(t, asking.Consume(ctx))
}

// Given: 无连接上下文（单测直调）
// When: AskRegistry.Set/Consume
// Then: 返回 false，不 panic
func Test_Cluster_when_NoConn(t *testing.T) {
	asking := NewAskRegistry()
	require.False(t, asking.Set(context.Background()))
	require.False(t, asking.Consume(context.Background()))
}

// 防腐：disabled 下 intercept 不吞任何命令。
func Test_Cluster_when_InterceptPassthrough(t *testing.T) {
	r, _ := openClusterSetup(t, nil)
	got := dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, got)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
}

// Given: 3 节点静态拓扑，本节点 nodeA(0-5460)
// When: 访问 foo(slot 12182，归 nodeC)
// Then: MOVED 12182 127.0.0.1:7002（字节对标 moved.fixture）
func Test_Cluster_when_WrongNodeMoved(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	got := dispatch(r, "GET", "foo")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "MOVED 12182 127.0.0.1:7002", got.S)
	got = dispatch(r, "SET", "foo", "x")
	require.Equal(t, "MOVED 12182 127.0.0.1:7002", got.S)
}

// Given: 持有槽位的 key
// When: 正常读写
// Then: 直通无拦截
func Test_Cluster_when_OwnedPassthrough(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7002"))
	require.Equal(t,
		protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SET", "foo", "bar"))
	require.Equal(t, protocol.BulkOf("bar"), dispatch(r, "GET", "foo"))
}

// Given: 跨槽多 key
// When: 同一命令触及两槽
// Then: 单个 CROSSSLOT
func Test_Cluster_when_CrossSlot(t *testing.T) {
	r, _ := openClusterSetup(t, fullTopo(t, "127.0.0.1:7777"))
	got := dispatch(r, "MSET", "{a}x", "1", "{b}y", "2")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "CROSSSLOT Keys in request don't hash to the same slot", got.S)
}

// Given: MULTI 队列混入跨槽
// When: EXEC
// Then: 单个 CROSSSLOT，队列未执行
func Test_Cluster_when_ExecCrossSlot(t *testing.T) {
	r, _ := openClusterSetup(t, fullTopo(t, "127.0.0.1:7777"))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.Equal(t, "OK", dispatchConn(r, c1, "MULTI").S)
	require.Equal(t, "QUEUED", dispatchConn(r, c1, "SET", "{a}x", "1").S)
	require.Equal(t, "QUEUED", dispatchConn(r, c1, "SET", "{b}y", "2").S)
	got := dispatchConn(r, c1, "EXEC")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "CROSSSLOT Keys in request don't hash to the same slot", got.S)
	require.Equal(t, protocol.KindBulkString, dispatch(r, "GET", "{a}x").Kind)
	require.Nil(t, dispatch(r, "GET", "{a}x").Bulk)
}

// Given: MULTI 队列单槽未持有
// When: EXEC
// Then: 单个 MOVED
func Test_Cluster_when_ExecWrongNode(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.Equal(t, "OK", dispatchConn(r, c1, "MULTI").S)
	require.Equal(t, "QUEUED", dispatchConn(r, c1, "SET", "foo", "1").S)
	got := dispatchConn(r, c1, "EXEC")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "MOVED 12182 127.0.0.1:7002", got.S)
}

// Given: ASKING 置位 + 未持有槽 + 本地有 key（对标 ask.fixture §4）
// When: 同连接 GET
// Then: 放行服务本地值；下条命令恢复 MOVED（一次性）
func Test_Cluster_when_AskingServedLocally(t *testing.T) {
	r, store := openTestSetup(t)
	txnReg := RegisterTxn(r, nil)
	asking := NewAskRegistry()
	h := RegisterCluster(r, store, splitTopo(t, "127.0.0.1:7000"), asking)
	txnReg.PreExec = h.CheckExec
	// foo 归 nodeC(12182)；绕过 intercept 直接种 key（静态世界无迁移写入路径）。
	require.NoError(t, store.Set(context.Background(),
		datastruct.StringKey("foo"), datastruct.EncodeString([]byte("bar"), 0)))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.Equal(t, "OK", dispatchConn(r, c1, "ASKING").S)
	require.Equal(t, protocol.BulkOf("bar"), dispatchConn(r, c1, "GET", "foo"))
	require.Equal(t, "MOVED 12182 127.0.0.1:7002", dispatchConn(r, c1, "GET", "foo").S)
}

// Given: ASKING 置位 + 未持有槽 + 本地无 key（对标 ask.fixture §2 源端）
// When: 同连接 GET
// Then: ASK 12182 127.0.0.1:7002
func Test_Cluster_when_AskingMissingAsks(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.Equal(t, "OK", dispatchConn(r, c1, "ASKING").S)
	got := dispatchConn(r, c1, "GET", "foo")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ASK 12182 127.0.0.1:7002", got.S)
}

// Given: nodeC 视角持有 foo
// When: CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT
// Then: 计数与 key 名（对标 ask.fixture §5）
func Test_Cluster_when_KeysInSlot(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterTxn(r, nil)
	asking := NewAskRegistry()
	RegisterCluster(r, store, splitTopo(t, "127.0.0.1:7002"), asking)
	require.Equal(t,
		protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SET", "foo", "bar"))
	got := dispatch(r, "CLUSTER", "COUNTKEYSINSLOT", "12182", "10")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(1), got.I)
	got = dispatch(r, "CLUSTER", "GETKEYSINSLOT", "12182", "10")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, "foo", string(got.Elems[0].Bulk))
	got = dispatch(r, "CLUSTER", "COUNTKEYSINSLOT", "0", "10")
	require.Equal(t, int64(0), got.I)
}

// Given: 拓扑变更子命令
// When: 非法/静态不支持的参数
// Then: 真机措辞（7.2.6 实测）或静态拒绝
func Test_Cluster_when_TopologyMutation(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	selfID := string(dispatch(r, "CLUSTER", "MYID").Bulk)
	cases := map[string][]string{
		"FORGET":    {selfID},
		"REPLICATE": {selfID},
		"FAILOVER":  {},
	}
	wants := map[string]string{
		"FORGET":    "ERR I tried hard but I can't forget myself...",
		"REPLICATE": "ERR Can't replicate myself",
		"FAILOVER":  "ERR You should send CLUSTER FAILOVER to a replica",
	}
	for sub, args := range cases {
		got := dispatch(r, append([]string{"CLUSTER", sub}, args...)...)
		require.Equal(t, protocol.KindError, got.Kind, sub)
		require.Equal(t, wants[sub], got.S, sub)
	}
	got := dispatch(r, "CLUSTER", "SETSLOT", "12182", "MIGRATING", "someid")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR I'm not the owner of hash slot 12182", got.S)
	got = dispatch(r, "CLUSTER", "SETSLOT", "12182", "IMPORTING", "someid")
	require.Equal(t, "ERR Static cluster topology does not support CLUSTER SETSLOT", got.S)
	got = dispatch(r, "CLUSTER", "BOGUS")
	require.True(t, strings.HasPrefix(got.S, "ERR unknown subcommand"))
}
