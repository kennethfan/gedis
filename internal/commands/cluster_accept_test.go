package commands

// M7 Cluster 最小行为集验收（#35 Q2）：双实例 B/C 对半分片，断言与原生
// 7.2.6 真机一致的最小行为（fixtures 见 testdata/cluster/*.redis）。
//
// 启动方式：进程内（net.Listen 127.0.0.1:0 + main.go 接线复刻），
// 与 repl_live_test.go 同模式；不依赖外部 redis/docker。

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// acceptServer 是单 Gedis 实例：store + 全命令注册 + Cluster 接线。
type acceptServer struct {
	addr string
	srv  *network.Server
	hub  *replication.Hub
}

// openAcceptServer 按 main.go 顺序接线：各 Register*、RegisterTxn、
// RegisterCluster（含 PreExec + OnConnClose）。specs 必须包含 selfAddr。
func openAcceptServer(t testing.TB, ln net.Listener, specs []cluster.NodeSpec) *acceptServer {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false, Fsync: storage.FsyncNo, Hub: hub})
	require.NoError(t, store.Open())
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterHash(r, store)
	RegisterList(r, store, nil)
	RegisterSet(r, store)
	RegisterZSet(r, store)
	RegisterScan(r, store)
	RegisterGeneric(r, store)
	RegisterWriteCommands(r)
	txnReg := RegisterTxn(r, hub)
	RegisterPubSub(r)
	RegisterLua(r, 0)
	selfAddr := ln.Addr().String()
	topo, err := cluster.Build(selfAddr, specs)
	require.NoError(t, err)
	asking := NewAskRegistry()
	clusterH := RegisterCluster(r, store, topo, asking)
	txnReg.PreExec = clusterH.CheckExec
	srv := network.NewServer(r)
	srv.OnConnClose(func(c net.Conn) {
		txnReg.ConnClosed(c)
		asking.ConnClosed(c)
	})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		hub.Close()
		_ = store.Close()
	})
	return &acceptServer{addr: selfAddr, srv: srv, hub: hub}
}

// openSplitPair 起 B/C 对半分片：B 持 0-8191，C 持 8192-16383。
// 先 listen 拿地址，再 Build（Build 要求 selfAddr 命中 specs）。
func openSplitPair(t testing.TB) (b, c *acceptServer) {
	t.Helper()
	lnB, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	lnC, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addrB, addrC := lnB.Addr().String(), lnC.Addr().String()
	specs := []cluster.NodeSpec{
		{Addr: addrB, Ranges: [][2]int{{0, 8191}}},
		{Addr: addrC, Ranges: [][2]int{{8192, 16383}}},
	}
	return openAcceptServer(t, lnB, specs), openAcceptServer(t, lnC, specs)
}

// acceptConn 是单连接 RESP 客户端：发命令、按行读（精确字节断言用）。
type acceptConn struct {
	conn net.Conn
	rd   *bufio.Reader
}

func dialAccept(t testing.TB, addr string) *acceptConn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	ac := &acceptConn{conn: conn, rd: bufio.NewReader(conn)}
	t.Cleanup(func() { _ = conn.Close() })
	return ac
}

func (ac *acceptConn) do(t testing.TB, args ...string) {
	t.Helper()
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	_, err := ac.conn.Write([]byte(sb.String()))
	require.NoError(t, err)
}

// line 读单行回复（+/-/:/$, 整行含 \r\n），精确字节断言用。
func (ac *acceptConn) line(t testing.TB) string {
	t.Helper()
	s, err := ac.rd.ReadString('\n')
	require.NoError(t, err)
	return s
}

// value 读完整回复（bulk/array/shards 等多行结构用）。
func (ac *acceptConn) value(t testing.TB) protocol.Value {
	t.Helper()
	v, err := protocol.Decode(ac.rd)
	require.NoError(t, err)
	return v
}

// bKey 找一个落在 B 分片（slot<8192）的 key；foo→12182 固定落 C（真机交叉验证）。
func bKey(t testing.TB) string {
	t.Helper()
	require.Equal(t, 12182, cluster.Slot("foo"))
	for i := 0; ; i++ {
		k := fmt.Sprintf("bkey%d", i)
		if cluster.Slot(k) < 8192 {
			return k
		}
		require.Less(t, i, 1<<20, "no B-slot key found")
	}
}

// Given: B/C 对半分片
// When: 向错分片（B）写 foo（slot 12182，归 C）
// Then: MOVED 回复与真机字节精确（对标 moved.redis §1）
func Test_ClusterAccept_MovedExactBytes(t *testing.T) {
	b, c := openSplitPair(t)
	ac := dialAccept(t, b.addr)
	ac.do(t, "SET", "foo", "v")
	require.Equal(t, "-MOVED 12182 "+c.addr+"\r\n", ac.line(t))
}

// Given: B/C 对半分片，foo 只在 C 上
// When: 在 B 读 foo 得 MOVED → 到 C 读写（-c 跟随的人工版）
// Then: C 侧读写正常；B 侧持续 MOVED
func Test_ClusterAccept_RedirectFollow(t *testing.T) {
	b, c := openSplitPair(t)
	cb, cc := dialAccept(t, b.addr), dialAccept(t, c.addr)

	cc.do(t, "SET", "foo", "v")
	require.Equal(t, "+OK\r\n", cc.line(t))

	cb.do(t, "GET", "foo")
	require.Equal(t, "-MOVED 12182 "+c.addr+"\r\n", cb.line(t))

	cc.do(t, "GET", "foo")
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte("v")}, cc.value(t))

	cb.do(t, "GET", "foo")
	require.Equal(t, "-MOVED 12182 "+c.addr+"\r\n", cb.line(t))
}

// Given: B/C 对半分片
// When: 在 C 上 EXEC 跨槽队列（foo 归 C，bkey 归 B）
// Then: 单个 CROSSSLOT 错误（对标真机：EXEC 预扫 abort，非 EXECABORT）
func Test_ClusterAccept_ExecCrossSlotSingleError(t *testing.T) {
	_, c := openSplitPair(t)
	bk := bKey(t)
	cc := dialAccept(t, c.addr)
	cc.do(t, "MULTI")
	require.Equal(t, "+OK\r\n", cc.line(t))
	cc.do(t, "SET", "foo", "a")
	require.Equal(t, "+QUEUED\r\n", cc.line(t))
	cc.do(t, "SET", bk, "b")
	require.Equal(t, "+QUEUED\r\n", cc.line(t))
	cc.do(t, "EXEC")
	require.Equal(t, "-CROSSSLOT Keys in request don't hash to the same slot\r\n", cc.line(t))
}

// Given: B/C 对半分片
// When: 在 C 上 MSET 跨槽（foo 归 C，bkey 归 B）
// Then: CROSSSLOT 错误（与原生单分片内多 key 语义一致）
func Test_ClusterAccept_MsetCrossSlot(t *testing.T) {
	_, c := openSplitPair(t)
	bk := bKey(t)
	cc := dialAccept(t, c.addr)
	cc.do(t, "MSET", "foo", "1", bk, "2")
	require.Equal(t, "-CROSSSLOT Keys in request don't hash to the same slot\r\n", cc.line(t))
}

// Given: B/C 对半分片，foo 在 B、C 均不存在
// When: 同连接 ASKING → GET（ASK）→ GET（MOVED，一次性消费）
// Then: ASK 链与真机一致（对标 ask.redis §2）；新连接直接 MOVED（标志不泄漏）
func Test_ClusterAccept_AskOneShot(t *testing.T) {
	b, c := openSplitPair(t)
	ac := dialAccept(t, b.addr)
	ac.do(t, "ASKING")
	require.Equal(t, "+OK\r\n", ac.line(t))
	ac.do(t, "GET", "foo")
	require.Equal(t, "-ASK 12182 "+c.addr+"\r\n", ac.line(t))
	ac.do(t, "GET", "foo")
	require.Equal(t, "-MOVED 12182 "+c.addr+"\r\n", ac.line(t))

	fresh := dialAccept(t, b.addr)
	fresh.do(t, "GET", "foo")
	require.Equal(t, "-MOVED 12182 "+c.addr+"\r\n", fresh.line(t))
}

// Given: B/C 对半分片
// When: 查四件套
// Then: 形状与原生一致：INFO 含 cluster_state:ok；NODES 含双节点与段记法；
// SLOTS 两段；SHARDS 含双地址
func Test_ClusterAccept_TopologyShapes(t *testing.T) {
	b, c := openSplitPair(t)
	ac := dialAccept(t, b.addr)

	ac.do(t, "CLUSTER", "INFO")
	info := string(ac.value(t).Bulk)
	require.Contains(t, info, "cluster_state:ok")
	require.Contains(t, info, "cluster_known_nodes:2")

	ac.do(t, "CLUSTER", "NODES")
	nodes := string(ac.value(t).Bulk)
	require.Contains(t, nodes, b.addr)
	require.Contains(t, nodes, c.addr)
	require.Contains(t, nodes, "0-8191")
	require.Contains(t, nodes, "8192-16383")

	ac.do(t, "CLUSTER", "SLOTS")
	slots := ac.value(t)
	require.Equal(t, protocol.KindArray, slots.Kind)
	require.Len(t, slots.Elems, 2)
	require.Equal(t, int64(0), slots.Elems[0].Elems[0].I)
	require.Equal(t, int64(8191), slots.Elems[0].Elems[1].I)
	require.Equal(t, int64(8192), slots.Elems[1].Elems[0].I)
	require.Equal(t, int64(16383), slots.Elems[1].Elems[1].I)

	ac.do(t, "CLUSTER", "SHARDS")
	shards := ac.value(t)
	_, portB, err := net.SplitHostPort(b.addr)
	require.NoError(t, err)
	_, portC, err := net.SplitHostPort(c.addr)
	require.NoError(t, err)
	require.True(t, valueContains(shards, "127.0.0.1"))
	require.True(t, valueContains(shards, "master"))
	require.True(t, valueContains(shards, portB))
	require.True(t, valueContains(shards, portC))
}

// valueContains 递归在回复树中找子串（SHARDS 嵌套 map/array 结构用）。
func valueContains(v protocol.Value, sub string) bool {
	if strings.Contains(v.S, sub) || strings.Contains(string(v.Bulk), sub) {
		return true
	}
	if v.Kind == protocol.KindInteger && strconv.FormatInt(v.I, 10) == sub {
		return true
	}
	for _, e := range v.Elems {
		if valueContains(e, sub) {
			return true
		}
	}
	for _, p := range v.Pairs {
		if valueContains(p.K, sub) || valueContains(p.V, sub) {
			return true
		}
	}
	return false
}
