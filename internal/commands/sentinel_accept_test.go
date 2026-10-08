package commands

// M7 Sentinel 最小发现版验收（#42）：进程内哨兵服（main.go 接线复刻）+
// stub 数据节点（+OK 应答），断言与原生 7.2.6 真机一致的最小形状
// （fixtures 见 testdata/sentinel/*.redis，录制自 7.2.6 一主一从一哨兵）。

import (
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/sentinel"
	"github.com/stretchr/testify/require"
)

// openSentinelAccept 按 main.go 顺序接线哨兵口：DefaultRouter、
// RegisterPubSub、RegisterSentinel；探活 downAfter=0 关闭。
// spec quorum=2（不断言默认 1，展示真实透传值）；预置一条 gossip
// 对端，覆盖 SENTINEL sentinels 含 peers。
func openSentinelAccept(t testing.TB, masterAddr, slaveAddr string) string {
	t.Helper()
	reg := sentinel.NewRegistry([]sentinel.NodeSpec{
		{Name: "mymaster", MasterAddr: masterAddr, Slaves: []string{slaveAddr}, Quorum: 2},
	}, 0)
	mh, mp, _ := net.SplitHostPort(masterAddr)
	port, _ := strconv.Atoi(mp)
	reg.Peers.Upsert(sentinel.Hello{IP: "127.0.0.1", Port: "26380", RunID: "peer1",
		Epoch: 1, Master: "mymaster", MasterIP: mh, MasterPort: port, MasterEpoch: 1})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	selfAddr := ln.Addr().String()
	r := network.DefaultRouter()
	pub := RegisterPubSub(r)
	RegisterSentinel(r, reg, selfAddr, pub)
	srv := network.NewServer(r)
	srv.OnConnClose(func(c net.Conn) { pub.ConnClosed(c) })
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return selfAddr
}

// fixtureFields 读 redis-cli 录制的平铺 k/v（奇行 key 偶行 value），返回 key 集。
func fixtureFields(t testing.TB, name string) map[string]bool {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "sentinel", name))
	require.NoError(t, err)
	out := map[string]bool{}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	for i := 0; i < len(lines); i += 2 {
		out[strings.TrimSpace(lines[i])] = true
	}
	return out
}

func flatKeys(v protocol.Value) map[string]bool {
	out := map[string]bool{}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		out[string(v.Elems[i].Bulk)] = true
	}
	return out
}

// Given: 原生 masters.redis（failover 前录制）
// When: 查 SENTINEL masters
// Then: 首个 master 的 key 集是真机字段的子集，且含发现必需的 6 个
func TestSentinelAccept_MastersKeys(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	ac := dialAccept(t, openSentinelAccept(t, master, slave))
	ac.do(t, "SENTINEL", "masters")
	got := ac.value(t)
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	keys := flatKeys(got.Elems[0])
	native := fixtureFields(t, "masters.redis")
	for _, k := range []string{"name", "ip", "port", "quorum", "down-after-milliseconds", "flags"} {
		require.True(t, keys[k], "missing key %q", k)
	}
	for k := range keys {
		require.True(t, native[k], "key %q not in native fixture", k)
	}
	require.Equal(t, "2", string(flatVal(t, got.Elems[0], "quorum").Bulk))
}

func flatVal(t testing.TB, v protocol.Value, key string) protocol.Value {
	t.Helper()
	for i := 0; i+1 < len(v.Elems); i += 2 {
		if string(v.Elems[i].Bulk) == key {
			return v.Elems[i+1]
		}
	}
	t.Fatalf("key %q not found", key)
	return protocol.Value{}
}

// Given: 原生 slaves.redis
// When: 查 SENTINEL slaves mymaster
// Then: key 集是真机字段的子集，且含 name/ip/port/flags
func TestSentinelAccept_SlavesKeys(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	ac := dialAccept(t, openSentinelAccept(t, master, slave))
	ac.do(t, "SENTINEL", "slaves", "mymaster")
	got := ac.value(t)
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	keys := flatKeys(got.Elems[0])
	native := fixtureFields(t, "slaves.redis")
	for _, k := range []string{"name", "ip", "port", "flags"} {
		require.True(t, keys[k], "missing key %q", k)
	}
	for k := range keys {
		require.True(t, native[k], "key %q not in native fixture", k)
	}
}

// Given: stub 主从地址
// When: SENTINEL get-master-addr-by-name / INFO sentinel
// Then: [ip port] 与真机同形；INFO 含 sentinel_masters:1（对标 info.redis）
func TestSentinelAccept_AddrAndInfo(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	ac := dialAccept(t, openSentinelAccept(t, master, slave))
	ac.do(t, "SENTINEL", "get-master-addr-by-name", "mymaster")
	got := ac.value(t)
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	host, port, _ := net.SplitHostPort(master)
	require.Equal(t, host, string(got.Elems[0].Bulk))
	require.Equal(t, port, string(got.Elems[1].Bulk))

	ac.do(t, "INFO", "sentinel")
	info := string(ac.value(t).Bulk)
	require.Contains(t, info, "sentinel_masters:1")
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "sentinel", "info.redis"))
	require.NoError(t, err)
	require.Contains(t, string(raw), "sentinel_masters:1")
}

// Given: A 连接订阅 +switch-master，B 连接执行 failover
// When: SENTINEL failover mymaster
// Then: +OK；A 收到 message 载荷与真机同形（对标 switch-master.redis）；主翻转
func TestSentinelAccept_FailoverSwitchMaster(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	addr := openSentinelAccept(t, master, slave)
	sub := dialAccept(t, addr)
	sub.do(t, "SUBSCRIBE", "+switch-master")
	conf := sub.value(t)
	require.Equal(t, protocol.KindArray, conf.Kind)

	ctl := dialAccept(t, addr)
	ctl.do(t, "SENTINEL", "failover", "mymaster")
	require.Equal(t, "+OK\r\n", ctl.line(t))

	msg := sub.value(t)
	require.Equal(t, protocol.KindArray, msg.Kind)
	require.Len(t, msg.Elems, 3)
	require.Equal(t, "message", string(msg.Elems[0].Bulk))
	require.Equal(t, "+switch-master", string(msg.Elems[1].Bulk))
	oldHost, oldPort, _ := net.SplitHostPort(master)
	newHost, newPort, _ := net.SplitHostPort(slave)
	require.Equal(t,
		"mymaster "+oldHost+" "+oldPort+" "+newHost+" "+newPort,
		string(msg.Elems[2].Bulk))

	ctl.do(t, "SENTINEL", "get-master-addr-by-name", "mymaster")
	got := ctl.value(t)
	require.Equal(t, newHost, string(got.Elems[0].Bulk))
	require.Equal(t, newPort, string(got.Elems[1].Bulk))
}

// Given: 原生 sentinels.redis + 预置 gossip 对端
// When: SENTINEL sentinels mymaster
// Then: 含自己 + 对端两条；每条 key 集是真机字段的子集，且含 name/ip/port/flags
func TestSentinelAccept_SentinelsKeys(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	ac := dialAccept(t, openSentinelAccept(t, master, slave))
	ac.do(t, "SENTINEL", "sentinels", "mymaster")
	got := ac.value(t)
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	native := fixtureFields(t, "sentinels.redis")
	foundPeer := false
	for _, e := range got.Elems {
		keys := flatKeys(e)
		for _, k := range []string{"name", "ip", "port", "flags"} {
			require.True(t, keys[k], "missing key %q", k)
		}
		for k := range keys {
			require.True(t, native[k], "key %q not in native fixture", k)
		}
		if string(flatVal(t, e, "port").Bulk) == "26380" {
			foundPeer = true
		}
	}
	require.True(t, foundPeer, "gossip peer missing from sentinels")
}

// Given: 静态拓扑最小版
// When: SENTINEL set/remove/reset
// Then: 一律静态拒绝（spec §3 非目标）
func TestSentinelAccept_StaticRefused(t *testing.T) {
	master, slave := startSentinelStub(t), startSentinelStub(t)
	ac := dialAccept(t, openSentinelAccept(t, master, slave))
	for _, args := range [][]string{
		{"SENTINEL", "set", "mymaster", "quorum", "2"},
		{"SENTINEL", "remove", "mymaster"},
		{"SENTINEL", "reset", "mymaster"},
	} {
		ac.do(t, args...)
		line := ac.line(t)
		require.True(t, strings.HasPrefix(line, "-ERR"), "args %v: %q", args, line)
		require.Contains(t, line, "Static sentinel")
	}
}
