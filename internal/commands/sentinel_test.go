package commands

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/sentinel"
	"github.com/stretchr/testify/require"
)

func newSentinelRouter(master, slave string, pub *PubSubRegistry) (*network.Router, *sentinel.Registry) {
	reg := sentinel.NewRegistry([]sentinel.NodeSpec{
		{Name: "mymaster", MasterAddr: master, Slaves: []string{slave}},
	}, 5*time.Second)
	r := network.DefaultRouter()
	RegisterSentinel(r, reg, "127.0.0.1:26379", pub, "test-runid")
	return r, reg
}

func sentinelCmd(args ...string) protocol.Value {
	elems := make([]protocol.Value, 0, len(args))
	for _, a := range args {
		elems = append(elems, protocol.BulkOf(a))
	}
	return protocol.ArrayOf(elems...)
}

func flatMap(v protocol.Value) map[string]string {
	out := map[string]string{}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		out[string(v.Elems[i].Bulk)] = string(v.Elems[i+1].Bulk)
	}
	return out
}

func TestSentinel_GetMasterAddr(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "get-master-addr-by-name", "mymaster"))
	require.Equal(t, protocol.KindArray, reply.Kind)
	require.Len(t, reply.Elems, 2)
	require.Equal(t, "127.0.0.1", string(reply.Elems[0].Bulk))
	require.Equal(t, "6380", string(reply.Elems[1].Bulk))
}

func TestSentinel_GetMasterAddrUnknown(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "get-master-addr-by-name", "nope"))
	require.Equal(t, protocol.KindError, reply.Kind)
	require.Contains(t, reply.S, "No such master")
}

func TestSentinel_MastersShape(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "masters"))
	require.Equal(t, protocol.KindArray, reply.Kind)
	require.Len(t, reply.Elems, 1)
	m := flatMap(reply.Elems[0])
	require.Equal(t, "mymaster", m["name"])
	require.Equal(t, "127.0.0.1", m["ip"])
	require.Equal(t, "6380", m["port"])
	require.Equal(t, "1", m["quorum"])
	require.Equal(t, "5000", m["down-after-milliseconds"])
	require.Contains(t, m["flags"], "master")
}

func TestSentinel_SlavesShape(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "slaves", "mymaster"))
	require.Equal(t, protocol.KindArray, reply.Kind)
	require.Len(t, reply.Elems, 1)
	m := flatMap(reply.Elems[0])
	require.Equal(t, "127.0.0.1", m["ip"])
	require.Equal(t, "6381", m["port"])
	require.Contains(t, m["flags"], "slave")
}

func TestSentinel_StaticRefused(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	for _, args := range [][]string{
		{"SENTINEL", "set", "mymaster", "quorum", "2"},
		{"SENTINEL", "remove", "mymaster"},
		{"SENTINEL", "reset", "mymaster"},
	} {
		reply := r.Dispatch(context.Background(), sentinelCmd(args...))
		require.Equal(t, protocol.KindError, reply.Kind, "%v", args)
		require.Contains(t, reply.S, "Static sentinel")
	}
}

func TestSentinel_FailoverUnknown(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "failover", "nope"))
	require.Equal(t, protocol.KindError, reply.Kind)
}

func TestSentinel_FailoverFlips(t *testing.T) {
	oldMaster := startSentinelStub(t)
	target := startSentinelStub(t)
	r, reg := newSentinelRouter(oldMaster, target, nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "failover", "mymaster"))
	require.Equal(t, protocol.KindSimpleString, reply.Kind)
	require.Equal(t, "OK", reply.S)
	_, port, _ := reg.GetMasterAddr("mymaster")
	_, wantPortStr, _ := net.SplitHostPort(target)
	wantPort, err := strconv.Atoi(wantPortStr)
	require.NoError(t, err)
	require.Equal(t, wantPort, port)
}

func startSentinelStub(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					if _, err := c.Read(buf); err != nil {
						return
					}
					if _, err := c.Write([]byte("+OK\r\n")); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return ln.Addr().String()
}

func TestSentinel_MyID(t *testing.T) {
	r := network.DefaultRouter()
	reg := sentinel.NewRegistry(nil, 5*time.Second)
	RegisterSentinel(r, reg, "127.0.0.1:26379", nil, "myrunid42")
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "myid"))
	require.Equal(t, protocol.BulkOf("myrunid42"), reply)
	bad := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "myid", "x"))
	require.Equal(t, protocol.KindError, bad.Kind)
}

func TestSentinel_ReplicasIsSlaveAlias(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	a := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "slaves", "mymaster"))
	b := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "replicas", "mymaster"))
	require.Equal(t, a, b)
	unknown := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "replicas", "nope"))
	require.Equal(t, protocol.KindError, unknown.Kind)
	require.Contains(t, unknown.S, "No such master")
}

func TestSentinel_CkQuorum(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	ok := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "ckquorum", "mymaster"))
	require.Equal(t, protocol.KindSimpleString, ok.Kind)
	require.Equal(t, "OK", ok.S)
	unknown := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "ckquorum", "nope"))
	require.Equal(t, protocol.KindError, unknown.Kind)
	r2 := network.DefaultRouter()
	reg2 := sentinel.NewRegistry([]sentinel.NodeSpec{
		{Name: "big", MasterAddr: "127.0.0.1:6380", Quorum: 5},
	}, 5*time.Second)
	RegisterSentinel(r2, reg2, "127.0.0.1:26379", nil, "runid")
	short := r2.Dispatch(context.Background(), sentinelCmd("SENTINEL", "ckquorum", "big"))
	require.Equal(t, protocol.KindError, short.Kind)
}

func TestSentinel_MonitorStaticTopo(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	reply := r.Dispatch(context.Background(), sentinelCmd("SENTINEL", "monitor", "m2", "127.0.0.1", "6380", "2"))
	require.Equal(t, protocol.KindError, reply.Kind)
	require.Contains(t, reply.S, "MONITOR")
}
