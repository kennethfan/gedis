package commands

import (
	"bufio"
	"context"
	"net"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WM: 每客户端一根 pipe——服务端端进 ctx，客户端端做 IO；直写与推送才能配对。
func openPubSubSetup(t testing.TB) (*network.Router, *PubSubRegistry, net.Conn, net.Conn, net.Conn, net.Conn) {
	t.Helper()
	r := network.DefaultRouter()
	reg := RegisterPubSub(r)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	return r, reg, srvA, cliA, srvB, cliB
}

func dispatchPub(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

func readPushed(t testing.TB, conn net.Conn) protocol.Value {
	t.Helper()
	v, err := protocol.Decode(bufio.NewReader(conn))
	require.NoError(t, err)
	return v
}

func confirmKind(channel string, count int64, verb string) protocol.Value {
	return protocol.ArrayOf(
		protocol.BulkOf(verb),
		protocol.BulkOf(channel),
		protocol.Value{Kind: protocol.KindInteger, I: count},
	)
}

// WM: SUBSCRIBE c1 c2 → 线上依次 conf(c1)、conf(c2)（后者为 handler 返回值）
func Test_PubSub_when_SubscribeConfirmations(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	type result struct{ v protocol.Value }
	ch := make(chan result, 1)
	go func() { ch <- result{dispatchPub(r, srvA, "SUBSCRIBE", "c1", "c2")} }()
	require.Equal(t, confirmKind("c1", 1, "subscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("c2", 2, "subscribe"), (<-ch).v)
}

// WM: 重复订阅重发确认且计数不变
func Test_PubSub_when_DuplicateSubscribe(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c1"))
	require.Equal(t, confirmKind("c1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c1"))
}

// WM: A 订阅 c1；B PUBLISH c1 hello → B 得 :1；A 收到 [message, c1, hello]
func Test_PubSub_when_PublishDelivers(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c1"))
	got := dispatchPub(r, srvB, "PUBLISH", "c1", "hello")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("message"),
		protocol.BulkOf("c1"),
		protocol.BulkOf("hello"),
	), readPushed(t, cliA))
}

// WM: A 订阅 c1；B PUBLISH c1 / PUBLISH noone → :1 / :0
func Test_PubSub_when_PublishCounts(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	require.Equal(t, int64(1), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
	require.Equal(t, int64(0), dispatchPub(r, srvB, "PUBLISH", "noone", "v").I)
	_ = readPushed(t, cliA)
}

// WM: A UNSUBSCRIBE c1 → [unsubscribe, c1, 0]；再 PUBLISH 得 :0 且无推送
func Test_PubSub_when_UnsubscribeStopsDelivery(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	require.Equal(t, confirmKind("c1", 0, "unsubscribe"), dispatchPub(r, srvA, "UNSUBSCRIBE", "c1"))
	require.Equal(t, int64(0), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
	cliA.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := protocol.Decode(bufio.NewReader(cliA))
	require.Error(t, err)
}

// WM: SUB c1 c2 c3 后裸 UNSUBSCRIBE → 逆序 c3:2、c2:1、c1:0（与 7.2.6 一致）
func Test_PubSub_when_UnsubscribeAllReversed(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	type result struct{ v protocol.Value }
	sub := make(chan result, 1)
	go func() { sub <- result{dispatchPub(r, srvA, "SUBSCRIBE", "c1", "c2", "c3")} }()
	require.Equal(t, confirmKind("c1", 1, "subscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("c2", 2, "subscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("c3", 3, "subscribe"), (<-sub).v)
	unsub := make(chan result, 1)
	go func() { unsub <- result{dispatchPub(r, srvA, "UNSUBSCRIBE")} }()
	require.Equal(t, confirmKind("c3", 2, "unsubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("c2", 1, "unsubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("c1", 0, "unsubscribe"), (<-unsub).v)
}

// WM: 零订阅裸 UNSUBSCRIBE → 单个 [unsubscribe nil 0]
func Test_PubSub_when_UnsubscribeEmpty(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("unsubscribe"),
		protocol.Value{Kind: protocol.KindBulkString},
		protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, srvA, "UNSUBSCRIBE"))
}

// WM: 订阅态 PING → [pong, ""] / [pong, hello]；双参报 ping-arity 错
func Test_PubSub_when_PingInSubMode(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("")),
		dispatchPub(r, srvA, "PING"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("hello")),
		dispatchPub(r, srvA, "PING", "hello"))
	require.Equal(t, "ERR wrong number of arguments for 'ping' command",
		dispatchPub(r, srvA, "PING", "a", "b").S)
}

// WM: 订阅态拒绝非放行命令；退订至零后恢复正常分发
func Test_PubSub_when_SubModeRejectsOthers(t *testing.T) {
	r, _, srvA, _, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	got := dispatchPub(r, srvA, "PUBLISH", "c1", "hi")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Can't execute 'publish'")
	unknown := dispatchPub(r, srvA, "NOSUCHCMD", "a")
	require.Equal(t, protocol.KindError, unknown.Kind)
	require.Contains(t, unknown.S, "Can't execute 'nosuchcmd'")
	require.Equal(t, int64(1), dispatchPub(r, srvB, "PUBLISH", "c1", "hi").I)
	dispatchPub(r, srvA, "UNSUBSCRIBE", "c1")
	after := dispatchPub(r, srvA, "NOSUCHCMD", "a")
	require.Equal(t, protocol.KindError, after.Kind)
	require.Contains(t, after.S, "unknown command")
}

// WM: A 订阅后断开（服务端清理）；B PUBLISH → :0
func Test_PubSub_when_ConnClosedCleansUp(t *testing.T) {
	r, reg, srvA, _, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	reg.ConnClosed(srvA)
	require.Equal(t, int64(0), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
}
