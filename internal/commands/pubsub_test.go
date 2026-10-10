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

// WM: SUB c1 后 PSUBSCRIBE p* → 混合计数 2；重复 PSUB 计数不变
func Test_PubSub_when_PsubscribeMixedCount(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c1"))
	type result struct{ v protocol.Value }
	ch := make(chan result, 1)
	go func() { ch <- result{dispatchPub(r, srvA, "PSUBSCRIBE", "p*")} }()
	require.Equal(t, confirmKind("p*", 2, "psubscribe"), (<-ch).v)
	_ = cliA
	require.Equal(t, confirmKind("p*", 2, "psubscribe"), dispatchPub(r, srvA, "PSUBSCRIBE", "p*"))
}

// WM: A PSUBSCRIBE c*；B PUBLISH c1 v → :1；A 收到 [pmessage, c*, c1, v]
func Test_PubSub_when_PatternDelivers(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c*", 1, "psubscribe"), dispatchPub(r, srvA, "PSUBSCRIBE", "c*"))
	require.Equal(t, int64(1), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("pmessage"),
		protocol.BulkOf("c*"),
		protocol.BulkOf("c1"),
		protocol.BulkOf("v"),
	), readPushed(t, cliA))
	require.Equal(t, int64(0), dispatchPub(r, srvB, "PUBLISH", "zzz", "v").I)
}

// WM: 同连接频道+pattern 双命中 → PUBLISH :2，先 message 后 pmessage（真机顺序）
func Test_PubSub_when_ChannelAndPatternBothHit(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	dispatchPub(r, srvA, "PSUBSCRIBE", "c*")
	require.Equal(t, int64(2), dispatchPub(r, srvB, "PUBLISH", "c1", "hi").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("message"), protocol.BulkOf("c1"), protocol.BulkOf("hi"),
	), readPushed(t, cliA))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("pmessage"), protocol.BulkOf("c*"), protocol.BulkOf("c1"), protocol.BulkOf("hi"),
	), readPushed(t, cliA))
}

// WM: PSUB a* b* 后裸 PUNSUBSCRIBE → 逆序 b*:1、a*:0；零 pattern 裸退订回 [punsubscribe nil 0]
func Test_PubSub_when_PunsubscribeAllReversed(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	type result struct{ v protocol.Value }
	sub := make(chan result, 1)
	go func() { sub <- result{dispatchPub(r, srvA, "PSUBSCRIBE", "a*", "b*")} }()
	require.Equal(t, confirmKind("a*", 1, "psubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("b*", 2, "psubscribe"), (<-sub).v)
	unsub := make(chan result, 1)
	go func() { unsub <- result{dispatchPub(r, srvA, "PUNSUBSCRIBE")} }()
	require.Equal(t, confirmKind("b*", 1, "punsubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("a*", 0, "punsubscribe"), (<-unsub).v)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("punsubscribe"),
		protocol.Value{Kind: protocol.KindBulkString},
		protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, srvA, "PUNSUBSCRIBE"))
}

// WM: UNSUB/PUNSUB 命名空间独立：裸 UNSUB 只清频道（仍订阅态），裸 PUNSUB 只清 pattern（退回正常态）
func Test_PubSub_when_UnsubNamespacesIndependent(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	dispatchPub(r, srvA, "PSUBSCRIBE", "p*")
	require.Equal(t, confirmKind("c1", 1, "unsubscribe"), dispatchPub(r, srvA, "UNSUBSCRIBE"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("")),
		dispatchPub(r, srvA, "PING"))
	require.Equal(t, confirmKind("p*", 0, "punsubscribe"), dispatchPub(r, srvA, "PUNSUBSCRIBE"))
	require.Equal(t, "PONG", dispatchPub(r, srvA, "PING").S)
}

// WM: 全退订后重订阅仍可投递（sender 重建；回归：曾往已 close 的 ch 发送 panic）
func Test_PubSub_when_ResubscribeAfterFullUnsub(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	dispatchPub(r, srvA, "UNSUBSCRIBE", "c1")
	require.Equal(t, confirmKind("c1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c1"))
	require.Equal(t, int64(1), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("message"), protocol.BulkOf("c1"), protocol.BulkOf("v"),
	), readPushed(t, cliA))
}

func emptyArray() protocol.Value {
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{}}
}

// newPubSubConn 额外开一根 pipe 作非订阅查询连接（订阅态连接发 PUBSUB 会被拦截）。
func newPubSubConn(t testing.TB) net.Conn {
	t.Helper()
	srv, cli := net.Pipe()
	t.Cleanup(func() { srv.Close(); cli.Close() })
	return srv
}

// WM: CHANNELS 空表回 *0；有订阅时列全部普通频道（字典序，真机为 dict 序——
// 顺序分歧已在 ledger 记录），pattern 订阅名不出现，跨连接去重
func Test_PubSub_when_PubsubChannels(t *testing.T) {
	r, _, srvA, _, srvB, _ := openPubSubSetup(t)
	q := newPubSubConn(t)
	require.Equal(t, emptyArray(), dispatchPub(r, q, "PUBSUB", "CHANNELS"))
	dispatchPub(r, srvA, "SUBSCRIBE", "c2")
	dispatchPub(r, srvB, "SUBSCRIBE", "c1")
	dispatchPub(r, srvB, "PSUBSCRIBE", "p*")
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("c1"), protocol.BulkOf("c2")),
		dispatchPub(r, q, "PUBSUB", "CHANNELS"))
}

// WM: CHANNELS 带 glob pattern 过滤（stringmatchlen 语义）
func Test_PubSub_when_PubsubChannelsPattern(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	q := newPubSubConn(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "alpha")
	dispatchPub(r, srvA, "SUBSCRIBE", "beta")
	dispatchPub(r, srvA, "SUBSCRIBE", "zeta")
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("alpha")),
		dispatchPub(r, q, "PUBSUB", "CHANNELS", "a*"))
	require.Equal(t, emptyArray(), dispatchPub(r, q, "PUBSUB", "CHANNELS", "zzz*"))
}

// WM: NUMSUB 扁平 [ch,n,...]；未订阅回 0；pattern 订阅不计；无参回 *0
func Test_PubSub_when_PubsubNumsub(t *testing.T) {
	r, _, srvA, _, srvB, _ := openPubSubSetup(t)
	q := newPubSubConn(t)
	require.Equal(t, emptyArray(), dispatchPub(r, q, "PUBSUB", "NUMSUB"))
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	dispatchPub(r, srvB, "PSUBSCRIBE", "c1")
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("c1"), protocol.Value{Kind: protocol.KindInteger, I: 1},
		protocol.BulkOf("nope"), protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, q, "PUBSUB", "NUMSUB", "c1", "nope"))
}

// WM: NUMPAT 是全局计数（所有连接 patterns 之和），退订递减
func Test_PubSub_when_PubsubNumpat(t *testing.T) {
	r, _, srvA, _, srvB, _ := openPubSubSetup(t)
	q := newPubSubConn(t)
	require.Equal(t, int64(0), dispatchPub(r, q, "PUBSUB", "NUMPAT").I)
	dispatchPub(r, srvA, "PSUBSCRIBE", "a*")
	dispatchPub(r, srvB, "PSUBSCRIBE", "b*")
	require.Equal(t, int64(2), dispatchPub(r, q, "PUBSUB", "NUMPAT").I)
	dispatchPub(r, srvA, "PUNSUBSCRIBE", "a*")
	require.Equal(t, int64(1), dispatchPub(r, q, "PUBSUB", "NUMPAT").I)
}

// WM: 裸 PUBSUB → 容器 arity 错；NUMPAT/HELP 多参 → 'pubsub|<sub>' arity 错（与 7.2.6 逐字节一致）
func Test_PubSub_when_PubsubArityErrors(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub' command",
		dispatchPub(r, srvA, "PUBSUB").S)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub|numpat' command",
		dispatchPub(r, srvA, "PUBSUB", "NUMPAT", "x").S)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub|help' command",
		dispatchPub(r, srvA, "PUBSUB", "HELP", "x").S)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub|numpat' command",
		dispatchPub(r, srvA, "PUBSUB", "numpat", "x").S)
}

// WM: 未知子命令 → Try PUBSUB HELP（原文子命令保大小写）
func Test_PubSub_when_PubsubUnknownSub(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, "ERR unknown subcommand 'foo'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "foo").S)
	require.Equal(t, "ERR unknown subcommand 'Foo'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "Foo").S)
}

// WM: CHANNELS 多于 1 个 pattern → unknown subcommand or wrong number of arguments（原文大小写）
func Test_PubSub_when_PubsubChannelsTooManyArgs(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, "ERR unknown subcommand or wrong number of arguments for 'CHANNELS'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "CHANNELS", "a", "b").S)
	require.Equal(t, "ERR unknown subcommand or wrong number of arguments for 'channels'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "channels", "a", "b").S)
}

// WM: PUBSUB HELP → 14 元素 simple-string 数组（首行与真机 7.2.6 逐字节一致）
func Test_PubSub_when_PubsubHelp(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	got := dispatchPub(r, srvA, "PUBSUB", "HELP")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 14)
	require.Equal(t, protocol.KindSimpleString, got.Elems[0].Kind)
	require.Equal(t, "PUBSUB <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", got.Elems[0].S)
	require.Equal(t, "HELP", got.Elems[12].S)
	require.Equal(t, "    Print this help.", got.Elems[13].S)
}

// WM: 订阅态下 PUBSUB 合法子命令报 'pubsub|<sub>' 拦截错；裸 PUBSUB / 未知子命令 /
// 子命令 arity 错优先于拦截（真机 7.2.6：arity 与 unknown-sub 先于 submode 检查）
func Test_PubSub_when_PubsubSubMode(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	dispatchPub(r, srvA, "SUBSCRIBE", "c1")
	require.Equal(t,
		"ERR Can't execute 'pubsub|numpat': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
		dispatchPub(r, srvA, "PUBSUB", "NUMPAT").S)
	require.Equal(t,
		"ERR Can't execute 'pubsub|channels': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
		dispatchPub(r, srvA, "PUBSUB", "CHANNELS").S)
	require.Equal(t,
		"ERR Can't execute 'pubsub|numsub': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
		dispatchPub(r, srvA, "PUBSUB", "NUMSUB").S)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub' command",
		dispatchPub(r, srvA, "PUBSUB").S)
	require.Equal(t, "ERR unknown subcommand 'foo'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "foo").S)
	require.Equal(t, "ERR wrong number of arguments for 'pubsub|numpat' command",
		dispatchPub(r, srvA, "PUBSUB", "NUMPAT", "x").S)
}

// WM: 计数分域——ssubscribe 计数只看 shard；subscribe/psubscribe 只看 subs+patterns
// （真机 count_probe：SSUBSCRIBE a→1, SUBSCRIBE b→1, SSUBSCRIBE c→2, PSUBSCRIBE p*→2）
func Test_PubSub_when_SsubscribeConfirmCountsByDomain(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("a", 1, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "a"))
	require.Equal(t, confirmKind("b", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "b"))
	require.Equal(t, confirmKind("c", 2, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "c"))
	require.Equal(t, confirmKind("p*", 2, "psubscribe"), dispatchPub(r, srvA, "PSUBSCRIBE", "p*"))
	require.Equal(t, confirmKind("a", 2, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "a"))
}

// WM: 多频道 SSUBSCRIBE → 逐参确认（前 n-1 推送、末个返回），shard 域计数
func Test_PubSub_when_SsubscribeMultiArg(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	type result struct{ v protocol.Value }
	ch := make(chan result, 1)
	go func() { ch <- result{dispatchPub(r, srvA, "SSUBSCRIBE", "s1", "s2")} }()
	require.Equal(t, confirmKind("s1", 1, "ssubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("s2", 2, "ssubscribe"), (<-ch).v)
}

// WM: SUNSUBSCRIBE 零 shard 无参 → [sunsubscribe nil 0]；显式未订阅频道 → [sunsubscribe zz 0]
func Test_PubSub_when_SunsubscribeFresh(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("sunsubscribe"),
		protocol.Value{Kind: protocol.KindBulkString},
		protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, srvA, "SUNSUBSCRIBE"))
	require.Equal(t, confirmKind("zz", 0, "sunsubscribe"),
		dispatchPub(r, srvA, "SUNSUBSCRIBE", "zz"))
}

// WM: 裸 SUNSUBSCRIBE → 逆序逐个退 shard（计数 = shard 域，与普通 UNSUBSCRIBE 同形）
func Test_PubSub_when_SunsubscribeAllReversed(t *testing.T) {
	r, _, srvA, cliA, _, _ := openPubSubSetup(t)
	type result struct{ v protocol.Value }
	sub := make(chan result, 1)
	go func() { sub <- result{dispatchPub(r, srvA, "SSUBSCRIBE", "s1", "s2", "s3")} }()
	require.Equal(t, confirmKind("s1", 1, "ssubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("s2", 2, "ssubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("s3", 3, "ssubscribe"), (<-sub).v)
	unsub := make(chan result, 1)
	go func() { unsub <- result{dispatchPub(r, srvA, "SUNSUBSCRIBE")} }()
	require.Equal(t, confirmKind("s3", 2, "sunsubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("s2", 1, "sunsubscribe"), readPushed(t, cliA))
	require.Equal(t, confirmKind("s1", 0, "sunsubscribe"), (<-unsub).v)
	require.Equal(t, "PONG", dispatchPub(r, srvA, "PING").S)
}

// WM: 各域独立退订——SUNSUBSCRIBE 只清 shard（pattern 保留仍订阅态），PUNSUBSCRIBE 后恢复
func Test_PubSub_when_SunsubscribeMixedKeepsPattern(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("q1", 1, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "q1"))
	require.Equal(t, confirmKind("qp*", 1, "psubscribe"), dispatchPub(r, srvA, "PSUBSCRIBE", "qp*"))
	require.Equal(t, confirmKind("q1", 0, "sunsubscribe"), dispatchPub(r, srvA, "SUNSUBSCRIBE"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("")),
		dispatchPub(r, srvA, "PING"))
	require.Equal(t, confirmKind("qp*", 0, "punsubscribe"), dispatchPub(r, srvA, "PUNSUBSCRIBE"))
	require.Equal(t, "PONG", dispatchPub(r, srvA, "PING").S)
}

// WM: SPUBLISH 只投 shard 订阅者（smessage），count = 命中连接数；对普通订阅不投
func Test_PubSub_when_SpublishDelivers(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c1", 1, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "c1"))
	require.Equal(t, int64(1), dispatchPub(r, srvB, "SPUBLISH", "c1", "hello").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("smessage"),
		protocol.BulkOf("c1"),
		protocol.BulkOf("hello"),
	), readPushed(t, cliA))
	require.Equal(t, int64(0), dispatchPub(r, srvB, "SPUBLISH", "noone", "v").I)
	require.Equal(t, int64(0), dispatchPub(r, srvB, "PUBLISH", "c1", "v").I)
	cliA.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := protocol.Decode(bufio.NewReader(cliA))
	require.Error(t, err)
}

// WM: 普通订阅者收不到 SPUBLISH；同连接普通+pattern+shard：PUBLISH :2、SPUBLISH :1
func Test_PubSub_when_SpublishSkipsRegularAndDualCount(t *testing.T) {
	r, _, srvA, cliA, srvB, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("both", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "both"))
	require.Equal(t, int64(0), dispatchPub(r, srvB, "SPUBLISH", "both", "v").I)
	cliA.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := protocol.Decode(bufio.NewReader(cliA))
	require.Error(t, err)
	cliA.SetReadDeadline(time.Time{})
	dispatchPub(r, srvA, "PSUBSCRIBE", "bo*")
	dispatchPub(r, srvA, "SSUBSCRIBE", "both")
	require.Equal(t, int64(2), dispatchPub(r, srvB, "PUBLISH", "both", "x").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("message"), protocol.BulkOf("both"), protocol.BulkOf("x"),
	), readPushed(t, cliA))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("pmessage"), protocol.BulkOf("bo*"), protocol.BulkOf("both"), protocol.BulkOf("x"),
	), readPushed(t, cliA))
	require.Equal(t, int64(1), dispatchPub(r, srvB, "SPUBLISH", "both", "x").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("smessage"), protocol.BulkOf("both"), protocol.BulkOf("x"),
	), readPushed(t, cliA))
}

// WM: 同名频道双命名空间——查询与投递互不渗透（CHANNELS/SHARDCHANNELS、NUMSUB/SHARDNUMSUB 各计各的）
func Test_PubSub_when_NamespacesIndependentQueries(t *testing.T) {
	r, _, srvA, cliA, srvB, cliB := openPubSubSetup(t)
	q := newPubSubConn(t)
	require.Equal(t, confirmKind("dup", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "dup"))
	require.Equal(t, confirmKind("dup", 1, "ssubscribe"), dispatchPub(r, srvB, "SSUBSCRIBE", "dup"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("dup")),
		dispatchPub(r, q, "PUBSUB", "CHANNELS"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("dup")),
		dispatchPub(r, q, "PUBSUB", "SHARDCHANNELS"))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("dup"), protocol.Value{Kind: protocol.KindInteger, I: 1},
	), dispatchPub(r, q, "PUBSUB", "NUMSUB", "dup"))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("dup"), protocol.Value{Kind: protocol.KindInteger, I: 1},
	), dispatchPub(r, q, "PUBSUB", "SHARDNUMSUB", "dup"))
	require.Equal(t, int64(1), dispatchPub(r, q, "PUBLISH", "dup", "v").I)
	require.Equal(t, int64(1), dispatchPub(r, q, "SPUBLISH", "dup", "w").I)
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("message"), protocol.BulkOf("dup"), protocol.BulkOf("v"),
	), readPushed(t, cliA))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("smessage"), protocol.BulkOf("dup"), protocol.BulkOf("w"),
	), readPushed(t, cliB))
	cliA.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err := protocol.Decode(bufio.NewReader(cliA))
	require.Error(t, err)
	cliB.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, err = protocol.Decode(bufio.NewReader(cliB))
	require.Error(t, err)
}

// WM: SHARDCHANNELS/SHARDNUMSUB 只扫 shard 域——glob 过滤、空表 *0、
// CHANNELS/NUMSUB 不列不计 shard 订阅
func Test_PubSub_when_ShardQueries(t *testing.T) {
	r, _, srvA, _, srvB, _ := openPubSubSetup(t)
	q := newPubSubConn(t)
	require.Equal(t, emptyArray(), dispatchPub(r, q, "PUBSUB", "SHARDCHANNELS"))
	require.Equal(t, emptyArray(), dispatchPub(r, q, "PUBSUB", "SHARDNUMSUB"))
	require.Equal(t, confirmKind("s1", 1, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "s1"))
	require.Equal(t, confirmKind("s2", 1, "ssubscribe"), dispatchPub(r, srvB, "SSUBSCRIBE", "s2"))
	require.Equal(t, confirmKind("p*", 1, "psubscribe"), dispatchPub(r, srvB, "PSUBSCRIBE", "p*"))
	require.Equal(t, confirmKind("r1", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "r1"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("s1"), protocol.BulkOf("s2")),
		dispatchPub(r, q, "PUBSUB", "SHARDCHANNELS"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("s2")),
		dispatchPub(r, q, "PUBSUB", "SHARDCHANNELS", "s2*"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("r1")),
		dispatchPub(r, q, "PUBSUB", "CHANNELS"))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("s1"), protocol.Value{Kind: protocol.KindInteger, I: 1},
		protocol.BulkOf("s2"), protocol.Value{Kind: protocol.KindInteger, I: 1},
		protocol.BulkOf("nope"), protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, q, "PUBSUB", "SHARDNUMSUB", "s1", "s2", "nope"))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("r1"), protocol.Value{Kind: protocol.KindInteger, I: 1},
		protocol.BulkOf("s1"), protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, q, "PUBSUB", "NUMSUB", "r1", "s1"))
}

func subErr(name string) string {
	return "ERR Can't execute '" + name +
		"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context"
}

// WM: shard-only 订阅态同样触发拦截（含 SPUBLISH 与 PUBSUB SHARD*，拦截优先于
// SHARDCHANNELS 多参错）；PING 是 [pong data] 形；全退订后恢复正常分发
func Test_PubSub_when_ShardSubModeRejects(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, confirmKind("c1", 1, "ssubscribe"), dispatchPub(r, srvA, "SSUBSCRIBE", "c1"))
	require.Equal(t, subErr("set"), dispatchPub(r, srvA, "SET", "k", "v").S)
	require.Equal(t, subErr("publish"), dispatchPub(r, srvA, "PUBLISH", "c1", "v").S)
	require.Equal(t, subErr("spublish"), dispatchPub(r, srvA, "SPUBLISH", "c1", "v").S)
	require.Equal(t, subErr("pubsub|shardchannels"), dispatchPub(r, srvA, "PUBSUB", "SHARDCHANNELS").S)
	require.Equal(t, subErr("pubsub|shardchannels"),
		dispatchPub(r, srvA, "PUBSUB", "SHARDCHANNELS", "a", "b").S)
	require.Equal(t, subErr("pubsub|shardnumsub"),
		dispatchPub(r, srvA, "PUBSUB", "SHARDNUMSUB", "x", "y").S)
	require.Equal(t, "ERR unknown subcommand 'foo'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "foo").S)
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("")),
		dispatchPub(r, srvA, "PING"))
	require.Equal(t, protocol.ArrayOf(protocol.BulkOf("pong"), protocol.BulkOf("hello")),
		dispatchPub(r, srvA, "PING", "hello"))
	require.Equal(t, confirmKind("c2", 1, "subscribe"), dispatchPub(r, srvA, "SUBSCRIBE", "c2"))
	require.Equal(t, confirmKind("c2", 0, "unsubscribe"), dispatchPub(r, srvA, "UNSUBSCRIBE", "c2"))
	require.Equal(t, confirmKind("c1", 0, "sunsubscribe"), dispatchPub(r, srvA, "SUNSUBSCRIBE", "c1"))
	after := dispatchPub(r, srvA, "NOSUCHCMD", "a")
	require.Equal(t, protocol.KindError, after.Kind)
	require.Contains(t, after.S, "unknown command")
}

// WM: shard 命令参数错误面（非订阅态，与 7.2.6 实测措辞一致）——
// ssubscribe 0 参、spublish 1/3 参、shardchannels 多参、shardnumsub 无参合法回 *0
func Test_PubSub_when_ShardArityErrors(t *testing.T) {
	r, _, srvA, _, _, _ := openPubSubSetup(t)
	require.Equal(t, "ERR wrong number of arguments for 'ssubscribe' command",
		dispatchPub(r, srvA, "SSUBSCRIBE").S)
	require.Equal(t, "ERR wrong number of arguments for 'ssubscribe' command",
		dispatchPub(r, srvA, "ssubscribe").S)
	require.Equal(t, "ERR wrong number of arguments for 'spublish' command",
		dispatchPub(r, srvA, "SPUBLISH", "c").S)
	require.Equal(t, "ERR wrong number of arguments for 'spublish' command",
		dispatchPub(r, srvA, "SPUBLISH", "a", "b", "c").S)
	require.Equal(t, "ERR wrong number of arguments for 'spublish' command",
		dispatchPub(r, srvA, "spublish", "c").S)
	require.Equal(t,
		"ERR unknown subcommand or wrong number of arguments for 'SHARDCHANNELS'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "SHARDCHANNELS", "a", "b").S)
	require.Equal(t,
		"ERR unknown subcommand or wrong number of arguments for 'shardchannels'. Try PUBSUB HELP.",
		dispatchPub(r, srvA, "PUBSUB", "shardchannels", "a", "b").S)
	require.Equal(t, emptyArray(), dispatchPub(r, srvA, "PUBSUB", "SHARDNUMSUB"))
	require.Equal(t, protocol.ArrayOf(
		protocol.BulkOf("x"), protocol.Value{Kind: protocol.KindInteger, I: 0},
		protocol.BulkOf("y"), protocol.Value{Kind: protocol.KindInteger, I: 0},
	), dispatchPub(r, srvA, "PUBSUB", "SHARDNUMSUB", "x", "y"))
}
