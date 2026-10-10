package commands

import (
	"net"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 列表 src=[a b c]
// When: LMOVE 从右弹左插 / 从左弹左插 / 空源 / 非法侧向词
// Then: 依次返回 c、a；dst=[a c]；空源 null；非法侧向 ERR syntax error
func TestLmove(t *testing.T) {
	r, _ := openListSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "RPUSH", "src", "a", "b", "c"))
	require.Equal(t, protocol.BulkOf("c"), dispatch(r, "LMOVE", "src", "dst", "RIGHT", "LEFT"))
	require.Equal(t, bulkList("c"), dispatch(r, "LRANGE", "dst", "0", "-1"))
	require.Equal(t, protocol.BulkOf("a"), dispatch(r, "LMOVE", "src", "dst", "LEFT", "LEFT"))
	require.Equal(t, bulkList("a", "c"), dispatch(r, "LRANGE", "dst", "0", "-1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "LMOVE", "missing", "dst", "LEFT", "LEFT"))
	got := dispatch(r, "LMOVE", "src", "dst", "MIDDLE", "LEFT")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR syntax error", got.S)
}

// Given: 列表 s=[a b]
// When: RPOPLPUSH s d
// Then: 返回 b；d=[b]；s=[a]
func TestRpoplpush(t *testing.T) {
	r, _ := openListSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "RPUSH", "s", "a", "b"))
	require.Equal(t, protocol.BulkOf("b"), dispatch(r, "RPOPLPUSH", "s", "d"))
	require.Equal(t, bulkList("b"), dispatch(r, "LRANGE", "d", "0", "-1"))
	require.Equal(t, bulkList("a"), dispatch(r, "LRANGE", "s", "0", "-1"))
}

// Given: 空源 w 与后台延迟写入；空源 e
// When: BLMOVE 等待写入 / 0.05 超时 / 负超时；BRPOPLPUSH 立即可用源
// Then: 等到 x；超时 null；负数 ERR timeout is negative；BRPOPLPUSH 返回 q
func TestBlmoveBlockingAndTimeout(t *testing.T) {
	r, _ := openListSetup(t)
	go func() {
		time.Sleep(200 * time.Millisecond)
		_ = dispatch(r, "RPUSH", "w", "x")
	}()
	require.Equal(t, protocol.BulkOf("x"), dispatch(r, "BLMOVE", "w", "dst", "LEFT", "LEFT", "2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "BLMOVE", "e", "dst", "LEFT", "LEFT", "0.05"))
	got := dispatch(r, "BLMOVE", "w", "dst", "LEFT", "LEFT", "-1")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR timeout is negative", got.S)

	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "RPUSH", "b2", "q"))
	require.Equal(t, protocol.BulkOf("q"), dispatch(r, "BRPOPLPUSH", "b2", "bd", "1"))
}

// Given: KEA 开启，A 订阅 __key*__:*，B 执行；src=[a b c]、src2=[z]
// When: LMOVE 弹一个 / LMOVE 把源弹空
// Then: dst push 事件先于 src pop 事件到达；弹空追加 src del
func TestLmoveEventsOrder(t *testing.T) {
	r, store, stats := openMonitorSetup(t)
	RegisterList(r, store, stats)
	reg := RegisterPubSub(r)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	dispatchPub(r, srvB, "RPUSH", "src", "a", "b", "c")
	expectEvents(t, cliA, wantEvent("src", "rpush"))

	dispatchPub(r, srvB, "LMOVE", "src", "dst", "RIGHT", "LEFT")
	expectEvents(t, cliA, append(wantEvent("dst", "lpush"), wantEvent("src", "rpop")...))

	dispatchPub(r, srvB, "RPUSH", "src2", "z")
	expectEvents(t, cliA, wantEvent("src2", "rpush"))
	dispatchPub(r, srvB, "LMOVE", "src2", "dst2", "RIGHT", "LEFT")
	expectEvents(t, cliA, append(append(wantEvent("dst2", "lpush"), wantEvent("src2", "rpop")...), wantEvent("src2", "del")...))
}

// Given: 连接层命令已注册
// When: READWRITE / READONLY / 多余参数 / 副本只读模式下两命令
// Then: 均 +OK；多余参数报 wrong number；只读模式下仍可执行
func TestReadonlyReadwrite(t *testing.T) {
	r, _ := openConnSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "READWRITE"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "READONLY"))

	got := dispatch(r, "READONLY", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")
	got = dispatch(r, "READWRITE", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")

	r.SetWriteCommands(WriteCommandSet())
	r.SetReadOnly(true)
	t.Cleanup(func() { r.SetReadOnly(false) })
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "READONLY"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "READWRITE"))
}
