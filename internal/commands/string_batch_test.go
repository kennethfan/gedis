package commands

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 空库与已存在且带 TTL 的 key
// When: GETSET 覆盖写
// Then: 返回旧值、新值生效、TTL 被清除；新 key 返回 null
func TestGetSet(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GETSET", "k", "v2"))
	require.Equal(t, protocol.BulkOf("v2"), dispatch(r, "GET", "k"))

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k2", "v", "EX", "100"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GETSET", "k2", "v2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: -1}, dispatch(r, "TTL", "k2"))

	got := dispatch(r, "GETSET", "fresh", "v")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "fresh"))
}

// Given: 空库
// When: SETEX/PSETEX 正常设 TTL 与非法时长
// Then: TTL/PTTL 落在窗口内；≤0 返回精确错误文案
func TestSetexPsetex(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SETEX", "k", "10", "v"))
	ttl := dispatch(r, "TTL", "k")
	require.Equal(t, protocol.KindInteger, ttl.Kind)
	require.GreaterOrEqual(t, ttl.I, int64(9))
	require.LessOrEqual(t, ttl.I, int64(10))

	got := dispatch(r, "SETEX", "k", "0", "v")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR invalid expire time in 'setex' command", got.S)

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "PSETEX", "k", "5000", "v"))
	pttl := dispatch(r, "PTTL", "k")
	require.Equal(t, protocol.KindInteger, pttl.Kind)
	require.GreaterOrEqual(t, pttl.I, int64(4000))
	require.LessOrEqual(t, pttl.I, int64(5000))

	got = dispatch(r, "PSETEX", "k", "-1", "v")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR invalid expire time in 'psetex' command", got.S)
}

// Given: 空库与已存在（带 TTL）的 key
// When: SETNX 两次
// Then: 新建返回 1 值写入；已存在返回 0 且值与 TTL 均不变
func TestSetnx(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SETNX", "n1", "v"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "n1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SETNX", "n1", "v2"))
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "n1"))

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "n2", "v", "EX", "100"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SETNX", "n2", "v2"))
	ttl := dispatch(r, "TTL", "n2")
	require.Equal(t, protocol.KindInteger, ttl.Kind)
	require.GreaterOrEqual(t, ttl.I, int64(99))
	require.LessOrEqual(t, ttl.I, int64(100))
}

// Given: 已存在的 key a 与缺失的 key b
// When: TOUCH a b / TOUCH 无参 / 只读模式下 TOUCH
// Then: 计数为 1；无参报 wrong number；只读模式下可执行（非写集合）
func TestTouch(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "a", "v"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "TOUCH", "a", "b"))

	got := dispatch(r, "TOUCH")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")

	r.SetWriteCommands(WriteCommandSet())
	r.SetReadOnly(true)
	t.Cleanup(func() { r.SetReadOnly(false) })
	require.Equal(t, protocol.KindError, dispatch(r, "SET", "x", "v").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "TOUCH", "a", "b"))
}

// Given: KEA 开启，A 订阅 __key*__:*，B 执行新命令
// When: SETNX 新建 / SETEX / GETSET
// Then: 事件按到达序为 set；SETEX 为 set 后接 expire
func TestNewStringCommandsEvents(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
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

	dispatchPub(r, srvB, "SETNX", "nx", "v")
	expectEvents(t, cliA, wantEvent("nx", "set"))

	dispatchPub(r, srvB, "SETEX", "sx", "10", "v")
	expectEvents(t, cliA, append(wantEvent("sx", "set"), wantEvent("sx", "expire")...))

	dispatchPub(r, srvB, "GETSET", "gs", "v")
	expectEvents(t, cliA, wantEvent("gs", "set"))
}
