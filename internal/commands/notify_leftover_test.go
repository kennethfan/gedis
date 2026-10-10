package commands

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

// openLeftoverSetup 搭事件核对台：Stats + Set/Strings/Monitor + 被测命令
// handler（generic/hash/zset/list/geo）+ PubSub 注册。
func openLeftoverSetup(t testing.TB) (*network.Router, *storage.Pebble, *network.Stats, *PubSubRegistry) {
	t.Helper()
	r, store, stats := openMonitorSetup(t)
	RegisterGeneric(r, store)
	RegisterHash(r, store)
	RegisterZSet(r, store)
	RegisterList(r, store, stats)
	RegisterGeo(r, store)
	reg := RegisterPubSub(r)
	return r, store, stats, reg
}

// Given: KEA 开启，A 订阅 __key*__:*，h 仅剩最后一个 field
// When: HDEL 删至空 hash
// Then: 官方表要求 hdel 后追加 del（结果 hash 空且 key 被移除）
func Test_NotifyLeftover_HdelEmptyHashPublishesDel(t *testing.T) {
	r, _, _, reg := openLeftoverSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	// 订阅前准备数据，避免准备写事件混入断言
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "HSET", "hf", "f", "v"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "HSET", "hx", "f", "v", "g", "w"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	// When: 删至空
	got := dispatch(r, "HDEL", "hf", "f")
	// Then: hdel + del（官方事件表：additional del event）
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, got)
	expectEvents(t, cliA, append(wantEvent("hf", "hdel"), wantEvent("hf", "del")...))

	// 对照：删至空后 key 应已移除 → HLEN 为 0 且再 HDEL 零事件
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HLEN", "hf"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HDEL", "hf", "f"))
	expectEvents(t, cliA, nil)

	// 对照：非空清空不发生时（删 g 留 f）只有 hdel，无 del
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "HDEL", "hx", "g"))
	expectEvents(t, cliA, wantEvent("hx", "hdel"))
	_ = cliB
}

// Given: KEA 开启，A 订阅 __key*__:*，各候选命令数据就绪
// When: 依次执行 ZPOPMIN/LMPOP/ZREMRANGEBYLEX/GEOSEARCHSTORE/GETEX/TOUCH
// Then: 官方事件表六组全部无条目 → 零事件（表外不发，pin住该决策）
func Test_NotifyLeftover_OutOfTableCommandsSilent(t *testing.T) {
	r, _, _, reg := openLeftoverSetup(t)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})

	// 订阅前准备数据（ZADD/LPUSH/GEOADD/SET 各自事件在订阅前不入流）
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "ZADD", "z", "1", "m"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "LPUSH", "l", "a", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "ZADD", "lex", "0", "a", "1", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "GEOADD", "g", "13.361389", "38.115556", "Palermo"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SET", "sk", "v"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SET", "tk", "v"))

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	run := func(args ...string) {
		t.Helper()
		got := dispatch(r, args...)
		require.NotEqual(t, protocol.KindError, got.Kind, "命令应成功执行: %v", args)
	}

	// When: 表外候选逐条执行（BZPOP*/BLMPOP/BZMPOP 同实现路径，覆盖 MIN 即可）
	run("ZPOPMIN", "z")
	run("LMPOP", "1", "l", "LEFT")
	run("ZREMRANGEBYLEX", "lex", "[a", "[b")
	run("GEOSEARCHSTORE", "gdst", "g", "FROMLONLAT", "13.38", "38.11", "BYRADIUS", "200", "km")
	run("GETEX", "sk")
	run("TOUCH", "tk")

	// Then: 零事件（多一条 FAIL，读超时证明零多余）
	expectEvents(t, cliA, nil)
	_ = cliB
}
