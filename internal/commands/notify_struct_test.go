package commands

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// expectEvents/wantEvent 复用 notify_string_test.go 同包 helper（3s 读超时防挂死 +
// 尾部 300ms 判零多余事件）。

func seq(events ...[]protocol.Value) []protocol.Value {
	var out []protocol.Value
	for _, e := range events {
		out = append(out, e...)
	}
	return out
}

// Given: KEA 开启，A 订阅 __key*__:*，B 执行 hash/list/set/zset 事件表用例
// When: 逐条执行动作序列（含准备写）
// Then: 双频道事件序列精确匹配、零多余；失败/缺 key/无变更零事件
func TestHashListSetZSetEvents(t *testing.T) {
	r, store, stats := openMonitorSetup(t)
	RegisterHash(r, store)
	RegisterList(r, store, stats)
	RegisterZSet(r, store)
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

	run := func(args ...string) {
		t.Helper()
		dispatchPub(r, srvB, args...)
	}

	// ---- hash ----
	// HSET 新建 → hset；HSETNX 成功 → hset；已存在失败 → 零
	run("HSET", "h", "f", "v")
	expectEvents(t, cliA, seq(wantEvent("h", "hset")))
	run("HSETNX", "h", "f2", "v2")
	expectEvents(t, cliA, seq(wantEvent("h", "hset")))
	run("HSETNX", "h", "f2", "v9")
	expectEvents(t, cliA, nil)

	// HMSET 变参 → 单条 hset
	run("HMSET", "hm", "a", "1", "b", "2")
	expectEvents(t, cliA, seq(wantEvent("hm", "hset")))

	// HDEL 真删字段 → hdel；清空仍只 hdel（gedis 保留空 hash，无 del）；删不存在字段 → 零
	run("HDEL", "h", "f2")
	expectEvents(t, cliA, seq(wantEvent("h", "hdel")))
	run("HDEL", "h", "f")
	expectEvents(t, cliA, seq(wantEvent("h", "hdel")))
	run("HDEL", "h", "nofield")
	expectEvents(t, cliA, nil)

	// HINCRBY/HINCRBYFLOAT 成功 → 各发一条；非整数错误 → 零
	run("HINCRBY", "hi", "n", "5")
	expectEvents(t, cliA, seq(wantEvent("hi", "hincrby")))
	run("HINCRBYFLOAT", "hf", "n", "2.5")
	expectEvents(t, cliA, seq(wantEvent("hf", "hincrbyfloat")))
	run("HINCRBY", "hi", "n", "bad")
	expectEvents(t, cliA, nil)

	// HEXPIRE 命令调用即发 hexpired；HPERSIST 成功 → hpersist；重复（无 TTL）→ 零
	run("HSET", "h", "f", "v")
	run("HEXPIRE", "h", "100", "FIELDS", "1", "f")
	expectEvents(t, cliA, seq(wantEvent("h", "hset"), wantEvent("h", "hexpired")))
	run("HPERSIST", "h", "FIELDS", "1", "f")
	expectEvents(t, cliA, seq(wantEvent("h", "hpersist")))
	run("HPERSIST", "h", "FIELDS", "1", "f")
	expectEvents(t, cliA, nil)

	// ---- list ----
	// LPUSH 变参 → 单条 lpush；LPOP 弹一个 → lpop；再弹空删 key → lpop,del；缺 key → 零
	run("LPUSH", "l", "a", "b")
	run("LPOP", "l")
	expectEvents(t, cliA, seq(wantEvent("l", "lpush"), wantEvent("l", "lpop")))
	run("LPOP", "l")
	expectEvents(t, cliA, seq(wantEvent("l", "lpop"), wantEvent("l", "del")))
	run("LPOP", "l")
	expectEvents(t, cliA, nil)

	// RPUSH → rpush；RPOP 弹空 → rpop,del
	run("RPUSH", "rl", "x")
	run("RPOP", "rl")
	expectEvents(t, cliA, seq(wantEvent("rl", "rpush"), wantEvent("rl", "rpop"), wantEvent("rl", "del")))

	// LSET 成功 → lset；索引越界 → 零
	run("LPUSH", "l2", "a", "b")
	run("LSET", "l2", "0", "Z")
	expectEvents(t, cliA, seq(wantEvent("l2", "lpush"), wantEvent("l2", "lset")))
	run("LSET", "l2", "9", "Z")
	expectEvents(t, cliA, nil)

	// LINSERT 命中 → linsert；pivot 不存在 → 零
	run("LPUSH", "li", "a")
	run("LINSERT", "li", "BEFORE", "a", "x")
	expectEvents(t, cliA, seq(wantEvent("li", "lpush"), wantEvent("li", "linsert")))
	run("LINSERT", "li", "BEFORE", "missing", "x")
	expectEvents(t, cliA, nil)

	// LREM 清空 → lrem,del；残留非空 → 仅 lrem；缺 key → 零
	run("LPUSH", "lp", "x")
	run("LREM", "lp", "0", "x")
	expectEvents(t, cliA, seq(wantEvent("lp", "lpush"), wantEvent("lp", "lrem"), wantEvent("lp", "del")))
	run("LPUSH", "lr", "x", "x", "y")
	run("LREM", "lr", "0", "x")
	expectEvents(t, cliA, seq(wantEvent("lr", "lpush"), wantEvent("lr", "lrem")))
	run("LREM", "nolist", "0", "x")
	expectEvents(t, cliA, nil)

	// LTRIM 残留 → 仅 ltrim；裁空 → ltrim,del；缺 key → 零
	run("LPUSH", "lt", "a", "b")
	run("LTRIM", "lt", "0", "0")
	expectEvents(t, cliA, seq(wantEvent("lt", "lpush"), wantEvent("lt", "ltrim")))
	run("LTRIM", "lt", "5", "6")
	expectEvents(t, cliA, seq(wantEvent("lt", "ltrim"), wantEvent("lt", "del")))
	run("LTRIM", "nolist", "0", "0")
	expectEvents(t, cliA, nil)

	// ---- set ----
	// SADD 变参 → 单条 sadd；重复成员仍发；SREM 不存在成员 → 零
	run("SADD", "s", "m1", "m2")
	run("SADD", "s", "m1")
	expectEvents(t, cliA, seq(wantEvent("s", "sadd"), wantEvent("s", "sadd")))
	run("SREM", "s", "missing")
	expectEvents(t, cliA, nil)

	// SMOVE 源 srem + 目标 sadd 按序；src==dst → 零；缺成员 → 零
	run("SADD", "s2", "k")
	run("SMOVE", "s", "s2", "m1")
	expectEvents(t, cliA, seq(wantEvent("s2", "sadd"), wantEvent("s", "srem"), wantEvent("s2", "sadd")))
	run("SMOVE", "s2", "s2", "k")
	expectEvents(t, cliA, nil)
	run("SMOVE", "s3", "s4", "ghost")
	expectEvents(t, cliA, nil)

	// SREM 清空 → srem,del
	run("SREM", "s", "m2")
	expectEvents(t, cliA, seq(wantEvent("s", "srem"), wantEvent("s", "del")))

	// SPOP 弹余下 → spop；弹空删 key → spop,del；缺 key → 零
	run("SADD", "sp", "a", "b")
	run("SPOP", "sp")
	expectEvents(t, cliA, seq(wantEvent("sp", "sadd"), wantEvent("sp", "spop")))
	run("SPOP", "sp")
	expectEvents(t, cliA, seq(wantEvent("sp", "spop"), wantEvent("sp", "del")))
	run("SPOP", "sp")
	expectEvents(t, cliA, nil)

	// ---- set STORE ----
	// 三准备写；SINTERSTORE 结果空且 out 已存在 → sinterstore,del
	run("SADD", "out", "z")
	run("SADD", "ia", "x")
	run("SADD", "ib", "y")
	expectEvents(t, cliA, seq(wantEvent("out", "sadd"), wantEvent("ia", "sadd"), wantEvent("ib", "sadd")))
	run("SINTERSTORE", "out", "ia", "ib")
	expectEvents(t, cliA, seq(wantEvent("out", "sinterstore"), wantEvent("out", "del")))
	// 结果非空覆盖 → 仅家族事件；结果空且旧 key 不存在 → 仅家族事件
	run("SINTERSTORE", "out2", "ia", "ia")
	expectEvents(t, cliA, seq(wantEvent("out2", "sinterstore")))
	run("SDIFFSTORE", "sd", "ia", "ia")
	expectEvents(t, cliA, seq(wantEvent("sd", "sdiffstore")))
	run("SUNIONSTORE", "un", "ia", "ib")
	expectEvents(t, cliA, seq(wantEvent("un", "sunionstore")))

	// ---- zset ----
	// ZADD 单条 zadd（重复成员也发）；ZINCRBY → zincr；ZREM 不存在成员 → 零；清空 → zrem,del
	run("ZADD", "z", "1", "m")
	run("ZADD", "z", "1", "m")
	expectEvents(t, cliA, seq(wantEvent("z", "zadd"), wantEvent("z", "zadd")))
	run("ZINCRBY", "z", "1", "m")
	expectEvents(t, cliA, seq(wantEvent("z", "zincr")))
	run("ZREM", "z", "missing")
	expectEvents(t, cliA, nil)
	run("ZREM", "z", "m")
	expectEvents(t, cliA, seq(wantEvent("z", "zrem"), wantEvent("z", "del")))

	// ZREMRANGEBYRANK 部分删 → zrembyrank；删空 → +del；缺 key → 零
	run("ZADD", "zr", "1", "a", "2", "b")
	run("ZREMRANGEBYRANK", "zr", "0", "0")
	expectEvents(t, cliA, seq(wantEvent("zr", "zadd"), wantEvent("zr", "zrembyrank")))
	run("ZREMRANGEBYRANK", "zr", "0", "5")
	expectEvents(t, cliA, seq(wantEvent("zr", "zrembyrank"), wantEvent("zr", "del")))
	run("ZREMRANGEBYRANK", "zr", "0", "1")
	expectEvents(t, cliA, nil)

	// ZREMRANGEBYSCORE 部分删 → zrembyscore；零删除 → 零；删空 → +del；缺 key → 零
	run("ZADD", "zs", "1", "a", "2", "b")
	run("ZREMRANGEBYSCORE", "zs", "1", "1")
	expectEvents(t, cliA, seq(wantEvent("zs", "zadd"), wantEvent("zs", "zrembyscore")))
	run("ZREMRANGEBYSCORE", "zs", "10", "20")
	expectEvents(t, cliA, nil)
	run("ZREMRANGEBYSCORE", "zs", "-inf", "+inf")
	expectEvents(t, cliA, seq(wantEvent("zs", "zrembyscore"), wantEvent("zs", "del")))
	run("ZREMRANGEBYSCORE", "zs", "-inf", "+inf")
	expectEvents(t, cliA, nil)

	// ---- zset STORE ----
	// 准备三写；ZDIFFSTORE 结果空且旧存在 → zdiffstore,del；ZINTERSTORE 空+旧不存在 → 仅家族；
	// ZUNIONSTORE 非空 → 仅家族
	run("ZADD", "za", "1", "x")
	run("ZADD", "zb", "2", "y")
	run("ZADD", "oe", "9", "old")
	expectEvents(t, cliA, seq(wantEvent("za", "zadd"), wantEvent("zb", "zadd"), wantEvent("oe", "zadd")))
	run("ZDIFFSTORE", "oe", "2", "za", "za")
	expectEvents(t, cliA, seq(wantEvent("oe", "zdiffstore"), wantEvent("oe", "del")))
	run("ZINTERSTORE", "ie", "2", "za", "zb")
	expectEvents(t, cliA, seq(wantEvent("ie", "zinterstore")))
	run("ZUNIONSTORE", "un2", "2", "za", "zb")
	expectEvents(t, cliA, seq(wantEvent("un2", "zunionstore")))
}
