package commands

import (
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: sicily 两城
// When: GEOHASH Palermo Catania
// Then: 标准 geohash 串（真机 7.2.6 录制）；缺成员 nil；缺 key 空数组
func Test_Scatter_when_GeoHash(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeo(r, store)
	dispatch(r, "GEOADD", "sicily", "13.361389", "38.115556", "Palermo", "15.087269", "37.502669", "Catania")
	got := dispatch(r, "GEOHASH", "sicily", "Palermo", "Catania", "Nope")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 3)
	require.Equal(t, protocol.BulkOf("sqc8b49rny0"), got.Elems[0])
	require.Equal(t, protocol.BulkOf("sqdtr74hyu0"), got.Elems[1])
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, got.Elems[2])
	require.Equal(t, protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{{Kind: protocol.KindBulkString}}},
		dispatch(r, "GEOHASH", "missing", "Palermo"))
	require.Equal(t, protocol.KindError, dispatch(r, "GEOHASH").Kind)
}

// Given: myhash{f1: v1, f2: v2}
// When: HRANDFIELD 各种 COUNT/WITHVALUES
// Then: 单个命中其一；COUNT 2 全取；负 COUNT 可重复且带值成对；缺 key nil
func Test_Scatter_when_HRandField(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	dispatch(r, "HSET", "myhash", "f1", "v1", "f2", "v2")
	one := dispatch(r, "HRANDFIELD", "myhash")
	require.Equal(t, protocol.KindBulkString, one.Kind)
	require.Contains(t, []string{"f1", "f2"}, string(one.Bulk))
	two := dispatch(r, "HRANDFIELD", "myhash", "2")
	require.Equal(t, protocol.KindArray, two.Kind)
	require.Len(t, two.Elems, 2)
	neg := dispatch(r, "HRANDFIELD", "myhash", "-2", "WITHVALUES")
	require.Equal(t, protocol.KindArray, neg.Kind)
	require.Len(t, neg.Elems, 2)
	for _, e := range neg.Elems {
		require.Equal(t, protocol.KindArray, e.Kind)
		require.Len(t, e.Elems, 2)
	}
	zero := dispatch(r, "HRANDFIELD", "myhash", "0")
	require.Equal(t, protocol.KindArray, zero.Kind)
	require.Len(t, zero.Elems, 0)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "HRANDFIELD", "missing"))
	require.Equal(t, protocol.KindError, dispatch(r, "HRANDFIELD", "myhash", "xx").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "HRANDFIELD", "myhash", "1", "WITHVALUES", "x").Kind)
}

// Given: myhash{f1: v1}
// When: HSTRLEN
// Then: 值长 2；缺 field/缺 key 回 0
func Test_Scatter_when_HStrLen(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	dispatch(r, "HSET", "myhash", "f1", "v1")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "HSTRLEN", "myhash", "f1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HSTRLEN", "myhash", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HSTRLEN", "missing", "f1"))
	require.Equal(t, protocol.KindError, dispatch(r, "HSTRLEN", "myhash").Kind)
}

// Given: k1{a,b,c} k2{b,c,d}
// When: SINTERCARD 2 k1 k2 [LIMIT n]
// Then: 交集基数 2；LIMIT 1 提前终止回 1；缺 key 按空集
func Test_Scatter_when_SInterCard(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterSet(r, store)
	dispatch(r, "SADD", "k1", "a", "b", "c")
	dispatch(r, "SADD", "k2", "b", "c", "d")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SINTERCARD", "2", "k1", "k2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "SINTERCARD", "2", "k1", "k2", "LIMIT", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "SINTERCARD", "2", "k1", "missing"))
	require.Equal(t, protocol.KindError, dispatch(r, "SINTERCARD", "3", "k1", "k2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "SINTERCARD", "2", "k1", "k2", "LIMIT", "x").Kind)
}

// Given: z1{a:1,b:2} z2{b:3,c:4}
// When: ZINTERCARD 2 z1 z2 [LIMIT n]
// Then: 交集基数 1；LIMIT 0 回 0；LIMIT 1 回 1；缺 key 按空集回 0；wrongtype 报错
func Test_Scatter_when_ZInterCard(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "z1", "1", "a", "2", "b")
	dispatch(r, "ZADD", "z2", "3", "b", "4", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZINTERCARD", "2", "z1", "z2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "ZINTERCARD", "2", "z1", "missing"))
	dispatch(r, "SET", "str", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "ZINTERCARD", "1", "str").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "ZINTERCARD", "0").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "ZINTERCARD", "2", "z1", "z2", "LIMIT", "x").Kind)
}

// Given: l1[a b c]
// When: LMPOP 1 l1 LEFT [COUNT n]
// Then: [l1 [a]]；COUNT 2 取 [b c]；miss 回 nil array；COUNT 0 报错；方向非法报错
func Test_Scatter_when_LMPop(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "l1", "a", "b", "c")
	got := dispatch(r, "LMPOP", "1", "l1", "LEFT")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("l1"), got.Elems[0])
	require.Equal(t, bulkList("a"), got.Elems[1])
	got = dispatch(r, "LMPOP", "1", "l1", "LEFT", "COUNT", "2")
	require.Equal(t, protocol.BulkOf("l1"), got.Elems[0])
	require.Equal(t, bulkList("b", "c"), got.Elems[1])
	miss := dispatch(r, "LMPOP", "1", "nope", "LEFT")
	require.Equal(t, protocol.KindArray, miss.Kind)
	require.Nil(t, miss.Elems)
	require.Equal(t, protocol.KindError, dispatch(r, "LMPOP", "1", "l1", "LEFT", "COUNT", "0").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "LMPOP", "1", "l1", "MIDDLE").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "LMPOP", "1", "l1").Kind)
}

// Given: z1{a:1,b:2}
// When: ZMPOP 1 z1 MIN [COUNT n]
// Then: [z1 [[a 1]]]；COUNT 2 取余下；miss 回 nil array；方向非法报错
func Test_Scatter_when_ZMPop(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "z1", "1", "a", "2", "b")
	got := dispatch(r, "ZMPOP", "1", "z1", "MIN")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("z1"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 1)
	require.Equal(t, protocol.BulkOf("a"), got.Elems[1].Elems[0].Elems[0])
	got = dispatch(r, "ZMPOP", "1", "z1", "MAX", "COUNT", "2")
	require.Equal(t, protocol.BulkOf("z1"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 1)
	require.Equal(t, protocol.BulkOf("b"), got.Elems[1].Elems[0].Elems[0])
	miss := dispatch(r, "ZMPOP", "1", "nope", "MIN")
	require.Equal(t, protocol.KindArray, miss.Kind)
	require.Nil(t, miss.Elems)
	require.Equal(t, protocol.KindError, dispatch(r, "ZMPOP", "1", "z1", "MIDDLE").Kind)
}

// Given: s 含 100-1/100-2（last=100-2）
// When: XSETID s <id> [ENTRIES-ADDED n] [MAXDELETEDID id]
// Then: 改 last-id 成功回 OK；miss 回 no such key；非法 ID/未知选项报错
func Test_Scatter_when_XSetID(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "XSETID", "s", "200-5"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "XSETID", "s", "300", "ENTRIES-ADDED", "7", "MAXDELETEDID", "50-3"))
	require.Equal(t, protocol.KindError, dispatch(r, "XSETID", "missing", "1-1").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "XSETID", "s", "bad-id").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "XSETID", "s", "1-1", "NOPE", "2").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "XSETID", "s").Kind)
}

// Given: list mylist={3,1,2}；bitmap b
// When: SORT_RO mylist / BITFIELD_RO b GET u8 0
// Then: 与 SORT/BITFIELD 一致；STORE→syntax error；SET/INCRBY→RO 专属错误
func Test_Scatter_when_ReadOnlyAliases(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterGeneric(r, store)
	RegisterBitmap(r, store)
	RegisterList(r, store, nil)
	dispatch(r, "RPUSH", "mylist", "3", "1", "2")
	require.Equal(t,
		dispatch(r, "SORT", "mylist"),
		dispatch(r, "SORT_RO", "mylist"))
	require.Equal(t, protocol.KindError, dispatch(r, "SORT_RO", "mylist", "STORE", "dst").Kind)
	require.Equal(t,
		dispatch(r, "BITFIELD", "b", "GET", "u8", "0"),
		dispatch(r, "BITFIELD_RO", "b", "GET", "u8", "0"))
	require.Equal(t, "ERR BITFIELD_RO only supports the GET subcommand",
		dispatch(r, "BITFIELD_RO", "b", "SET", "u8", "0", "1").S)
	require.Equal(t, "ERR BITFIELD_RO only supports the GET subcommand",
		dispatch(r, "BITFIELD_RO", "b", "INCRBY", "u8", "0", "1").S)
}

// Given: lcsa=abcdef lcsb=xbcdyz（真机 7.2.6 录制）
// When: LCS 裸/LEN/IDX/MINMATCHLEN/缺 key/双 LEN+IDX
// Then: 裸 bcd；LEN 3；IDX 扁平 matches+len；MINMATCHLEN 只滤 matches；缺 key 视空串
func Test_Scatter_when_LCS(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStrings(r, store)
	dispatch(r, "SET", "lcsa", "abcdef")
	dispatch(r, "SET", "lcsb", "xbcdyz")
	require.Equal(t, protocol.BulkOf("bcd"), dispatch(r, "LCS", "lcsa", "lcsb"))
	require.Equal(t, int64(3), dispatch(r, "LCS", "lcsa", "lcsb", "LEN").I)
	got := dispatch(r, "LCS", "lcsa", "lcsb", "IDX")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 4)
	require.Equal(t, protocol.BulkOf("matches"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 1)
	require.Equal(t, protocol.BulkOf("len"), got.Elems[2])
	require.Equal(t, int64(3), got.Elems[3].I)
	min := dispatch(r, "LCS", "lcsa", "lcsb", "IDX", "MINMATCHLEN", "4")
	require.Len(t, min.Elems[1].Elems, 0)
	require.Equal(t, int64(3), min.Elems[3].I)
	require.Equal(t, protocol.BulkOf(""), dispatch(r, "LCS", "lcsa", "nosuchkey"))
	require.Equal(t, int64(0), dispatch(r, "LCS", "lcsa", "nosuchkey", "LEN").I)
	require.Equal(t, protocol.KindError, dispatch(r, "LCS", "lcsa", "lcsb", "LEN", "IDX").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "LCS", "lcsa").Kind)
}

// Given: 带过期 exk=EX 10000、无过期 nok、缺失 miss
// When: EXPIRETIME/PEXPIRETIME 查询
// Then: miss -2、无过期 -1、有过期回绝对时间（秒/毫秒，容差5s）
func Test_Scatter_when_ExpireTime(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStrings(r, store)
	dispatch(r, "SET", "exk", "v", "EX", "10000")
	dispatch(r, "SET", "nok", "v")
	require.Equal(t, int64(-2), dispatch(r, "EXPIRETIME", "miss").I)
	require.Equal(t, int64(-1), dispatch(r, "EXPIRETIME", "nok").I)
	require.Equal(t, int64(-2), dispatch(r, "PEXPIRETIME", "miss").I)
	require.Equal(t, int64(-1), dispatch(r, "PEXPIRETIME", "nok").I)
	sec := dispatch(r, "EXPIRETIME", "exk").I
	require.InDelta(t, time.Now().Unix()+10000, sec, 5)
	ms := dispatch(r, "PEXPIRETIME", "exk").I
	require.InDelta(t, time.Now().UnixMilli()+10000*1000, ms, 5000)
	require.Equal(t, protocol.KindError, dispatch(r, "EXPIRETIME").Kind)
}

// Given: 单库引擎，movek 存在、miss 不存在
// When: MOVE/RANDOMKEY 调用
// Then: 同库 ERR、异库恒 0（单库无处可搬）、miss 0；空库 RANDOMKEY nil、非空返回现存 key
func Test_Scatter_when_MoveRandomKey(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStrings(r, store)
	require.Equal(t, protocol.KindBulkString, dispatch(r, "RANDOMKEY").Kind)
	require.Equal(t, 0, len(dispatch(r, "RANDOMKEY").Bulk))
	dispatch(r, "SET", "movek", "v")
	require.Equal(t, int64(0), dispatch(r, "MOVE", "miss", "1").I)
	require.Equal(t, int64(0), dispatch(r, "MOVE", "movek", "1").I)
	require.Equal(t, protocol.KindError, dispatch(r, "MOVE", "movek", "0").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "MOVE", "movek").Kind)
	dispatch(r, "SET", "onlykey", "v")
	require.Contains(t, []string{"movek", "onlykey"}, string(dispatch(r, "RANDOMKEY").Bulk))
}
