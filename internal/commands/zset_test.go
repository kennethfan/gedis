package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func openZSetup(t testing.TB) {
	t.Helper()
}

// Given: 空库
// When: ZADD 多成员 / 重复更新 / ZSCORE / ZMSCORE / ZCARD
// Then: 新增计数正确，score 可读，缺失返回 nil
func Test_ZSet_when_AddScoreCard(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "ZADD", "myz", "1", "a", "2", "b", "3", "c"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZADD", "myz", "4", "a"))
	require.Equal(t, protocol.BulkOf("4"), dispatch(r, "ZSCORE", "myz", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "ZSCORE", "myz", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "ZSCORE", "missing", "a"))
	got := dispatch(r, "ZMSCORE", "myz", "a", "nope")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("4"), got.Elems[0])
	require.Equal(t, protocol.KindBulkString, got.Elems[1].Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "ZCARD", "myz"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger}, dispatch(r, "ZCARD", "missing"))
}

// Given: 空库
// When: ZADD 带 NX/XX/GT/LT/CH/INCR
// Then: 语义与 Redis 一致
func Test_ZSet_when_AddFlags(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZADD", "myz", "1", "a", "2", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZADD", "myz", "NX", "9", "a", "3", "c"))
	require.Equal(t, protocol.BulkOf("1"), dispatch(r, "ZSCORE", "myz", "a"))
	require.Equal(t, protocol.BulkOf("3"), dispatch(r, "ZSCORE", "myz", "c"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZADD", "myz", "XX", "9", "a", "5", "new"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "ZSCORE", "myz", "new"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZADD", "myz", "GT", "0", "a"))
	require.Equal(t, protocol.BulkOf("9"), dispatch(r, "ZSCORE", "myz", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZADD", "myz", "LT", "99", "a"))
	require.Equal(t, protocol.BulkOf("9"), dispatch(r, "ZSCORE", "myz", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZADD", "myz", "CH", "10", "a", "11", "d"))
	require.Equal(t, protocol.BulkOf("7"), dispatch(r, "ZADD", "myz", "INCR", "5", "b"))
	require.Equal(t, protocol.BulkOf("7"), dispatch(r, "ZSCORE", "myz", "b"))
}

// Given: 含成员 zset
// When: ZRANK/ZREVRANK（含 WITHSCORE）/ ZCOUNT / ZINCRBY
// Then: 排名与计数正确
func Test_ZSet_when_RankCountIncr(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "myz", "1", "a", "2", "b", "3", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZRANK", "myz", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZRANK", "myz", "c"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZREVRANK", "myz", "c"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZREVRANK", "myz", "a"))
	got := dispatch(r, "ZRANK", "myz", "b", "WITHSCORE")
	require.Len(t, got.Elems, 2)
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, protocol.BulkOf("2"), got.Elems[1])
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "ZRANK", "myz", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZCOUNT", "myz", "1", "2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZCOUNT", "myz", "(1", "2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "ZCOUNT", "myz", "-inf", "+inf"))
	require.Equal(t, protocol.BulkOf("5"), dispatch(r, "ZINCRBY", "myz", "3", "b"))
}

// Given: 含成员 zset
// When: ZREM / ZPOPMIN / ZPOPMAX / 删空
// Then: 计数正确，删空后 key 消失
func Test_ZSet_when_RemPop(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "myz", "1", "a", "2", "b", "3", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZREM", "myz", "a", "nope"))
	got := dispatch(r, "ZPOPMIN", "myz")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("b"), got.Elems[0])
	got = dispatch(r, "ZPOPMAX", "myz")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.BulkOf("c"), got.Elems[0])
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger}, dispatch(r, "ZCARD", "missing2"))
	got = dispatch(r, "ZPOPMIN", "missing2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
}

// Given: 含成员 zset
// When: ZRANGE 新语法（BYSCORE/BYLEX/REV/LIMIT/WITHSCORES）与老命令
// Then: 顺序与过滤正确
func Test_ZSet_when_Ranges(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "myz", "1", "a", "2", "b", "3", "c", "4", "d")
	require.Equal(t, toBulkArray([]string{"a", "b", "c"}), dispatch(r, "ZRANGE", "myz", "0", "2"))
	require.Equal(t, toBulkArray([]string{"d", "c", "b"}), dispatch(r, "ZRANGE", "myz", "0", "2", "REV"))
	require.Equal(t, toBulkArray([]string{"a", "1", "b", "2"}), dispatch(r, "ZRANGE", "myz", "1", "2", "BYSCORE", "WITHSCORES"))
	require.Equal(t, toBulkArray([]string{"a", "b"}), dispatch(r, "ZRANGE", "myz", "-", "[b", "BYLEX"))
	require.Equal(t, toBulkArray([]string{"b", "c"}), dispatch(r, "ZRANGE", "myz", "1", "3", "BYSCORE", "LIMIT", "1", "2"))
	require.Equal(t, toBulkArray([]string{"b", "c"}), dispatch(r, "ZRANGEBYSCORE", "myz", "2", "3"))
	require.Equal(t, toBulkArray([]string{"c", "b"}), dispatch(r, "ZREVRANGEBYSCORE", "myz", "3", "2"))
	require.Equal(t, toBulkArray([]string{"a", "b"}), dispatch(r, "ZRANGEBYLEX", "myz", "-", "[b"))
	require.Equal(t, toBulkArray([]string{"d", "c"}), dispatch(r, "ZREVRANGEBYLEX", "myz", "+", "(b"))
	require.Equal(t, toBulkArray([]string{"d", "c"}), dispatch(r, "ZREVRANGE", "myz", "0", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZREMRANGEBYRANK", "myz", "0", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZREMRANGEBYSCORE", "myz", "3", "4"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "ZREMRANGEBYLEX", "myz", "[d", "+"))
}

// Given: 两个 zset
// When: 交并差及其 STORE（含 WEIGHTS/AGGREGATE）/ ZRANGESTORE
// Then: 结果与写入正确
func Test_ZSet_when_SetOps(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "z1", "1", "a", "2", "b")
	dispatch(r, "ZADD", "z2", "3", "b", "4", "c")
	require.Equal(t, toBulkArray([]string{"a", "c", "b"}), dispatch(r, "ZUNION", "2", "z1", "z2"))
	require.Equal(t, toBulkArray([]string{"b"}), dispatch(r, "ZINTER", "2", "z1", "z2"))
	require.Equal(t, toBulkArray([]string{"a"}), dispatch(r, "ZDIFF", "2", "z1", "z2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "ZUNIONSTORE", "out", "2", "z1", "z2"))
	require.Equal(t, protocol.BulkOf("5"), dispatch(r, "ZSCORE", "out", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "ZINTERSTORE", "out2", "2", "z1", "z2", "WEIGHTS", "2", "3", "AGGREGATE", "MAX"))
	require.Equal(t, protocol.BulkOf("9"), dispatch(r, "ZSCORE", "out2", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "ZRANGESTORE", "out3", "z1", "0", "-1"))
}

// Given: 含成员 zset
// When: ZRANDMEMBER / ZSCAN / BZPOPMIN（立即命中与超时）/ WRONGTYPE / TYPE
// Then: 行为正确
func Test_ZSet_when_RandScanBlock(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	RegisterStrings(r, store)
	dispatch(r, "ZADD", "myz", "1", "a", "2", "b")
	got := dispatch(r, "ZRANDMEMBER", "myz", "2")
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "ZRANDMEMBER", "missing"))
	got = dispatch(r, "ZSCAN", "myz", "0")
	require.Equal(t, protocol.BulkOf("0"), got.Elems[0])
	require.Len(t, got.Elems[1].Elems, 4)
	got = dispatch(r, "BZPOPMIN", "myz", "0")
	require.Len(t, got.Elems, 3)
	require.Equal(t, protocol.BulkOf("myz"), got.Elems[0])
	got = dispatch(r, "BZPOPMIN", "missing", "0.1")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Nil(t, got.Elems)
	require.Contains(t, dispatch(r, "SET", "str", "v").S, "OK")
	require.Contains(t, dispatch(r, "ZADD", "str", "1", "a").S, "WRONGTYPE")
	require.Equal(t, "zset", dispatch(r, "TYPE", "myz").S)
	require.Equal(t, "listpack", string(dispatch(r, "OBJECT", "ENCODING", "myz").Bulk))
}

// Given: 空库
// When: 写入边界量级 score 后读回
// Then: 打印规则与真 Redis 一致（1e-6/1e21 为界，-0 归零）
func Test_ZSet_when_ScoreFormat(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterZSet(r, store)
	dispatch(r, "ZADD", "z", "0.000001", "a", "0.0000001", "b", "1000000000000000000000", "c",
		"100000000000000000000", "d", "1.1", "e", "123.450", "f", "10000000000000000000", "g",
		"1000000000000000000", "h", "123456789012345680", "i")
	require.Equal(t, protocol.BulkOf("0.000001"), dispatch(r, "ZSCORE", "z", "a"))
	require.Equal(t, protocol.BulkOf("1e-7"), dispatch(r, "ZSCORE", "z", "b"))
	require.Equal(t, protocol.BulkOf("1e+21"), dispatch(r, "ZSCORE", "z", "c"))
	require.Equal(t, protocol.BulkOf("1e+20"), dispatch(r, "ZSCORE", "z", "d"))
	require.Equal(t, protocol.BulkOf("1.1"), dispatch(r, "ZSCORE", "z", "e"))
	require.Equal(t, protocol.BulkOf("123.45"), dispatch(r, "ZSCORE", "z", "f"))
	require.Equal(t, protocol.BulkOf("1e+19"), dispatch(r, "ZSCORE", "z", "g"))
	require.Equal(t, protocol.BulkOf("1000000000000000000"), dispatch(r, "ZSCORE", "z", "h"))
	require.Equal(t, protocol.BulkOf("123456789012345680"), dispatch(r, "ZSCORE", "z", "i"))
	dispatch(r, "ZADD", "z", "12340000000000000000", "j", "12345678901230000000", "k", "12500000000000000000", "l")
	require.Equal(t, protocol.BulkOf("1.234e+19"), dispatch(r, "ZSCORE", "z", "j"))
	require.Equal(t, protocol.BulkOf("12345678901230000000"), dispatch(r, "ZSCORE", "z", "k"))
	require.Equal(t, protocol.BulkOf("1.25e+19"), dispatch(r, "ZSCORE", "z", "l"))
	dispatch(r, "ZADD", "z", "-0.0", "z0")
	require.Equal(t, protocol.BulkOf("0"), dispatch(r, "ZSCORE", "z", "z0"))
}
