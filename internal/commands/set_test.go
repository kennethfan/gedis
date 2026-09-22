package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openSetSetup(t testing.TB) (*network.Router, *storage.Pebble) {
	t.Helper()
	r, store := openTestSetup(t)
	RegisterSet(r, store)
	return r, store
}

// Given: 空库
// When: SADD 多成员 / 重复 SADD / SMEMBERS
// Then: 返回新增数，去重后有序返回
func Test_Set_when_AddMembers(t *testing.T) {
	r, _ := openSetSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "SADD", "myset", "c", "a", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SADD", "myset", "b", "d"))
	got := dispatch(r, "SMEMBERS", "myset")
	require.Equal(t, toBulkArray([]string{"a", "b", "c", "d"}), got)
	got = dispatch(r, "SMEMBERS", "missing")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
}

// Given: 含成员的 set
// When: SISMEMBER / SMISMEMBER / SCARD
// Then: 存在性与基数正确，缺失 key 返回 0
func Test_Set_when_MembershipCard(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "myset", "a", "b")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SISMEMBER", "myset", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SISMEMBER", "myset", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SISMEMBER", "missing", "a"))
	got := dispatch(r, "SMISMEMBER", "myset", "a", "nope")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, int64(0), got.Elems[1].I)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SCARD", "myset"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SCARD", "missing"))
}

// Given: 含成员的 set
// When: SREM 部分成员 / 删空
// Then: 删除计数正确，删空后 key 消失
func Test_Set_when_Rem(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "myset", "a", "b", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SREM", "myset", "a", "nope", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SCARD", "myset"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SREM", "missing", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SREM", "myset", "c"))
	require.Equal(t, "none", dispatch(r, "TYPE", "myset").S)
}

// Given: 含成员的 set
// When: SPOP / SRANDMEMBER 带与不带 count
// Then: 弹出删除成员，随机返回是子集；缺失 key 返回 null 或空数组
func Test_Set_when_PopRand(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "single", "only")
	require.Equal(t, protocol.BulkOf("only"), dispatch(r, "SPOP", "single"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SCARD", "single"))

	dispatch(r, "SADD", "myset", "a", "b", "c", "d")
	got := dispatch(r, "SPOP", "myset", "2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	for _, e := range got.Elems {
		require.Contains(t, []string{"a", "b", "c", "d"}, string(e.Bulk))
	}
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SCARD", "myset"))

	got = dispatch(r, "SRANDMEMBER", "myset")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Contains(t, []string{"a", "b", "c", "d"}, string(got.Bulk))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "SCARD", "myset"))

	got = dispatch(r, "SRANDMEMBER", "myset", "2")
	require.Len(t, got.Elems, 2)
	got = dispatch(r, "SRANDMEMBER", "myset", "-3")
	require.Len(t, got.Elems, 3)

	got = dispatch(r, "SPOP", "missing")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
	got = dispatch(r, "SPOP", "missing", "2")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
	got = dispatch(r, "SRANDMEMBER", "missing")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 两个 set
// When: SMOVE 搬移成员 / 同 key 搬移 / 缺失成员
// Then: 返回值正确，源删目标增
func Test_Set_when_Move(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "src", "a", "b")
	dispatch(r, "SADD", "dst", "c")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SMOVE", "src", "dst", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SISMEMBER", "src", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SISMEMBER", "dst", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SMOVE", "src", "dst", "nope"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "SMOVE", "missing", "dst", "a"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SMOVE", "src", "src", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "SISMEMBER", "src", "b"))
}

// Given: string 类型的 key
// When: SADD / SMEMBERS / SCARD 该 key
// Then: WRONGTYPE 错误
func Test_Set_when_WrongType(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SET", "s", "v")
	for _, args := range [][]string{{"SADD", "s", "m"}, {"SMEMBERS", "s"}, {"SCARD", "s"}, {"SREM", "s", "m"}} {
		got := dispatch(r, args...)
		require.Equal(t, protocol.KindError, got.Kind)
		require.Contains(t, got.S, "WRONGTYPE")
	}
}
