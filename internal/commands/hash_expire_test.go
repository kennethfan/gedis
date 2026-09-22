package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 含 field 的 hash
// When: HEXPIRE / HTTL / HGET
// Then: field 过期后读写都不可见
func Test_HashExpire_when_SetGet(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f1", "v1", "f2", "v2")
	got := dispatch(r, "HEXPIRE", "h", "100", "FIELDS", "1", "f1")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 100}, dispatch(r, "HTTL", "h", "FIELDS", "1", "f1").Elems[0])
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "HGET", "h", "f1"))

	got = dispatch(r, "HEXPIRE", "h", "0", "FIELDS", "1", "f2")
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatch(r, "HGET", "h", "f2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "HEXISTS", "h", "f2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HLEN", "h"))
}

// Given: 带过期 field 的 hash
// When: NX/XX/GT/LT 条件
// Then: 条件语义正确
func Test_HashExpire_when_Conds(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "a", "1", "b", "2")
	dispatch(r, "HEXPIRE", "h", "100", "FIELDS", "1", "a")

	got := dispatch(r, "HEXPIRE", "h", "200", "NX", "FIELDS", "2", "a", "b")
	require.Equal(t, int64(0), got.Elems[0].I)
	require.Equal(t, int64(1), got.Elems[1].I)

	got = dispatch(r, "HEXPIRE", "h", "300", "XX", "FIELDS", "2", "a", "nope")
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, int64(-2), got.Elems[1].I)

	got = dispatch(r, "HEXPIRE", "h", "100", "GT", "FIELDS", "1", "a")
	require.Equal(t, int64(0), got.Elems[0].I)
	got = dispatch(r, "HEXPIRE", "h", "400", "GT", "FIELDS", "1", "a")
	require.Equal(t, int64(1), got.Elems[0].I)

	got = dispatch(r, "HEXPIRE", "h", "500", "LT", "FIELDS", "1", "a")
	require.Equal(t, int64(0), got.Elems[0].I)
}

// Given: hash field
// When: HPERSIST / HPTTL / 缺失 key-field
// Then: 返回值语义正确
func Test_HashExpire_when_PersistTtl(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f", "v", "plain", "p")
	dispatch(r, "HEXPIRE", "h", "100", "FIELDS", "1", "f")

	got := dispatch(r, "HPERSIST", "h", "FIELDS", "2", "f", "plain")
	require.Equal(t, int64(1), got.Elems[0].I)
	require.Equal(t, int64(-1), got.Elems[1].I)
	got = dispatch(r, "HPERSIST", "h", "FIELDS", "1", "nope")
	require.Equal(t, int64(-2), got.Elems[0].I)

	dispatch(r, "HPEXPIRE", "h", "60000", "FIELDS", "1", "f")
	got = dispatch(r, "HPTTL", "h", "FIELDS", "2", "f", "plain")
	require.Greater(t, got.Elems[0].I, int64(0))
	require.Equal(t, int64(-1), got.Elems[1].I)
	got = dispatch(r, "HTTL", "missing", "FIELDS", "1", "f")
	require.Equal(t, int64(-2), got.Elems[0].I)
}

// Given: string key
// When: HEXPIRE
// Then: WRONGTYPE
func Test_HashExpire_when_WrongType(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "SET", "s", "v")
	got := dispatch(r, "HEXPIRE", "s", "100", "FIELDS", "1", "f")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}

// Given: 带 field 过期的 hash 被 DEL 后重建
// When: 重建同名 hash 查 HTTL
// Then: 无残留过期（sidecar 随 DEL 清理）
func Test_HashExpire_when_DelCleansSidecar(t *testing.T) {
	r, _ := openHashSetup(t)
	dispatch(r, "HSET", "h", "f", "v")
	dispatch(r, "HEXPIRE", "h", "100", "FIELDS", "1", "f")
	dispatch(r, "DEL", "h")
	dispatch(r, "HSET", "h", "f", "v2")
	got := dispatch(r, "HTTL", "h", "FIELDS", "1", "f")
	require.Equal(t, int64(-1), got.Elems[0].I)
	require.Equal(t, protocol.BulkOf("v2"), dispatch(r, "HGET", "h", "f"))
}
