package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: set key
// When: EXPIRE / TTL / PERSIST / TYPE
// Then: 过期语义与 string key 一致
func Test_Set_when_ExpireTTL(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "myset", "a", "b")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "EXPIRE", "myset", "100"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 100}, dispatch(r, "TTL", "myset"))
	require.Equal(t, "set", dispatch(r, "TYPE", "myset").S)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PERSIST", "myset"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: -1}, dispatch(r, "TTL", "myset"))
}

// Given: 小整数 set 与大 set
// When: OBJECT ENCODING
// Then: 小整数 set 返回 intset，大 set 返回 hashtable
func Test_Set_when_ObjectEncoding(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SADD", "small", "1", "2", "3")
	require.Equal(t, "intset", string(dispatch(r, "OBJECT", "ENCODING", "small").Bulk))
	dispatch(r, "SADD", "big", "hello")
	got := dispatch(r, "OBJECT", "ENCODING", "big")
	require.Equal(t, "hashtable", string(got.Bulk))
}

// Given: string key 与 set key 并存
// When: KEYS *
// Then: 两类 key 都返回
func Test_Set_when_KeysCrossType(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SET", "str", "v")
	dispatch(r, "SADD", "st", "a")
	got := dispatch(r, "KEYS", "*")
	require.Equal(t, protocol.KindArray, got.Kind)
	names := map[string]bool{}
	for _, e := range got.Elems {
		names[string(e.Bulk)] = true
	}
	require.True(t, names["str"])
	require.True(t, names["st"])
}

// Given: s: 与 st: 前缀的 key 并存（"s:" 是 "st:" 的字面前缀）
// When: KEYS *
// Then: 精确返回两个 key，无幽灵 "t:st"，无重复计数
func Test_Set_when_KeysNoGhost(t *testing.T) {
	r, _ := openSetSetup(t)
	dispatch(r, "SET", "str", "v")
	dispatch(r, "SADD", "st", "a")
	got := dispatch(r, "KEYS", "*")
	require.Len(t, got.Elems, 2)
}
