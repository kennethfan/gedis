package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 已存在 key k=hello 与缺失 key
// When: SUBSTR 与 GETRANGE 同参数对照（正索引/负索引/越界/缺 key）
// Then: 逐字节相同
func Test_Substr_BehavesLikeGetrange(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "hello"))
	for _, c := range [][3]string{
		{"k", "0", "-1"},
		{"k", "-4", "-2"},
		{"k", "0", "100"},
		{"k", "4", "2"},
		{"nokey", "0", "-1"},
	} {
		got := dispatch(r, "SUBSTR", c[0], c[1], c[2])
		want := dispatch(r, "GETRANGE", c[0], c[1], c[2])
		require.Equal(t, want, got, "SUBSTR %v", c)
	}
}

// Given: 单库引擎
// When: SWAPDB 0 0 / 0 1 / 1 0 / WriteCommandSet 名单
// Then: 0 0 → +OK；越界 → DB index is out of range；SWAPDB 在写集合
func Test_Swapdb_SingleDbNoop(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SWAPDB", "0", "0"))
	for _, c := range [][2]string{{"0", "1"}, {"1", "0"}, {"2", "3"}} {
		got := dispatch(r, "SWAPDB", c[0], c[1])
		require.Equal(t, protocol.KindError, got.Kind, "SWAPDB %v", c)
		require.Equal(t, "ERR DB index is out of range", got.S, "SWAPDB %v", c)
	}
	require.True(t, WriteCommandSet()["SWAPDB"])
}

// Given: 已注册 SET 与 HEXPIRETIME
// When: COMMAND LIST / COMMAND LIST FILTERBY PATTERN x*
// Then: 大写命令名数组含两命令；带参 → unknown argument
func Test_CommandList_ListsRegistered(t *testing.T) {
	r, store, _, _, _ := openServerSetup(t)
	RegisterHash(r, store)
	got := dispatch(r, "COMMAND", "LIST")
	require.Equal(t, protocol.KindArray, got.Kind)
	names := map[string]bool{}
	for _, e := range got.Elems {
		require.Equal(t, protocol.KindBulkString, e.Kind)
		names[string(e.Bulk)] = true
	}
	require.True(t, names["SET"])
	require.True(t, names["HEXPIRETIME"])
	require.False(t, names["set"], "必须大写")

	err := dispatch(r, "COMMAND", "LIST", "FILTERBY", "PATTERN", "x*")
	require.Equal(t, protocol.KindError, err.Kind)
	require.Contains(t, err.S, "unknown argument")
}

// Given: 本引擎无配置持久化
// When: CONFIG REWRITE
// Then: 诚实报错
func Test_ConfigRewrite_HonestError(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	got := dispatch(r, "CONFIG", "REWRITE")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR not supported on this engine: config rewrite not implemented", got.S)
}
