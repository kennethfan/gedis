package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: SET 后连读 5 次的 string key
// When: OBJECT FREQ / IDLETIME / 缺 key
// Then: FREQ=初始1+5=6（stats 先于 getAny 读取，顺序不可颠倒）；
// IDLETIME=0（at 刚被 GET 刷新）；不存在 key 回 null bulk
func TestObject_when_FreqIdleTime(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "SET", "oidle", "v"))
	for i := 0; i < 5; i++ {
		require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "oidle"))
	}

	got := dispatch(r, "OBJECT", "FREQ", "oidle")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(6), got.I, "FREQ = 初始 1 + 5 次 GET")

	got = dispatch(r, "OBJECT", "IDLETIME", "oidle")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Equal(t, int64(0), got.I, "IDLETIME：at 刚被 GET 刷新")

	got = dispatch(r, "OBJECT", "FREQ", "nokey")
	require.Equal(t, protocol.KindBulkString, got.Kind, "不存在 key 回 null bulk")
	require.Nil(t, got.Bulk)
}
