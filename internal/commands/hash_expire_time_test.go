package commands

import (
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 缺失 key / 无过期 field / 有过期 field
// When: HEXPIRETIME / HPEXPIRETIME
// Then: -2 / -1 / 绝对 Unix 秒（HEXPIRETIME）或毫秒（HPEXPIRETIME）时间戳
func Test_Hexpiretime_ReturnsUnixTimestamp(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)

	got := dispatch(r, "HEXPIRETIME", "h", "nofield")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, int64(-2), got.Elems[0].I)

	require.NotEqual(t, protocol.KindError, dispatch(r, "HSET", "h", "f", "v").Kind)
	got = dispatch(r, "HEXPIRETIME", "h", "f")
	require.Equal(t, int64(-1), got.Elems[0].I)

	require.NotEqual(t, protocol.KindError, dispatch(r, "HEXPIRE", "h", "100", "FIELDS", "1", "f").Kind)
	nowSec := time.Now().Unix()
	got = dispatch(r, "HEXPIRETIME", "h", "f")
	require.GreaterOrEqual(t, got.Elems[0].I, nowSec+99)
	require.LessOrEqual(t, got.Elems[0].I, nowSec+100)

	require.NotEqual(t, protocol.KindError, dispatch(r, "HPEXPIRE", "h", "100000", "FIELDS", "1", "f").Kind)
	nowMs := time.Now().UnixMilli()
	got = dispatch(r, "HPEXPIRETIME", "h", "f")
	require.GreaterOrEqual(t, got.Elems[0].I, nowMs+99000)
	require.LessOrEqual(t, got.Elems[0].I, nowMs+100000)
}
