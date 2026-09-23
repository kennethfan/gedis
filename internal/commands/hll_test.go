package commands

import (
	"fmt"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_HLL_when_AddCountSmall(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHLL(r, store)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "PFCOUNT", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PFADD", "h", "one"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "PFADD", "h", "one"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PFADD", "h", "two"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "PFCOUNT", "h"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PFADD", "bare"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "PFCOUNT", "bare"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "PFCOUNT", "h", "bare", "missing"))
	require.Equal(t, protocol.KindError, dispatch(r, "PFCOUNT").Kind)
}

// Given: 1k/10k 元素集
// When: PFADD 后 PFCOUNT
// Then: 1008（容差1）/ 10073 精确命中仲裁值
func Test_HLL_when_AddCountArbitration(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHLL(r, store)
	args := []string{"PFADD", "e"}
	for i := 0; i < 1000; i++ {
		args = append(args, fmt.Sprintf("e%d", i))
	}
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, args...))
	got := dispatch(r, "PFCOUNT", "e")
	require.InDelta(t, 1008, got.I, 1)

	args = []string{"PFADD", "w"}
	for i := 0; i < 10000; i++ {
		args = append(args, fmt.Sprintf("w%d", i))
	}
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, args...))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 10073}, dispatch(r, "PFCOUNT", "w"))
}

// Given: 已存在 HLL
// When: PFMERGE 系列（含缺失源、合入已存在 dest、wrongtype）
// Then: 与真 Redis 一致
func Test_HLL_when_Merge(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHLL(r, store)
	RegisterStrings(r, store)
	dispatch(r, "PFADD", "a", "x", "y")
	dispatch(r, "PFADD", "b", "y", "z")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "PFMERGE", "m", "a", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "PFCOUNT", "m"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "PFMERGE", "m2", "a", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "PFCOUNT", "m2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "PFMERGE", "none1", "missing1", "missing2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "PFCOUNT", "none1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "EXISTS", "none1"))

	dispatch(r, "SET", "s", "v")
	require.Equal(t, protocol.KindError, dispatch(r, "PFADD", "s", "x").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "PFCOUNT", "s").Kind)
	require.Equal(t, protocol.KindError, dispatch(r, "PFMERGE", "m3", "s").Kind)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "EXISTS", "m3"))
	require.Equal(t, protocol.KindError, dispatch(r, "PFMERGE", "s", "a").Kind)

	dispatch(r, "PFADD", "have", "x", "y")
	dispatch(r, "EXPIRE", "have", "100")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "PFMERGE", "have", "a", "b"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 3}, dispatch(r, "PFCOUNT", "have"))
	ttl := dispatch(r, "TTL", "have")
	require.Greater(t, ttl.I, int64(0))
	require.LessOrEqual(t, ttl.I, int64(100))
}
