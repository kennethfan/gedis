package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/stretchr/testify/require"
)

// Given: 带 stats 的 ctx
// When: 写入已过期的 key 后 GET
// Then: 被动删除 + expired 计数 1
func Test_Expire_when_PassiveCounts(t *testing.T) {
	r, _, s := openMonitorSetup(t)
	ctx := network.ContextWithStats(context.Background(), s)
	require.Equal(t, int64(0), s.Snapshot().ExpiredKeys)
	dispatch(r, "SET", "k", "v", "EXAT", "1")
	got := r.Dispatch(ctx, cmd("GET", "k"))
	require.Nil(t, got.Bulk)
	require.Equal(t, int64(1), s.Snapshot().ExpiredKeys)
}
