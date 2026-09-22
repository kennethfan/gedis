package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: s:a s:b 与 o:c 三个键
// When: Scan s: 前缀
// Then: 只返回 s:a s:b（拷贝，与 DB 状态解耦）
func Test_Scan_WhenPrefix_ThenMatchingKeys(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	t.Cleanup(func() { _ = p.Close() })

	require.NoError(t, p.Set(ctx, []byte("s:a"), []byte("1")))
	require.NoError(t, p.Set(ctx, []byte("s:b"), []byte("2")))
	require.NoError(t, p.Set(ctx, []byte("o:c"), []byte("3")))

	keys, err := p.Scan(ctx, []byte("s:"))
	require.NoError(t, err)
	require.Len(t, keys, 2)
	require.Equal(t, []byte("s:a"), keys[0])
	require.Equal(t, []byte("s:b"), keys[1])
}
