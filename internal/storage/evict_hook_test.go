package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: 上限 100B + 逐出 hook 已注入
// When: 写入 20 个超限 key 触发驱逐
// Then: 每次真实驱逐 hook 收到被删的 raw key
func TestEvictHookFiresOnEviction(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	p.SetMaxBytes(100)
	var got []string
	p.SetEvictHook(func(k string) { got = append(got, k) })

	for i := 0; i < 20; i++ {
		require.NoError(t, p.Set(ctx, []byte("k"+strings.Repeat("x", 3)+string(rune('a'+i))), []byte(strings.Repeat("v", 20))))
	}
	require.Greater(t, p.EvictedCount(), int64(0))
	require.NotEmpty(t, got, "驱逐发生时 hook 必须被调用")
}

// Given: noeviction 策略 + hook 已注入
// When: 写入超限 key 返回 OOM
// Then: hook 零次调用（OOM 不是逐出）
func TestEvictHookNotFiredOnOOM(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("noeviction"))
	p.SetMaxBytes(100)
	var got []string
	p.SetEvictHook(func(k string) { got = append(got, k) })

	require.NoError(t, p.Set(ctx, []byte("s:k1"), make([]byte, 80)))
	require.ErrorIs(t, p.Set(ctx, []byte("s:k2"), make([]byte, 80)), ErrOOM)
	require.Empty(t, got, "OOM 不得触发逐出 hook")
}
