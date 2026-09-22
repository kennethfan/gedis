package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// Given: 空 Pebble（t.TempDir 隔离）
// When: Set 后 Get 同一个 key
// Then: 读回的值与写入一致
func Test_Store_RoundTrip_when_SetThenGet(t *testing.T) {
	s := New(t.TempDir())
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("s:hello"), []byte("world")))

	got, err := s.Get(ctx, []byte("s:hello"))
	require.NoError(t, err)
	require.Equal(t, []byte("world"), got)
}

// Given: 空库
// When: Get 不存在的 key
// Then: 返回 ErrNotFound（哨兵错误，可用 errors.Is 判定）
func Test_Store_Get_when_MissingKey(t *testing.T) {
	s := New(t.TempDir())
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })

	_, err := s.Get(context.Background(), []byte("s:nope"))
	require.ErrorIs(t, err, ErrNotFound)
}

// Given: 已写入的 key
// When: Delete 后再 Get
// Then: 返回 ErrNotFound
func Test_Store_Delete_when_ExistingKey(t *testing.T) {
	s := New(t.TempDir())
	require.NoError(t, s.Open())
	t.Cleanup(func() { _ = s.Close() })

	ctx := context.Background()
	require.NoError(t, s.Set(ctx, []byte("s:bye"), []byte("x")))
	require.NoError(t, s.Delete(ctx, []byte("s:bye")))

	_, err := s.Get(ctx, []byte("s:bye"))
	require.ErrorIs(t, err, ErrNotFound)
}
