package storage

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/stretchr/testify/require"
)

func Test_Evict_when_OverLimit(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	p.SetMaxBytes(100)

	for i := 0; i < 20; i++ {
		require.NoError(t, p.Set(ctx, []byte("k"+strings.Repeat("x", 3)+string(rune('a'+i))), []byte(strings.Repeat("v", 20))))
	}
	require.Greater(t, p.EvictedCount(), int64(0))
	require.LessOrEqual(t, p.UsedBytes(), int64(100+64))
}

func Test_Evict_when_VolatileSkipsPersistent(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("volatile-lru"))
	p.SetMaxBytes(400)

	persistVal := datastruct.Encode(datastruct.TypeString, 0, []byte(strings.Repeat("v", 20)))
	require.NoError(t, p.Set(ctx, []byte("persist"), persistVal))
	exp := datastruct.Encode(datastruct.TypeString, time.Now().Add(time.Hour).UnixNano(), []byte("e"))
	require.NoError(t, p.Set(ctx, []byte("s:volatile"), exp))
	for i := 0; i < 10; i++ {
		require.NoError(t, p.Set(ctx, []byte("s:tmp"+string(rune('a'+i))), exp))
	}
	_, err := p.Get(ctx, []byte("persist"))
	require.NoError(t, err)
}

func Test_Evict_when_NothingEvictable(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("volatile-lru"))
	p.SetMaxBytes(60)

	persistVal := datastruct.Encode(datastruct.TypeString, 0, []byte(strings.Repeat("v", 20)))
	require.NoError(t, p.Set(ctx, []byte("k1"), persistVal))
	err := p.Set(ctx, []byte("k2"), persistVal)
	require.ErrorIs(t, err, ErrOOM)
	_, err = p.Get(ctx, []byte("k2"))
	require.ErrorIs(t, err, ErrNotFound)
}
