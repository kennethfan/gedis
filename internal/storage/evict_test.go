package storage

import (
	"bytes"
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

// noeviction：超限必须 OOM，已写 key 原样保留。
func TestEvictNoEvictionReturnsOOM(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("noeviction"))
	p.SetMaxBytes(128)

	require.NoError(t, p.Set(ctx, []byte("s:k1"), bytes.Repeat([]byte("x"), 100)))
	err := p.Set(ctx, []byte("s:k2"), bytes.Repeat([]byte("y"), 100))
	require.ErrorIs(t, err, ErrOOM)
	_, err = p.Get(ctx, []byte("s:k1"))
	require.NoError(t, err, "noeviction 不得逐出任何 key")
}

// volatile-ttl：逐出先到期的带 TTL key，永不动无 TTL key。
func TestEvictVolatileTTLKeepsNonExpiringKey(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("volatile-ttl"))

	payload := []byte(strings.Repeat("v", 40))
	noTTL := datastruct.Encode(datastruct.TypeString, 0, payload)
	shortTTL := datastruct.Encode(datastruct.TypeString, time.Now().Add(10*time.Second).UnixNano(), payload)
	longTTL := datastruct.Encode(datastruct.TypeString, time.Now().Add(time.Hour).UnixNano(), payload)

	// 三条等大（key 均 3 字节、payload 同长、编码头同宽）；max 恰容 2 条
	keySize := int64(len("s:a") + len(shortTTL))
	p.SetMaxBytes(2 * keySize)

	require.NoError(t, p.Set(ctx, []byte("s:a"), noTTL))
	require.NoError(t, p.Set(ctx, []byte("s:b"), shortTTL))
	require.NoError(t, p.Set(ctx, []byte("s:c"), longTTL))

	require.Equal(t, int64(1), p.EvictedCount())
	_, err := p.Get(ctx, []byte("s:a"))
	require.NoError(t, err, "无 TTL key 不得被 volatile-ttl 逐出")
	_, err = p.Get(ctx, []byte("s:b"))
	require.ErrorIs(t, err, ErrNotFound, "先到期的短 TTL key 应先被逐")
	_, err = p.Get(ctx, []byte("s:c"))
	require.NoError(t, err)
}

// allkeys-lfu：逐出采样中访问频次最低的 key，与写入新旧（LRU）无关。
func TestEvictLFULowestFreqFirst(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.SetPolicy("allkeys-lfu"))

	val := bytes.Repeat([]byte("v"), 100)
	// 三条等大；max 恰容 2 条
	p.SetMaxBytes(int64(2 * (len("s:a") + len(val))))

	require.NoError(t, p.Set(ctx, []byte("s:a"), val))
	require.NoError(t, p.Set(ctx, []byte("s:b"), val))
	for i := 0; i < 10; i++ {
		_, err := p.Get(ctx, []byte("s:a"))
		require.NoError(t, err)
	}
	// 第三条超限（309 > 206）→ 逐出频次最低的 b（freq 1 < a 的 11）
	require.NoError(t, p.Set(ctx, []byte("s:c"), val))

	require.Equal(t, int64(1), p.EvictedCount())
	_, err := p.Get(ctx, []byte("s:b"))
	require.ErrorIs(t, err, ErrNotFound, "低频 key 应先被逐")
	_, err = p.Get(ctx, []byte("s:a"))
	require.NoError(t, err)
	_, err = p.Get(ctx, []byte("s:c"))
	require.NoError(t, err)
}
