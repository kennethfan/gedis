package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Memory_when_TracksSizes(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	require.NoError(t, p.Set(ctx, []byte("k1"), []byte("12345")))
	require.Equal(t, int64(len("k1")+5), p.UsedBytes())
	require.NoError(t, p.Set(ctx, []byte("k1"), []byte("12")))
	require.Equal(t, int64(len("k1")+2), p.UsedBytes())
	require.NoError(t, p.Set(ctx, []byte("k22"), []byte("x")))
	require.Equal(t, int64(len("k1")+2+len("k22")+1), p.UsedBytes())
	require.NoError(t, p.Delete(ctx, []byte("k1")))
	require.Equal(t, int64(len("k22")+1), p.UsedBytes())
}

func Test_Memory_when_PolicyValidation(t *testing.T) {
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	require.NoError(t, p.SetPolicy("allkeys-lru"))
	require.NoError(t, p.SetPolicy("volatile-lru"))
	require.Error(t, p.SetPolicy("bogus"))
	require.Equal(t, "volatile-lru", p.Policy())
	p.SetMaxBytes(1024)
	require.Equal(t, int64(1024), p.MaxBytes())
}

// Redis 八种驱逐策略全部合法；非法策略报错且不改已存策略。
func TestSetPolicyAcceptsRedisEvictionPolicies(t *testing.T) {
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	for _, pol := range []string{
		"noeviction", "allkeys-random", "volatile-random", "volatile-ttl",
		"allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu",
	} {
		require.NoError(t, p.SetPolicy(pol), "policy %q 应被接受", pol)
		require.Equal(t, pol, p.Policy())
	}
	require.Error(t, p.SetPolicy("bogus"))
	require.Equal(t, "volatile-lfu", p.Policy())
}
