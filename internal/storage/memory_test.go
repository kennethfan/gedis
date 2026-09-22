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
