package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Batch_when_WriteBatch(t *testing.T) {
	ctx := context.Background()
	p := NewWithOptions(t.TempDir(), Options{AppendOnly: true, Fsync: FsyncAlways})
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	require.NoError(t, p.Set(ctx, []byte("old"), []byte("0")))
	err := p.WriteBatch(ctx, []BatchOp{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("old"), Delete: true},
	})
	require.NoError(t, err)
	got, err := p.Get(ctx, []byte("a"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), got)
	got, err = p.Get(ctx, []byte("b"))
	require.NoError(t, err)
	require.Equal(t, []byte("2"), got)
	_, err = p.Get(ctx, []byte("old"))
	require.ErrorIs(t, err, ErrNotFound)
}

func Test_Batch_when_Empty(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.WriteBatch(ctx, nil))
}
