package storage

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/replication"
	"github.com/stretchr/testify/require"
)

func Test_Publish_when_SetDelete(t *testing.T) {
	ctx := context.Background()
	hub := replication.NewHub(16)
	p := NewWithOptions(t.TempDir(), Options{AppendOnly: true, Fsync: FsyncNo, Hub: hub})
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
	require.NoError(t, p.Delete(ctx, []byte("k")))

	ops, err := hub.Backlog().Since(0)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	require.False(t, ops[0].Del)
	require.Equal(t, []byte("k"), ops[0].Key)
	require.True(t, ops[1].Del)
}

func Test_Publish_when_WriteBatch(t *testing.T) {
	ctx := context.Background()
	hub := replication.NewHub(16)
	p := NewWithOptions(t.TempDir(), Options{AppendOnly: true, Fsync: FsyncNo, Hub: hub})
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()

	require.NoError(t, p.WriteBatch(ctx, []BatchOp{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("gone"), Delete: true},
	}))
	ops, err := hub.Backlog().Since(0)
	require.NoError(t, err)
	require.Len(t, ops, 3)
	require.False(t, ops[0].Del)
	require.Equal(t, []byte("a"), ops[0].Key)
	require.True(t, ops[2].Del)
}

func Test_Publish_when_NoHub(t *testing.T) {
	ctx := context.Background()
	p := New(t.TempDir())
	require.NoError(t, p.Open())
	defer func() { _ = p.Close() }()
	require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
}
