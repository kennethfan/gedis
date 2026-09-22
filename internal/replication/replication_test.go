package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Backlog_when_AppendSince(t *testing.T) {
	b := NewBacklog(16)
	b.Append(Op{Del: false, Key: []byte("k1"), Value: []byte("v1")})
	b.Append(Op{Del: true, Key: []byte("k2")})

	ops, err := b.Since(0)
	require.NoError(t, err)
	require.Len(t, ops, 2)
	require.Equal(t, []byte("k1"), ops[0].Key)
	require.Equal(t, int64(1), ops[0].Offset)
	require.True(t, ops[1].Del)

	ops, err = b.Since(1)
	require.NoError(t, err)
	require.Len(t, ops, 1)
	require.Equal(t, int64(2), ops[0].Offset)
}

func Test_Backlog_when_Stale(t *testing.T) {
	b := NewBacklog(2)
	b.Append(Op{Key: []byte("a")})
	b.Append(Op{Key: []byte("b")})
	b.Append(Op{Key: []byte("c")})

	_, err := b.Since(0)
	require.ErrorIs(t, err, ErrStaleOffset)
	ops, err := b.Since(1)
	require.NoError(t, err)
	require.Len(t, ops, 2)
}

func Test_RDB_when_Roundtrip(t *testing.T) {
	entries := []RawEntry{
		{Key: []byte("s:a"), Value: []byte("t\x00hello")},
		{Key: []byte("s:b"), Value: []byte{0, 1, 2, 255}},
	}
	raw := MarshalRDB(entries)
	got, err := UnmarshalRDB(raw)
	require.NoError(t, err)
	require.Equal(t, entries, got)

	_, err = UnmarshalRDB([]byte{1, 2, 3})
	require.Error(t, err)
}
