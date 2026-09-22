package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_FsyncPolicy_when_Parse(t *testing.T) {
	for s, want := range map[string]FsyncPolicy{
		"always":   FsyncAlways,
		"everysec": FsyncEverysec,
		"no":       FsyncNo,
		"ALWAYS":   FsyncAlways,
	} {
		got, err := ParseFsyncPolicy(s)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := ParseFsyncPolicy("bogus")
	require.Error(t, err)
}

func Test_Persistence_when_AlwaysRoundtrip(t *testing.T) {
	ctx := context.Background()
	p := NewWithOptions(t.TempDir(), Options{AppendOnly: true, Fsync: FsyncAlways})
	require.NoError(t, p.Open())
	require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
	got, err := p.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, p.Close())

	require.NoError(t, p.Open())
	got, err = p.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, p.Close())
}

func Test_Persistence_when_EverysecFlushes(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	p := NewWithOptions(dir, Options{AppendOnly: true, Fsync: FsyncEverysec})
	p.flushInterval = 10 * time.Millisecond
	require.NoError(t, p.Open())
	require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
	time.Sleep(200 * time.Millisecond)
	require.NoError(t, p.Close())

	p2 := NewWithOptions(dir, Options{AppendOnly: true, Fsync: FsyncEverysec})
	require.NoError(t, p2.Open())
	got, err := p2.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, p2.Close())
}

func Test_Persistence_when_ReopenMatrix(t *testing.T) {
	ctx := context.Background()
	for name, opts := range map[string]Options{
		"always":    {AppendOnly: true, Fsync: FsyncAlways},
		"everysec":  {AppendOnly: true, Fsync: FsyncEverysec},
		"no":        {AppendOnly: true, Fsync: FsyncNo},
		"nowal":     {AppendOnly: false, Fsync: FsyncNo},
		"default":   DefaultOptions(),
		"legacyNew": {AppendOnly: true},
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			p := NewWithOptions(dir, opts)
			require.NoError(t, p.Open())
			require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
			require.NoError(t, p.Close())

			p2 := NewWithOptions(dir, opts)
			require.NoError(t, p2.Open())
			got, err := p2.Get(ctx, []byte("k"))
			require.NoError(t, err)
			require.Equal(t, []byte("v"), got)
			require.NoError(t, p2.Close())
		})
	}
}

func Test_Persistence_when_AppendOnlyDisabled(t *testing.T) {
	ctx := context.Background()
	p := NewWithOptions(t.TempDir(), Options{AppendOnly: false, Fsync: FsyncNo})
	require.NoError(t, p.Open())
	require.NoError(t, p.Set(ctx, []byte("k"), []byte("v")))
	got, err := p.Get(ctx, []byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), got)
	require.NoError(t, p.Close())
}
