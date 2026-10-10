package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

const rdbFixture = "../../internal/rdb/testdata/fix-p0.rdb"

func Test_LoadRDBFile_empty_db_loads_fixture(t *testing.T) {
	s := storage.New(t.TempDir())
	require.NoError(t, s.Open())
	defer func() { _ = s.Close() }()

	require.NoError(t, loadRDBFile(context.Background(), s, rdbFixture))

	raw, err := s.Get(context.Background(), []byte("s:str0"))
	require.NoError(t, err)
	require.Contains(t, string(raw), "hello")
}

func Test_LoadRDBFile_nonempty_db_refuses(t *testing.T) {
	s := storage.New(t.TempDir())
	require.NoError(t, s.Open())
	defer func() { _ = s.Close() }()

	require.NoError(t, s.Set(context.Background(), []byte("s:x"), []byte("y")))
	err := loadRDBFile(context.Background(), s, rdbFixture)
	require.ErrorContains(t, err, "not empty")
}

func Test_LoadRDBFile_missing_file_errors(t *testing.T) {
	s := storage.New(t.TempDir())
	require.NoError(t, s.Open())
	defer func() { _ = s.Close() }()

	err := loadRDBFile(context.Background(), s, filepath.Join(t.TempDir(), "nope.rdb"))
	require.Error(t, err)
}
