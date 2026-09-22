package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Config_Load_parses_all_sections(t *testing.T) {
	// Given — a golden TOML file
	toml := `
[server]
host = "127.0.0.1"
port = 6380

[storage]
datadir = "/tmp/redrock-test"

[memory]
maxmemory = 1073741824
policy = "allkeys-lru"

[persistence]
appendonly = true
fsync = "everysec"

[metrics]
enabled = true
port = 9121
`
	path := filepath.Join(t.TempDir(), "redrock.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	// When
	cfg, err := Load(path)

	// Then
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", cfg.Server.Host)
	require.Equal(t, 6380, cfg.Server.Port)
	require.Equal(t, "/tmp/redrock-test", cfg.Storage.DataDir)
	require.Equal(t, int64(1073741824), cfg.Memory.MaxMemory)
	require.Equal(t, "allkeys-lru", cfg.Memory.Policy)
	require.True(t, cfg.Persistence.AppendOnly)
	require.Equal(t, "everysec", cfg.Persistence.Fsync)
	require.True(t, cfg.Metrics.Enabled)
	require.Equal(t, 9121, cfg.Metrics.Port)
}

func Test_Config_Load_rejects_missing_file(t *testing.T) {
	// Given — a path that does not exist
	path := filepath.Join(t.TempDir(), "nope.toml")

	// When
	_, err := Load(path)

	// Then
	require.ErrorIs(t, err, ErrNotFound)
}
