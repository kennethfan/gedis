package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Config_TLS_absent_section_defaults_disabled(t *testing.T) {
	// Given — 无 [tls] 段的最小配置
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte("[server]\nhost = \"127.0.0.1\"\nport = 6380\n"), 0o600))

	// When
	cfg, err := Load(path)

	// Then
	require.NoError(t, err)
	require.False(t, cfg.TLS.Enabled)
	require.Empty(t, cfg.TLS.CertFile)
	require.Empty(t, cfg.TLS.KeyFile)
	require.False(t, cfg.TLS.InsecureSkipVerify)
}

func Test_Config_TLS_parses_enabled_with_cert_files(t *testing.T) {
	// Given — 带 [tls] 段的配置
	toml := `[server]
host = "127.0.0.1"
port = 6380

[tls]
enabled = true
cert_file = "/tmp/gedis-test/server.crt"
key_file = "/tmp/gedis-test/server.key"
`
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	// When
	cfg, err := Load(path)

	// Then
	require.NoError(t, err)
	require.True(t, cfg.TLS.Enabled)
	require.Equal(t, "/tmp/gedis-test/server.crt", cfg.TLS.CertFile)
	require.Equal(t, "/tmp/gedis-test/server.key", cfg.TLS.KeyFile)
	require.False(t, cfg.TLS.InsecureSkipVerify)
}

func Test_Config_TLS_parses_insecure_skip_verify(t *testing.T) {
	// Given — [tls] 段显式打开 insecure_skip_verify
	toml := `[server]
host = "127.0.0.1"
port = 6380

[tls]
enabled = true
cert_file = "/tmp/gedis-test/server.crt"
key_file = "/tmp/gedis-test/server.key"
insecure_skip_verify = true
`
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	// When
	cfg, err := Load(path)

	// Then
	require.NoError(t, err)
	require.True(t, cfg.TLS.InsecureSkipVerify)
}
