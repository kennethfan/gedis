package config

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/stretchr/testify/require"
)

func Test_Config_Load_parses_all_sections(t *testing.T) {
	// Given — a golden TOML file
	toml := `
[server]
host = "127.0.0.1"
port = 6380

[storage]
datadir = "/tmp/gedis-test"

[memory]
maxmemory = 1073741824
policy = "allkeys-lru"

[persistence]
appendonly = true
fsync = "everysec"

[metrics]
enabled = true
port = 9121

[lua]
time_limit = 100
`
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	// When
	cfg, err := Load(path)

	// Then
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", cfg.Server.Host)
	require.Equal(t, 6380, cfg.Server.Port)
	require.Equal(t, "/tmp/gedis-test", cfg.Storage.DataDir)
	require.Equal(t, int64(1073741824), cfg.Memory.MaxMemory)
	require.Equal(t, "allkeys-lru", cfg.Memory.Policy)
	require.True(t, cfg.Persistence.AppendOnly)
	require.Equal(t, "everysec", cfg.Persistence.Fsync)
	require.True(t, cfg.Metrics.Enabled)
	require.Equal(t, 9121, cfg.Metrics.Port)
	require.NotNil(t, cfg.Lua.TimeLimitMs)
	require.Equal(t, int64(100), *cfg.Lua.TimeLimitMs)
}

func Test_Config_Lua_absent_section_defaults_to_5000ms(t *testing.T) {
	// Given — a TOML without [lua]
	toml := `
[server]
host = "127.0.0.1"
port = 6380
`
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	// When
	cfg, err := Load(path)

	// Then — key absent, effective limit falls back to the default
	require.NoError(t, err)
	require.Nil(t, cfg.Lua.TimeLimitMs)
	got, err := cfg.Lua.EffectiveTimeLimit()
	require.NoError(t, err)
	require.Equal(t, 5*time.Second, got)
}

func Test_Config_Lua_EffectiveTimeLimit(t *testing.T) {
	// Given — table-driven ms inputs
	ptr := func(v int64) *int64 { return &v }
	cases := []struct {
		name    string
		ms      *int64
		want    time.Duration
		wantErr bool
	}{
		{"explicit value honored", ptr(100), 100 * time.Millisecond, false},
		{"explicit 0 means unlimited", ptr(0), 0, false},
		{"negative rejected", ptr(-1), 0, true},
		{"overflowing Duration rejected", ptr(math.MaxInt64), 0, true},
	}

	// When + Then
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Lua{TimeLimitMs: tc.ms}.EffectiveTimeLimit()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func Test_Config_Load_rejects_missing_file(t *testing.T) {
	// Given — a path that does not exist
	path := filepath.Join(t.TempDir(), "nope.toml")

	// When
	_, err := Load(path)

	// Then
	require.ErrorIs(t, err, ErrNotFound)
}

func Test_Config_Cluster_parses_nodes_and_specs(t *testing.T) {
	toml := `
[server]
host = "127.0.0.1"
port = 7000

[cluster]
enabled = true

[[cluster.nodes]]
id = "nodeA"
addr = "127.0.0.1:7000"
slots = ["0-5460", "7001"]

[[cluster.nodes]]
addr = "127.0.0.1:7001"
slots = ["5461-7000", "7002-16383"]
`
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte(toml), 0o600))

	cfg, err := Load(path)
	require.NoError(t, err)
	require.True(t, cfg.Cluster.Enabled)
	require.Len(t, cfg.Cluster.Nodes, 2)

	specs, err := cfg.Cluster.Specs()
	require.NoError(t, err)
	require.Equal(t, "nodeA", specs[0].ID)
	require.Equal(t, [][2]int{{0, 5460}, {7001, 7001}}, specs[0].Ranges)
	require.Equal(t, "", specs[1].ID)

	topo, err := cluster.Build("127.0.0.1:7000", specs)
	require.NoError(t, err)
	require.Len(t, topo.Nodes()[1].ID, 40)
	require.True(t, topo.Owns(0))
	require.False(t, topo.Owns(5461))
}

func Test_Config_Cluster_absent_means_disabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "gedis.toml")
	require.NoError(t, os.WriteFile(path, []byte("[server]\nport = 1\n"), 0o600))
	cfg, err := Load(path)
	require.NoError(t, err)
	require.False(t, cfg.Cluster.Enabled)
	require.Empty(t, cfg.Cluster.Nodes)
}

func Test_Config_Cluster_bad_slots_fail_fast(t *testing.T) {
	c := Cluster{Nodes: []ClusterNode{{Addr: "127.0.0.1:7000", Slots: []string{"0-99999"}}}}
	_, err := c.Specs()
	require.Error(t, err)
}
