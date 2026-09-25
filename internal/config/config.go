package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/kennethfan/gedis/internal/cluster"
)

// ErrNotFound is returned when the config file does not exist.
var ErrNotFound = errors.New("config: file not found")

// Config is the parsed gedis.toml. Fields are exported for TOML
// decoding; use Load as the sole entry point (parse-don't-validate).
type Config struct {
	Server      Server      `toml:"server"`
	Storage     Storage     `toml:"storage"`
	Memory      Memory      `toml:"memory"`
	Persistence Persistence `toml:"persistence"`
	Metrics     Metrics     `toml:"metrics"`
	Lua         Lua         `toml:"lua"`
	Cluster     Cluster     `toml:"cluster"`
}

// Server holds listener settings.
type Server struct {
	Host string `toml:"host"`
	Port int    `toml:"port"`
}

// Storage holds engine paths.
type Storage struct {
	DataDir string `toml:"datadir"`
}

// Memory holds eviction settings.
type Memory struct {
	MaxMemory int64  `toml:"maxmemory"`
	Policy    string `toml:"policy"`
}

// Persistence holds WAL/fsync settings.
type Persistence struct {
	AppendOnly bool   `toml:"appendonly"`
	Fsync      string `toml:"fsync"`
}

// Metrics holds the Prometheus endpoint settings. Disabled by default
// (Enabled=false or Port=0 means no HTTP server).
type Metrics struct {
	Enabled bool `toml:"enabled"`
	Port    int  `toml:"port"`
}

// DefaultLuaTimeLimitMs mirrors Redis' lua-time-limit default (5000ms).
const DefaultLuaTimeLimitMs = 5000

// Lua holds scripting settings.
type Lua struct {
	// TimeLimitMs caps a single script run in milliseconds.
	// Nil (section/key absent) means the default; explicit 0 disables
	// the limit (mirrors Redis, where 0 disables lua-time-limit).
	TimeLimitMs *int64 `toml:"time_limit"`
}

// EffectiveTimeLimit resolves the configured limit: default when absent,
// 0 (unlimited) when explicitly 0. Negative or overflowing values error.
func (l Lua) EffectiveTimeLimit() (time.Duration, error) {
	ms := int64(DefaultLuaTimeLimitMs)
	if l.TimeLimitMs != nil {
		ms = *l.TimeLimitMs
	}
	if ms < 0 {
		return 0, fmt.Errorf("lua.time_limit must be between 0 and %d inclusive, got %d",
			int64(math.MaxInt64)/int64(time.Millisecond), ms)
	}
	if ms > math.MaxInt64/int64(time.Millisecond) {
		return 0, fmt.Errorf("lua.time_limit %d overflows time.Duration", ms)
	}
	return time.Duration(ms) * time.Millisecond, nil
}

// Cluster holds 静态集群拓扑配置（#40）：缺席即关闭；enabled 且无 nodes
// 即本节点持有全部分片。
type Cluster struct {
	Enabled bool          `toml:"enabled"`
	Nodes   []ClusterNode `toml:"nodes"`
}

// ClusterNode 是 [[cluster.nodes]] 表：slots 为混合段记法（"0-5460"/"7001"）。
// id 缺席时由 addr 稳定派生（cluster.DeriveID）。
type ClusterNode struct {
	ID    string   `toml:"id"`
	Addr  string   `toml:"addr"`
	Slots []string `toml:"slots"`
}

// Specs 展开全部节点 slots 为 cluster.NodeSpec（fail-fast，返回首错）。
func (c Cluster) Specs() ([]cluster.NodeSpec, error) {
	var out []cluster.NodeSpec
	for _, n := range c.Nodes {
		ranges, err := cluster.ParseSlotRanges(n.Slots)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", n.Addr, err)
		}
		out = append(out, cluster.NodeSpec{ID: n.ID, Addr: n.Addr, Ranges: ranges})
	}
	return out, nil
}

// Load parses the TOML file at path into a Config.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, fmt.Errorf("read %s: %w", path, ErrNotFound)
		}
		return Config{}, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return cfg, nil
}
