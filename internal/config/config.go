package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/sentinel"
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
	Sentinel    Sentinel    `toml:"sentinel"`
	// RequirePass 映射为 default 用户口令（空=默认全开放）；ACLFile 缺席即关闭持久化。
	RequirePass string `toml:"requirepass"`
	ACLFile     string `toml:"aclfile"`
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

// Sentinel holds 哨兵配置：缺席即关闭；down_after_ms 缺席默认 5000，
// 显式 0 表示只报配置主、不探活；<0 启动报错。failover_timeout_ms 缺席
// 默认 30000，显式 0 表示自动转移无冷却。sentinels 为种子哨兵地址列表。
type Sentinel struct {
	Enabled         bool             `toml:"enabled"`
	Port            int              `toml:"port"`
	DownAfterMs     int64            `toml:"down_after_ms"`
	FailoverTimeoutMs int64          `toml:"failover_timeout_ms"`
	Sentinels       []string         `toml:"sentinels"`
	Masters         []SentinelMaster `toml:"masters"`
}

// SentinelMaster 是 [[sentinel.masters]] 表：quorum 为 ODOWN 判定阈值
// （含自己一票）；缺席/0 默认 1，<0 启动报错。
type SentinelMaster struct {
	Name       string   `toml:"name"`
	MasterAddr string   `toml:"master_addr"`
	Quorum     int      `toml:"quorum"`
	Slaves     []string `toml:"slaves"`
}

// DefaultSentinelPort 是哨兵口默认端口（Port==0 时用）。
const DefaultSentinelPort = 26379

// DefaultSentinelDownAfterMs 是 down_after_ms 缺席时的默认值。
const DefaultSentinelDownAfterMs = 5000

// DefaultSentinelFailoverTimeoutMs 是 failover_timeout_ms 缺席时的默认值。
const DefaultSentinelFailoverTimeoutMs = 30000

// Specs 展开全部 masters 为 sentinel.NodeSpec（fail-fast，返回首错）。
func (s Sentinel) Specs() ([]sentinel.NodeSpec, error) {
	if s.DownAfterMs < 0 {
		return nil, fmt.Errorf("sentinel.down_after_ms must be >= 0, got %d", s.DownAfterMs)
	}
	for _, seed := range s.Sentinels {
		if err := checkHostPort(seed); err != nil {
			return nil, fmt.Errorf("sentinel seed %q: %w", seed, err)
		}
	}
	seen := make(map[string]struct{}, len(s.Masters))
	var out []sentinel.NodeSpec
	for _, m := range s.Masters {
		if m.Name == "" {
			return nil, fmt.Errorf("sentinel master name must not be empty")
		}
		if _, dup := seen[m.Name]; dup {
			return nil, fmt.Errorf("sentinel master %q duplicated", m.Name)
		}
		seen[m.Name] = struct{}{}
		if err := checkHostPort(m.MasterAddr); err != nil {
			return nil, fmt.Errorf("sentinel master %q: %w", m.Name, err)
		}
		if len(m.Slaves) == 0 {
			return nil, fmt.Errorf("sentinel master %q: slaves must not be empty", m.Name)
		}
		for _, sl := range m.Slaves {
			if err := checkHostPort(sl); err != nil {
				return nil, fmt.Errorf("sentinel master %q: %w", m.Name, err)
			}
		}
		q := m.Quorum
		if q == 0 {
			q = 1
		}
		if q < 0 {
			return nil, fmt.Errorf("sentinel master %q: quorum must be >= 0, got %d", m.Name, m.Quorum)
		}
		out = append(out, sentinel.NodeSpec{Name: m.Name, MasterAddr: m.MasterAddr, Slaves: m.Slaves, Quorum: q})
	}
	return out, nil
}

func checkHostPort(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return fmt.Errorf("invalid addr %q", addr)
	}
	return nil
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
	md, err := toml.Decode(string(data), &cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse %s: %w", path, err)
	}
	if md.IsDefined("sentinel") && !md.IsDefined("sentinel", "down_after_ms") {
		cfg.Sentinel.DownAfterMs = DefaultSentinelDownAfterMs
	}
	if md.IsDefined("sentinel") && !md.IsDefined("sentinel", "failover_timeout_ms") {
		cfg.Sentinel.FailoverTimeoutMs = DefaultSentinelFailoverTimeoutMs
	}
	return cfg, nil
}
