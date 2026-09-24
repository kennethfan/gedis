package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/BurntSushi/toml"
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
