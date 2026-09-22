package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/BurntSushi/toml"
)

// ErrNotFound is returned when the config file does not exist.
var ErrNotFound = errors.New("config: file not found")

// Config is the parsed redrock.toml. Fields are exported for TOML
// decoding; use Load as the sole entry point (parse-don't-validate).
type Config struct {
	Server      Server      `toml:"server"`
	Storage     Storage     `toml:"storage"`
	Memory      Memory      `toml:"memory"`
	Persistence Persistence `toml:"persistence"`
	Metrics     Metrics     `toml:"metrics"`
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
