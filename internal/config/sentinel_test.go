package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSentinelSpecs_Valid(t *testing.T) {
	cfg := Sentinel{
		Enabled: true, Port: 26379, DownAfterMs: 5000,
		Masters: []SentinelMaster{{Name: "mymaster", MasterAddr: "127.0.0.1:6380", Quorum: 1, Slaves: []string{"127.0.0.1:6381"}}},
	}
	specs, err := cfg.Specs()
	if err != nil {
		t.Fatalf("specs: %v", err)
	}
	if len(specs) != 1 || specs[0].Name != "mymaster" {
		t.Fatalf("got %+v", specs)
	}
}

func TestSentinelSpecs_DupNameFails(t *testing.T) {
	cfg := Sentinel{Masters: []SentinelMaster{
		{Name: "a", MasterAddr: "127.0.0.1:6380"},
		{Name: "a", MasterAddr: "127.0.0.1:6382"},
	}}
	if _, err := cfg.Specs(); err == nil {
		t.Fatal("expect dup name error")
	}
}

func TestSentinelSpecs_BadAddrFails(t *testing.T) {
	cfg := Sentinel{Masters: []SentinelMaster{
		{Name: "a", MasterAddr: "not-an-addr", Slaves: []string{"127.0.0.1:6381"}},
	}}
	if _, err := cfg.Specs(); err == nil {
		t.Fatal("expect bad addr error")
	}
}

func TestSentinelSpecs_NegativeDownAfterFails(t *testing.T) {
	cfg := Sentinel{DownAfterMs: -1, Masters: []SentinelMaster{
		{Name: "a", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}},
	}}
	if _, err := cfg.Specs(); err == nil {
		t.Fatal("expect negative down_after_ms error")
	}
}

func TestSentinelLoad_DownAfterDefault(t *testing.T) {
	dir := t.TempDir()
	absent := filepath.Join(dir, "absent.toml")
	if err := os.WriteFile(absent, []byte("[sentinel]\nenabled = true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(absent)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sentinel.DownAfterMs != DefaultSentinelDownAfterMs {
		t.Fatalf("absent down_after_ms: got %d, want %d", cfg.Sentinel.DownAfterMs, DefaultSentinelDownAfterMs)
	}
	explicit := filepath.Join(dir, "explicit.toml")
	if err := os.WriteFile(explicit, []byte("[sentinel]\nenabled = true\ndown_after_ms = 0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg2, err := Load(explicit)
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.Sentinel.DownAfterMs != 0 {
		t.Fatalf("explicit down_after_ms=0: got %d, want 0", cfg2.Sentinel.DownAfterMs)
	}
}
