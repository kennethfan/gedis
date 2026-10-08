package sentinel

import (
	"testing"
)

// SeedPeers 把种子地址记为占位对端；非法地址忽略。
func TestSeedPeers_Placeholder(t *testing.T) {
	p := NewPeerTable()
	p.SeedPeers([]string{"127.0.0.1:27401", "bad-addr", "127.0.0.1:"})
	addrs := p.Addrs()
	if len(addrs) != 1 || addrs[0] != "127.0.0.1:27401" {
		t.Fatalf("got %v", addrs)
	}
}

func TestSeedPeers_DedupWithRealHello(t *testing.T) {
	p := NewPeerTable()
	p.SeedPeers([]string{"127.0.0.1:27401"})
	p.Upsert(Hello{IP: "127.0.0.1", Port: "27401", RunID: NewRunID(), Epoch: 1})
	if addrs := p.Addrs(); len(addrs) != 1 || addrs[0] != "127.0.0.1:27401" {
		t.Fatalf("seed+hello dup: got %v", addrs)
	}
}

func TestNormalizeAddr_Localhost(t *testing.T) {
	if got := NormalizeAddr("localhost:26379"); got != "127.0.0.1:26379" {
		t.Fatalf("got %q", got)
	}
}
