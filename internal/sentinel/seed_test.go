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
