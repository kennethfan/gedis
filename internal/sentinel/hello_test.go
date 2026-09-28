package sentinel

import "testing"

func TestHello_RoundTrip(t *testing.T) {
	h := Hello{IP: "127.0.0.1", Port: "26379", RunID: "abc", Epoch: 3, Master: "m", MasterIP: "127.0.0.1", MasterPort: 6380, MasterEpoch: 3}
	got, err := ParseHello(h.Encode())
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != h {
		t.Fatalf("got %+v want %+v", got, h)
	}
}

func TestHello_PeerUpsert(t *testing.T) {
	pt := NewPeerTable()
	pt.Upsert(Hello{RunID: "a", Epoch: 1})
	pt.Upsert(Hello{RunID: "a", Epoch: 2})
	if len(pt.Addrs()) != 0 {
		t.Fatal("peer without addr must not list")
	}
	pt.Upsert(Hello{IP: "127.0.0.1", Port: "26380", RunID: "a", Epoch: 2})
	if len(pt.Addrs()) != 1 {
		t.Fatal("expect 1 peer")
	}
}
