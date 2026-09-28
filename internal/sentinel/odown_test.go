package sentinel

import (
	"testing"
	"time"
)

func TestODOWN_QuorumBoundary(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if r.IsObjectivelyDown("m", 0) {
		t.Fatal("self-only must not reach quorum 2")
	}
	if !r.IsObjectivelyDown("m", 1) {
		t.Fatal("self+1 peer must reach quorum 2")
	}
}

func TestODOWN_QuorumUnreachable(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 5}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if r.IsObjectivelyDown("m", 3) {
		t.Fatal("quorum beyond cluster must never be odown")
	}
}
