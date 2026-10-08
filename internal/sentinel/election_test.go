package sentinel

import (
	"testing"
	"time"
)

func TestElection_GrantHigherEpoch(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	ok, ep := r.HandleVote("m", 1, "runA")
	if !ok || ep != 1 {
		t.Fatalf("got %v %d", ok, ep)
	}
}

func TestElection_RejectLowerEpoch(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 2}}, time.Second)
	r.HandleVote("m", 3, "runA")
	if ok, _ := r.HandleVote("m", 2, "runB"); ok {
		t.Fatal("lower epoch must be rejected")
	}
	if ok, _ := r.HandleVote("m", 3, "runB"); ok {
		t.Fatal("same epoch second candidate must be rejected")
	}
}

func TestElection_Majority(t *testing.T) {
	if !hasMajority(2, 3) || hasMajority(1, 3) {
		t.Fatal("majority rule broken")
	}
}
