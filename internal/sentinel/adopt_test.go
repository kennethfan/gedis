package sentinel

import (
	"testing"
)

func TestAdoptMaster_HigherEpoch(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, 0)
	if !r.AdoptMaster("m", "127.0.0.1:6381", 3, "peer") {
		t.Fatal("higher epoch hello must be adopted")
	}
	h, p, _ := r.GetMasterAddr("m")
	if h != "127.0.0.1" || p != 6381 {
		t.Fatalf("got %s:%d", h, p)
	}
}

func TestAdoptMaster_StaleOrUnattributed(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, 0)
	if r.AdoptMaster("m", "127.0.0.1:6381", 0, "other") {
		t.Fatal("equal epoch without vote for sender must not adopt")
	}
	r.HandleVote("m", 1, "peer")
	if !r.AdoptMaster("m", "127.0.0.1:6381", 1, "peer") {
		t.Fatal("equal epoch from election winner must adopt")
	}
	if r.AdoptMaster("m", "127.0.0.1:6381", 1, "peer") {
		t.Fatal("adopting same addr must report no change")
	}
}

func TestObserveMaster_Unconditional(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, 0)
	if !r.ObserveMaster("m", "127.0.0.1:6381") {
		t.Fatal("must adopt observed master")
	}
	if r.ObserveMaster("m", "127.0.0.1:6381") {
		t.Fatal("same addr must report no change")
	}
	if r.ObserveMaster("nope", "127.0.0.1:6381") {
		t.Fatal("unknown master must not adopt")
	}
}
