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

func TestObserveMaster_Unconditional(t *testing.T) {	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, 0)
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

func TestFindPromotedMaster_FindsOther(t *testing.T) {
	masterStub := startPeerStub(t, "$33\r\nrole:master\r\nconnected_slaves:0\r\n\r\n")
	slaveStub := startPeerStub(t, "$58\r\nrole:slave\r\nmaster_link_status:up\r\nslave_repl_offset:99\r\n\r\n")
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{slaveStub, masterStub}, Quorum: 1}}, 0)
	got, ok := r.FindPromotedMaster("m", []string{slaveStub, masterStub})
	if !ok || got != masterStub {
		t.Fatalf("must find already-promoted master, got %q,%v", got, ok)
	}
}

func TestFindPromotedMaster_NoneWhenAllSlaves(t *testing.T) {
	slaveStub := startPeerStub(t, "$58\r\nrole:slave\r\nmaster_link_status:up\r\nslave_repl_offset:99\r\n\r\n")
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{slaveStub}, Quorum: 1}}, 0)
	if got, ok := r.FindPromotedMaster("m", []string{slaveStub}); ok {
		t.Fatalf("no master among slaves must report none, got %q", got)
	}
}
