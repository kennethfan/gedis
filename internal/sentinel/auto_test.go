package sentinel

import (
	"errors"
	"testing"
	"time"
)

func TestAuto_CooldownSuppresses(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	if !r.InCooldown("m", 0) {
		// zero lastFailover + 0 timeout: only true if lastFailover set; fresh registry must NOT be in cooldown
	}
	if r.InCooldown("m", time.Hour) {
		t.Fatal("fresh registry must not be in cooldown")
	}
}

func TestAuto_AbortNoHealthy(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	r.SetDown("127.0.0.1:6381", true)
	err := r.AutoTick("m", true, nil, 0, nil)
	if !errors.Is(err, ErrNoHealthySlave) {
		t.Fatalf("got %v", err)
	}
	if _, p, _ := r.GetMasterAddr("m"); p != 6380 {
		t.Fatal("abort must not flip current")
	}
}
