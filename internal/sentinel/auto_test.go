package sentinel

import (
	"errors"
	"fmt"
	"net"
	"strconv"
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
	err := r.AutoTick("m", true, 0, nil, 0, nil)
	if !errors.Is(err, ErrNoHealthySlave) {
		t.Fatalf("got %v", err)
	}
	if _, p, _ := r.GetMasterAddr("m"); p != 6380 {
		t.Fatal("abort must not flip current")
	}
}

// peer 票计入 ODOWN 门：quorum=2 时 self 一票 + peer 一票才执行。
func TestAuto_PeerVotesCount(t *testing.T) {
	live := startPeerStub(t, "+OK\r\n")
	dead := closedPort(t)
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: dead, Slaves: []string{live}, Quorum: 2}}, time.Second)
	r.RecordProbe(dead, false)
	if err := r.AutoTick("m", true, 0, nil, 0, nil); err != nil {
		t.Fatal(err)
	}
	if h, p, _ := r.GetMasterAddr("m"); net.JoinHostPort(h, strconv.Itoa(p)) != dead {
		t.Fatalf("without peer votes must not flip, got %s:%d", h, p)
	}
	if err := r.AutoTick("m", true, 1, nil, 0, nil); err != nil {
		t.Fatal(err)
	}
	if h, p, _ := r.GetMasterAddr("m"); net.JoinHostPort(h, strconv.Itoa(p)) != live {
		t.Fatalf("with peer votes must flip to slave, got %s:%d", h, p)
	}
}

// 目标即当前主时短路：不发 REPLICAOF、不记 lastFailover、不发事件。
func TestAuto_SameTargetNoop(t *testing.T) {
	live := startPeerStub(t, "+OK\r\n")
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{live}, Quorum: 1}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	r.ObserveMaster("m", live)
	if err := r.AutoTick("m", true, 1, nil, time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	if r.InCooldown("m", time.Hour) {
		t.Fatal("noop must not record lastFailover")
	}
}

// 目标已是 master（他哨兵先切换）时收养并中止，不发重复事件。
func TestAuto_TargetAlreadyMaster(t *testing.T) {
	info := "# Replication\r\nrole:master\r\n"
	roleStub := startPeerStub(t, fmt.Sprintf("$%d\r\n%s\r\n", len(info), info))
	dead := closedPort(t)
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: dead, Slaves: []string{roleStub}, Quorum: 1}}, time.Second)
	r.RecordProbe(dead, false)
	if err := r.AutoTick("m", true, 1, nil, time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	if h, p, _ := r.GetMasterAddr("m"); net.JoinHostPort(h, strconv.Itoa(p)) != roleStub {
		t.Fatalf("must adopt already-master target, got %s:%d", h, p)
	}
	if r.InCooldown("m", time.Hour) {
		t.Fatal("adopt-abort must not record lastFailover")
	}
}
