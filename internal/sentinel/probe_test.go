package sentinel

import (
	"net"
	"testing"
	"time"
)

func closedPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// 探活失败必须累计 SDOWN（生产环境自动转移的输入）。
func TestProbe_RecordsSDown(t *testing.T) {
	dead := closedPort(t)
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: dead, Slaves: []string{dead}, Quorum: 1}}, time.Second)
	r.ProbeOnce()
	if !r.IsSubjectivelyDown(dead) {
		t.Fatal("probe failure must record sdown")
	}
}

// failover 后探活必须跟随有效主（current），而非配置主事先；
// 否则旧主反复被标 down，他哨兵的 ODOWN 永不清。
func TestProbe_UsesCurrentMaster(t *testing.T) {
	live := startPeerStub(t, "+OK\r\n")
	dead := closedPort(t)
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: dead, Slaves: []string{live}, Quorum: 1}}, time.Second)
	if err := r.Failover("m"); err != nil {
		t.Fatal(err)
	}
	r.RecordProbe(dead, true)
	r.SetDown(dead, false)
	r.ProbeOnce()
	if r.IsDown(dead) {
		t.Fatal("after failover probes must follow current master, not spec master")
	}
	if r.IsDown(live) {
		t.Fatal("live current master must not be marked down")
	}
}
