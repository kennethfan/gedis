package sentinel

import (
	"testing"
	"time"
)

func TestSelect_PriorityOffsetRunID(t *testing.T) {
	infos := []SlaveInfo{
		{Addr: "127.0.0.1:6381", Priority: 100, Offset: 900, RunID: "zzz"},
		{Addr: "127.0.0.1:6382", Priority: 100, Offset: 1000, RunID: "aaa"},
		{Addr: "127.0.0.1:6383", Priority: 50, Offset: 10, RunID: "mmm"},
	}
	got := SortSlaves(infos)
	if got[0].Addr != "127.0.0.1:6383" || got[1].Addr != "127.0.0.1:6382" {
		t.Fatalf("order wrong: %+v", got)
	}
}

func TestSelect_SkipsDown(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381", "127.0.0.1:6382"}, Quorum: 1}}, time.Second)
	r.SetDown("127.0.0.1:6381", true)
	addr, err := r.SelectSlave("m", []SlaveInfo{{Addr: "127.0.0.1:6381", Priority: 100, Offset: 999}, {Addr: "127.0.0.1:6382", Priority: 100, Offset: 1}})
	if err != nil || addr != "127.0.0.1:6382" {
		t.Fatalf("got %q %v", addr, err)
	}
}
