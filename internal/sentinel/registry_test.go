package sentinel

import (
	"bufio"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

type stubCmd struct {
	name string
	args []string
}

type stubServer struct {
	ln   net.Listener
	mu   sync.Mutex
	cmds []stubCmd
}

func startStub(t *testing.T) *stubServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen stub: %v", err)
	}
	s := &stubServer{ln: ln}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(c)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

func (s *stubServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		cmd, err := protocol.Decode(rd)
		if err != nil {
			return
		}
		if cmd.Kind != protocol.KindArray || len(cmd.Elems) == 0 {
			return
		}
		rec := stubCmd{name: string(cmd.Elems[0].Bulk)}
		for _, a := range cmd.Elems[1:] {
			rec.args = append(rec.args, string(a.Bulk))
		}
		s.mu.Lock()
		s.cmds = append(s.cmds, rec)
		s.mu.Unlock()
		_, _ = c.Write([]byte("+OK\r\n"))
	}
}

func (s *stubServer) received() []stubCmd {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]stubCmd(nil), s.cmds...)
}

func (s *stubServer) addr() string { return s.ln.Addr().String() }

func TestRegistry_GetMasterAddrDefault(t *testing.T) {
	reg := NewRegistry([]NodeSpec{{Name: "mymaster", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}}}, time.Second)
	h, p, ok := reg.GetMasterAddr("mymaster")
	if !ok || h != "127.0.0.1" || p != 6380 {
		t.Fatalf("got %s:%d ok=%v", h, p, ok)
	}
	if _, _, ok := reg.GetMasterAddr("nope"); ok {
		t.Fatal("expect unknown master not ok")
	}
}

func TestRegistry_FailoverPicksHealthy(t *testing.T) {
	oldMaster := startStub(t)
	target := startStub(t)
	reg := NewRegistry([]NodeSpec{{Name: "mymaster", MasterAddr: oldMaster.addr(), Slaves: []string{target.addr()}}}, time.Second)
	if err := reg.Failover("mymaster"); err != nil {
		t.Fatalf("failover: %v", err)
	}
	h, p, _ := reg.GetMasterAddr("mymaster")
	th, tp, _ := net.SplitHostPort(target.addr())
	_ = h
	if pStr := strconv.Itoa(p); pStr != tp || h != th {
		t.Fatalf("current not flipped: got %s:%d want %s", h, p, target.addr())
	}
	got := target.received()
	if len(got) != 1 || got[0].name != "REPLICAOF" || len(got[0].args) != 2 {
		t.Fatalf("target cmds: %+v", got)
	}
	gotOld := oldMaster.received()
	if len(gotOld) != 1 || gotOld[0].name != "REPLICAOF" {
		t.Fatalf("old master cmds: %+v", gotOld)
	}
}

func TestRegistry_FailoverAllDownAborts(t *testing.T) {
	oldMaster := startStub(t)
	target := startStub(t)
	reg := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: oldMaster.addr(), Slaves: []string{target.addr()}}}, time.Second)
	reg.SetDown(target.addr(), true)
	if err := reg.Failover("m"); err != ErrNoHealthySlave {
		t.Fatalf("got %v", err)
	}
	_, p, _ := reg.GetMasterAddr("m")
	_, wantPort, _ := net.SplitHostPort(oldMaster.addr())
	if strconv.Itoa(p) != wantPort {
		t.Fatalf("cache must not flip, got port %d", p)
	}
	if len(target.received()) != 0 || len(oldMaster.received()) != 0 {
		t.Fatal("no REPLICAOF must be sent on abort")
	}
}

func TestRegistry_FailoverUnknownMaster(t *testing.T) {
	reg := NewRegistry(nil, time.Second)
	if err := reg.Failover("nope"); err != ErrUnknownMaster {
		t.Fatalf("got %v", err)
	}
}

func TestRegistry_ProbeMarksDown(t *testing.T) {
	up := startStub(t)
	reg := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: up.addr(), Slaves: []string{"127.0.0.1:1"}}}, 200*time.Millisecond)
	reg.ProbeOnce()
	if reg.IsDown("127.0.0.1:1") != true {
		t.Fatal("closed addr must be down")
	}
	if reg.IsDown(up.addr()) {
		t.Fatal("reachable stub must not be down")
	}
}
