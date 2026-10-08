package sentinel

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// startPeerStub 起一个只回固定 payload 的 stub 哨兵/数据节点：
// 接入后读掉请求并回 payload 关连接。
func startPeerStub(t *testing.T, payload string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 4096)
				_, _ = c.Read(buf)
				_, _ = c.Write([]byte(payload))
			}()
		}
	}()
	return ln.Addr().String()
}

func TestQueryPeer_Triple(t *testing.T) {
	addr := startPeerStub(t, "*3\r\n$1\r\n1\r\n$4\r\npeer\r\n$1\r\n7\r\n")
	down, leader, epoch, err := QueryPeer(addr, "127.0.0.1", 6380, 7, "me", "mymaster")
	if err != nil {
		t.Fatal(err)
	}
	if !down || leader != "peer" || epoch != 7 {
		t.Fatalf("got down=%v leader=%q epoch=%d", down, leader, epoch)
	}
}

func TestQueryPeer_DialFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	if _, _, _, err := QueryPeer(addr, "127.0.0.1", 6380, 1, "me", "mymaster"); err == nil {
		t.Fatal("expected dial error")
	}
}

func TestFetchSlaveInfos_Parses(t *testing.T) {
	info := "# Replication\r\nrole:master\r\nslave0:ip=127.0.0.1,port=6381,state=online,offset=1024\r\n"
	addr := startPeerStub(t, fmt.Sprintf("$%d\r\n%s\r\n", len(info), info))
	infos, err := FetchSlaveInfos(addr, []string{"127.0.0.1:6381", "127.0.0.1:6382"})
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 2 {
		t.Fatalf("got %v", infos)
	}
	if infos[0].Offset != 1024 || infos[0].State != "online" {
		t.Fatalf("got %+v", infos[0])
	}
	if infos[1].Priority != 100 || infos[1].State != "" {
		t.Fatalf("absent entry must keep defaults, got %+v", infos[1])
	}
}

func TestFetchSlaveInfos_SlaveSideOffset(t *testing.T) {
	slaveInfo := "role:slave\r\nmaster_host:127.0.0.1\r\nmaster_port:6380\r\nmaster_link_status:up\r\nslave_repl_offset:2048\r\n"
	slave := startPeerStub(t, fmt.Sprintf("$%d\r\n%s\r\n", len(slaveInfo), slaveInfo))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadMaster := ln.Addr().String()
	_ = ln.Close()
	infos, err := FetchSlaveInfos(deadMaster, []string{slave})
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("got %v", infos)
	}
	if infos[0].Offset != 2048 || infos[0].State != "online" {
		t.Fatalf("slave-side offset must win when master is down, got %+v", infos[0])
	}
}

func TestFetchRole_Master(t *testing.T) {
	info := "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"
	addr := startPeerStub(t, fmt.Sprintf("$%d\r\n%s\r\n", len(info), info))
	role, err := FetchRole(addr)
	if err != nil {
		t.Fatal(err)
	}
	if role != "master" {
		t.Fatalf("got %q", role)
	}
}

func TestFetchHello_Message(t *testing.T) {
	body := "127.0.0.1,27401,run1,5,mymaster,127.0.0.1,7391,5"
	payload := "*3\r\n$9\r\nsubscribe\r\n$18\r\n__sentinel__:hello\r\n:1\r\n" +
		fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$18\r\n__sentinel__:hello\r\n$%d\r\n%s\r\n", len(body), body)
	addr := startPeerStub(t, payload)
	h, err := FetchHello(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if h.RunID != "run1" || h.Port != "27401" || h.Epoch != 5 || h.MasterPort != 7391 {
		t.Fatalf("got %+v", h)
	}
}
