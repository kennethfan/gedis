package network

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func startTestServer(t *testing.T) net.Addr {
	t.Helper()
	r := NewRouter()
	r.Register("PING", handlePing)
	s := NewServer(r)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(func() { _ = s.Close() })
	return l.Addr()
}

func ping(t *testing.T, addr net.Addr, payload string) string {
	t.Helper()
	conn, err := net.Dial("tcp", addr.String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = fmt.Fprintf(conn, "*1\r\n$4\r\n%s\r\n", payload)
	require.NoError(t, err)
	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	return line
}

// Given: 运行中的 Server
// When: 发 PING / 未知命令
// Then: 回 +PONG / -ERR
func Test_Server_when_PingAndUnknown(t *testing.T) {
	addr := startTestServer(t)
	require.Equal(t, "+PONG\r\n", ping(t, addr, "PING"))
	require.Contains(t, ping(t, addr, "NOPE"), "-ERR unknown command")
}

// Given: 运行中的 Server
// When: 20 个并发连接各发 PING
// Then: 全部收到 PONG
func Test_Server_when_Concurrent(t *testing.T) {
	addr := startTestServer(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.Equal(t, "+PONG\r\n", ping(t, addr, "PING"))
		}()
	}
	wg.Wait()
}
