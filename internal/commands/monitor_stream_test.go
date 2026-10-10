package commands

import (
	"net"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func readMonitorFrame(t *testing.T, c net.Conn) string {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(2*time.Second)))
	buf := make([]byte, 512)
	n, err := c.Read(buf)
	require.NoError(t, err)
	return string(buf[:n])
}

func expectNoMoreFrames(t *testing.T, c net.Conn) {
	t.Helper()
	require.NoError(t, c.SetReadDeadline(time.Now().Add(150*time.Millisecond)))
	buf := make([]byte, 512)
	_, err := c.Read(buf)
	require.Error(t, err)
	ne, ok := err.(net.Error)
	require.True(t, ok && ne.Timeout(), "期望超时无帧，实得 %v", err)
}

// Given: connA 订阅 MONITOR，connB 执行 SET，connA 自身执行 GET
// When: 命令流广播
// Then: connA 收到 set 行与自身 get 行（简单字符串帧，含时间戳/DB/地址/小写命令/参数）
func Test_Monitor_StreamsCommands(t *testing.T) {
	defaultMonitors.reset()
	r, _, _, _, _ := openServerSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t, "OK", dispatchConn(r, c1, "MONITOR").S)

	cb1, cb2 := net.Pipe()
	defer cb1.Close()
	defer cb2.Close()
	require.Equal(t, "OK", dispatchConn(r, cb1, "SET", "k", "v").S)

	line := readMonitorFrame(t, c2)
	require.Regexp(t, `(?m)^\+\d+\.\d{6} \[0 .+\] "set" "k" "v"\r\n$`, line)

	require.Equal(t, protocol.KindBulkString, dispatchConn(r, c1, "GET", "k").Kind)
	line = readMonitorFrame(t, c2)
	require.Regexp(t, `^\+\d+\.\d{6} \[0 .+\] "get" "k"\r\n$`, line)
}

// Given: 已订阅的 conn 执行 QUIT/AUTH/CONFIG（admin 名单）
// When: 之后执行 SET
// Then: 流中只有 set 行；QUIT/AUTH/CONFIG 不出现；AUTH 密码参数不出现
func Test_Monitor_SkipsAndRedacts(t *testing.T) {
	defaultMonitors.reset()
	r, _, _, _, _ := openServerSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t, "OK", dispatchConn(r, c1, "MONITOR").S)
	dispatchConn(r, c1, "QUIT")
	dispatchConn(r, c1, "AUTH", "admin", "s3cr3t-pass")
	dispatchConn(r, c1, "CONFIG", "GET", "maxmemory")
	require.Equal(t, "OK", dispatchConn(r, c1, "SET", "k2", "v2").S)

	line := readMonitorFrame(t, c2)
	require.Contains(t, line, `"set" "k2" "v2"`)
	require.NotContains(t, line, "quit")
	require.NotContains(t, line, "auth")
	require.NotContains(t, line, "config")
	require.NotContains(t, line, "s3cr3t-pass")
	expectNoMoreFrames(t, c2)
}

// Given: 同一连接重复 MONITOR；订阅端随后写失败（对端关闭）
// When: 重复订阅 / 写错后继续广播
// Then: 重复 +OK 且登记不重复；写错自动摘除（Count 归零）
func Test_Monitor_IdempotentAndCleanup(t *testing.T) {
	defaultMonitors.reset()
	r, _, _, _, _ := openServerSetup(t)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	require.Equal(t, "OK", dispatchConn(r, c1, "MONITOR").S)
	require.Equal(t, "OK", dispatchConn(r, c1, "MONITOR").S)
	require.Equal(t, 1, defaultMonitors.Count())

	_ = c2.Close()
	require.Eventually(t, func() bool {
		dispatchConn(r, c1, "SET", "x", "1")
		return defaultMonitors.Count() == 0
	}, 2*time.Second, 20*time.Millisecond)
}
