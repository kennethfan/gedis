package commands

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// MonitorRegistry 登记 MONITOR 订阅连接（conn → ConnPipe）。
type MonitorRegistry struct {
	mu    sync.Mutex
	pipes map[net.Conn]*ConnPipe
}

func NewMonitorRegistry() *MonitorRegistry {
	return &MonitorRegistry{pipes: make(map[net.Conn]*ConnPipe)}
}

var defaultMonitors = NewMonitorRegistry()

func (mr *MonitorRegistry) Monitor(conn net.Conn) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if _, ok := mr.pipes[conn]; !ok {
		mr.pipes[conn] = NewConnPipe(conn)
	}
}

func (mr *MonitorRegistry) Unmonitor(conn net.Conn) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	if p, ok := mr.pipes[conn]; ok {
		p.shutdown()
		delete(mr.pipes, conn)
	}
}

func (mr *MonitorRegistry) Count() int {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	return len(mr.pipes)
}

func (mr *MonitorRegistry) reset() {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for c, p := range mr.pipes {
		p.shutdown()
		delete(mr.pipes, c)
	}
}

// Broadcast 向全部订阅者投递一帧；写错/缓冲满的管道即时摘除。
func (mr *MonitorRegistry) Broadcast(frame string) {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	for conn, p := range mr.pipes {
		if !p.Enqueue(frame) {
			p.shutdown()
			delete(mr.pipes, conn)
		}
	}
}

// monitorSkip 显式跳过名单：自身控制命令 + admin 集合。
// 与真机 admin 全集的差异见 task report。
var monitorSkip = map[string]bool{
	"MONITOR": true, "QUIT": true, "RESET": true, "AUTH": true,
	"CONFIG": true, "ACL": true, "DEBUG": true, "SHUTDOWN": true,
	"SAVE": true, "BGSAVE": true, "BGREWRITEAOF": true,
	"LATENCY": true, "SLOWLOG": true, "SCRIPT": true,
	"CLIENT": true, "HELLO": true,
}

// monitorLine 生成 `<sec>.<06usec> [0 <addr>] "cmd" "arg"...`：
// db 恒 0（单库）；取不到 conn 的 ctx 记 local（真机 lua 位记 lua，近似）。
func monitorLine(conn net.Conn, name string, args []protocol.Value) string {
	addr := "local"
	if conn != nil && conn.RemoteAddr() != nil {
		addr = conn.RemoteAddr().String()
	}
	n := time.Now().UnixNano()
	var b strings.Builder
	fmt.Fprintf(&b, "+%d.%06d [0 %s] %q", n/1e9, (n%1e9)/1000, addr, strings.ToLower(name))
	for _, a := range args {
		s, _ := argString(a)
		if strings.EqualFold(name, "AUTH") {
			s = "***"
		}
		fmt.Fprintf(&b, " %q", s)
	}
	b.WriteString("\r\n")
	return b.String()
}

// InstallMonitorHook 把 MONITOR 广播挂到 Router Dispatch 观测点。
// 钩子内只做名单过滤 + 非阻塞投递，不碰 Dispatch 节奏。
func InstallMonitorHook(r *network.Router) {
	r.SetMonitorHook(func(ctx context.Context, name string, args []protocol.Value) {
		if monitorSkip[strings.ToUpper(name)] {
			return
		}
		conn, _ := network.ConnFromContext(ctx)
		defaultMonitors.Broadcast(monitorLine(conn, name, args))
	})
}

// startMonitor 订阅当前连接的命令流（MONITOR / CLIENT MONITOR 共用）。
func startMonitor(ctx context.Context) protocol.Value {
	conn, ok := network.ConnFromContext(ctx)
	if !ok || conn == nil {
		return errValueStr("ERR no connection to monitor on")
	}
	defaultMonitors.Monitor(conn)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}
