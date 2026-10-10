package commands

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/kennethfan/gedis/internal/network"
)

// ConnPipe 是 per-conn 串行化出站管道：Enqueue 只投递到有界缓冲
// （cap 1024，满即丢弃并关闭，绝不阻塞调用方），单 writer goroutine
// 经 network.LockedWrite 落盘，写错自动摘除。MONITOR 与后续 T5 失效
// 推送共用。
type ConnPipe struct {
	conn    net.Conn
	ch      chan string
	mu      sync.Mutex
	closed  bool
	dropped atomic.Int64
}

const connPipeBuf = 1024

func NewConnPipe(conn net.Conn) *ConnPipe {
	p := &ConnPipe{conn: conn, ch: make(chan string, connPipeBuf)}
	go p.writeLoop()
	return p
}

func (p *ConnPipe) writeLoop() {
	for frame := range p.ch {
		if err := network.LockedWrite(p.conn, []byte(frame)); err != nil {
			p.shutdown()
			return
		}
	}
}

// Enqueue 投递一帧完整报文；缓冲满/已关闭返回 false 且自动关闭管道（幂等）。
func (p *ConnPipe) Enqueue(frame string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	select {
	case p.ch <- frame:
		return true
	default:
		p.dropped.Add(1)
		p.closed = true
		close(p.ch)
		return false
	}
}

func (p *ConnPipe) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.ch)
}

func (p *ConnPipe) Dropped() int64 {
	return p.dropped.Load()
}
