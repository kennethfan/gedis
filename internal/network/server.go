package network

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/kennethfan/gedis/internal/protocol"
)

// Server TCP 服务端：accept 循环 + 每连接一个 goroutine，RESP 解码→Router 分发→编码回写。
type Server struct {
	router *Router
	stats  *Stats

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

func NewServer(router *Router) *Server {
	s := &Server{router: router, conns: make(map[net.Conn]struct{}), stats: NewStats()}
	router.AttachStats(s.stats)
	return s
}

// Stats 返回服务统计容器（INFO/SLOWLOG/metrics 共用）。
func (s *Server) Stats() *Stats {
	return s.stats
}

// Serve 在给定 listener 上 accept，listener 关闭或 Close 调用后返回。
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("network: server closed")
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return fmt.Errorf("network: accept: %w", err)
		}
		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()
		s.stats.incConn()
		s.wg.Add(1)
		go s.handle(conn)
	}
}

// Close 关闭 listener 与全部活跃连接并等待处理完成。
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	if s.ln != nil {
		_ = s.ln.Close()
	}
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) handle(conn net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.stats.decConn()
		_ = conn.Close()
	}()

	rd := bufio.NewReader(conn)
	ctx := ContextWithStats(ContextWithConn(context.Background(), conn), s.stats)
	for {
		cmd, err := protocol.Decode(rd)
		if err != nil {
			return
		}
		reply := s.router.Dispatch(ctx, cmd)
		if _, err := conn.Write(reply.Append(nil)); err != nil {
			return
		}
	}
}
