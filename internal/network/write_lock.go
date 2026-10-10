package network

import (
	"errors"
	"net"
	"sync"
)

// per-conn 写锁：serve 循环回复与 MONITOR 广播可能并发写同一连接，
// 无锁会交织 RESP 帧。锁表仿 close.go 的 conn-keyed 模式，连接清理时
// ForgetWriteLock 兜底防泄漏。

var (
	writeMu    sync.Mutex
	writeLocks = make(map[net.Conn]*sync.Mutex)
)

func writeLockFor(conn net.Conn) *sync.Mutex {
	writeMu.Lock()
	defer writeMu.Unlock()
	m, ok := writeLocks[conn]
	if !ok {
		m = &sync.Mutex{}
		writeLocks[conn] = m
	}
	return m
}

// LockedWrite 在 per-conn 锁下写完整回复帧（serve 循环与异步广播共用）。
func LockedWrite(conn net.Conn, b []byte) error {
	if conn == nil {
		return errors.New("network: nil conn")
	}
	m := writeLockFor(conn)
	m.Lock()
	defer m.Unlock()
	_, err := conn.Write(b)
	return err
}

// ForgetWriteLock 丢弃连接的写锁表项（连接关闭时兜底，防 map 泄漏）。
func ForgetWriteLock(conn net.Conn) {
	writeMu.Lock()
	defer writeMu.Unlock()
	delete(writeLocks, conn)
}
