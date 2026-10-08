package network

import (
	"net"
	"sync"
)

// QUIT 的"回完即断"机制：handler 在 Dispatch 内只能看到 ctx 里的连接，
// 够不到 Server。RequestClose 标记连接，serve 循环写完回复后检查标记再退出。
// 标记一次性（CloseRequested 读即删），连接清理时 ForgetClose 兜底。

var (
	closeMu    sync.Mutex
	closeAfter = make(map[net.Conn]bool)
)

// RequestClose 标记 conn 在当前回复写完后关闭（QUIT 用）。
func RequestClose(conn net.Conn) {
	if conn == nil {
		return
	}
	closeMu.Lock()
	defer closeMu.Unlock()
	closeAfter[conn] = true
}

// CloseRequested 取并消费关闭标记。
func CloseRequested(conn net.Conn) bool {
	closeMu.Lock()
	defer closeMu.Unlock()
	if !closeAfter[conn] {
		return false
	}
	delete(closeAfter, conn)
	return true
}

// ForgetClose 丢弃标记（连接已关闭时兜底，防 map 泄漏）。
func ForgetClose(conn net.Conn) {
	closeMu.Lock()
	defer closeMu.Unlock()
	delete(closeAfter, conn)
}
