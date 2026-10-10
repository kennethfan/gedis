package commands

import (
	"context"
	"net"
	"sync"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// 失效推送（CLIENT TRACKING 的服务端出口）：存储层变更 hook → 跟踪表
// 反查观察者 → RESP3 直推 KindPush / RESP2 发布 __redis__:invalidate 通道。
// 硬约束 S7：本通路不读、不受 notify-keyspace-events 门控。

var (
	invMu        sync.RWMutex
	changeSink   func(ctx context.Context, op byte, rawKey []byte)
	invPublisher func(string, protocol.Value) int64
	invalidator  *invalidation
)

// SetChangeSink 装配失效推送实现（storage 变更回调入口转发目标；nil 清除）。
func SetChangeSink(fn func(ctx context.Context, op byte, rawKey []byte)) {
	invMu.Lock()
	changeSink = fn
	invMu.Unlock()
}

// InvalidateChange 是 storage change/evict hook 的标准入口：未装配零开销
// 返回；sink panic 在此 recover，不外溢打断存储写路径。
func InvalidateChange(ctx context.Context, op byte, rawKey []byte) {
	invMu.RLock()
	fn := changeSink
	invMu.RUnlock()
	if fn == nil {
		return
	}
	defer func() { _ = recover() }()
	fn(ctx, op, rawKey)
}

// SetInvalidatePublisher 装配 RESP2 通道发布器（main / 测试装配；nil 清除）。
func SetInvalidatePublisher(pub func(string, protocol.Value) int64) {
	invMu.Lock()
	invPublisher = pub
	invMu.Unlock()
}

// OnEvicted 是逐出的统一出口：evicted 键空间通知（受 notify 门控）+
// 失效推送 op 'e'（不受门控；ctx 无 conn → NOLOOP 无从排除，照发）。
func OnEvicted(rawKey string) {
	NotifyEvicted(rawKey)
	InvalidateChange(context.Background(), 'e', []byte(rawKey))
}

// invalidation 把存储变更推给跟踪该 key 的连接（per-conn ConnPipe 复用 T2）。
type invalidation struct {
	conns  *ConnRegistry
	tracks *TrackTable
	pipeMu sync.Mutex
	pipes  map[net.Conn]*ConnPipe
}

// InstallInvalidation 装配失效推送（RegisterServer 调）并接管 change sink。
func InstallInvalidation(conns *ConnRegistry, tracks *TrackTable) {
	if conns == nil || tracks == nil {
		return
	}
	inv := &invalidation{conns: conns, tracks: tracks, pipes: make(map[net.Conn]*ConnPipe)}
	invMu.Lock()
	invalidator = inv
	invMu.Unlock()
	SetChangeSink(inv.emit)
}

// emit 是变更 sink 本体：raw key 归一 → 跟踪表反查（无观察者零开销直通）
// → 按观察者 RESP 版本分发；NOLOOP 仅跳过写者自己。
func (inv *invalidation) emit(ctx context.Context, op byte, rawKey []byte) {
	key := userKeyFromRaw(string(rawKey))
	if key == "" {
		return
	}
	ids := inv.tracks.ConnsFor(key)
	ids = append(ids, inv.tracks.BcastFor(key)...)
	seen := make(map[int64]struct{}, len(ids))
	observers := make([]int64, 0, len(ids))
	for _, id := range ids {
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		observers = append(observers, id)
	}
	if len(observers) == 0 {
		return
	}
	writerID := int64(-1)
	writerNoLoop := false
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		writerID = inv.conns.IDOf(conn)
		writerNoLoop = inv.conns.TrackingOf(conn).NoLoop
	}
	push := protocol.Value{Kind: protocol.KindPush, Elems: []protocol.Value{
		protocol.BulkOf("invalidate"),
		protocol.ArrayOf(protocol.BulkOf(key)),
	}}
	frame := string(push.Append(nil))
	needResp2 := false
	for _, id := range observers {
		if id == writerID && writerNoLoop {
			continue
		}
		target, ok := inv.conns.ConnByID(id)
		if !ok {
			continue
		}
		if inv.conns.ProtoOf(target) >= 3 {
			inv.push(target, frame)
		} else {
			needResp2 = true
		}
	}
	if needResp2 {
		invMu.RLock()
		pub := invPublisher
		invMu.RUnlock()
		if pub != nil {
			pub("__redis__:invalidate", protocol.ArrayOf(protocol.BulkOf(key)))
		}
	}
}

// push 投递一帧到 conn 的出站管道（懒建）；写错或缓冲满摘除管道。
func (inv *invalidation) push(conn net.Conn, frame string) {
	inv.pipeMu.Lock()
	p, ok := inv.pipes[conn]
	if !ok {
		p = NewConnPipe(conn)
		inv.pipes[conn] = p
	}
	inv.pipeMu.Unlock()
	if !p.Enqueue(frame) {
		inv.dropPipe(conn)
		untrack(conn, inv.conns, inv.tracks)
	}
}

// dropPipe 摘除并关闭 conn 的推送管道（连接关闭 / 写失败兜底）。
func (inv *invalidation) dropPipe(conn net.Conn) {
	inv.pipeMu.Lock()
	if p, ok := inv.pipes[conn]; ok {
		p.shutdown()
		delete(inv.pipes, conn)
	}
	inv.pipeMu.Unlock()
}

// DropInvalidationPipe 摘除连接的失效推送管道（ConnRegistry.ConnClosed
// 连接关闭路径调用，防 map 泄漏）。
func DropInvalidationPipe(conn net.Conn) {
	invMu.RLock()
	inv := invalidator
	invMu.RUnlock()
	if inv != nil {
		inv.dropPipe(conn)
	}
}
