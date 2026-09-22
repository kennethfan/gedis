package replication

import (
	"sync"
)

const subBufSize = 256

// Hub 是主库的复制扇出：写操作先入 Backlog，再非阻塞广播给订阅者。
// 消费跟不上的订阅者被丢弃（关闭 channel），由其重连走 PSYNC。并发安全。
type Hub struct {
	mu      sync.Mutex
	backlog *Backlog
	subs    map[int64]chan []Op
	nextSub int64
}

// NewHub 返回 backlog 容量 maxOps 条的 Hub。
func NewHub(maxOps int) *Hub {
	return &Hub{backlog: NewBacklog(maxOps), subs: make(map[int64]chan []Op)}
}

// Backlog 返回底层的 backlog（PSYNC 断点续传用）。
func (h *Hub) Backlog() *Backlog {
	return h.backlog
}

// Publish 追加操作并广播，返回其 Offset。
func (h *Hub) Publish(op Op) int64 {
	off := h.backlog.Append(op)
	op.Offset = off
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		select {
		case ch <- []Op{op}:
		default:
			close(ch)
			delete(h.subs, id)
		}
	}
	return off
}

// Subscribe 返回订阅 id 与操作 channel；掉线（channel 关闭）后重调。
func (h *Hub) Subscribe() (int64, <-chan []Op) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextSub++
	ch := make(chan []Op, subBufSize)
	h.subs[h.nextSub] = ch
	return h.nextSub, ch
}

// SubscribeSince 原子地订阅并返回 offset 之后的操作：Publish 先写 backlog
// 再广播，订阅先于 Since 加锁，因此错过的操作必在两者之一，无间隙
// （重复投递可能，raw KV 覆盖/删除幂等可消化）。
// offset 已滑出 backlog 时返回 ErrStaleOffset（调用方走全量同步）。
func (h *Hub) SubscribeSince(offset int64) (int64, <-chan []Op, []Op) {
	h.mu.Lock()
	h.nextSub++
	ch := make(chan []Op, subBufSize)
	h.subs[h.nextSub] = ch
	id := h.nextSub
	h.mu.Unlock()
	missed, err := h.backlog.Since(offset)
	if err != nil {
		h.Unsubscribe(id)
		return 0, nil, nil
	}
	return id, ch, missed
}

// Unsubscribe 移除订阅并关闭 channel；重复调用安全。
func (h *Hub) Unsubscribe(id int64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if ch, ok := h.subs[id]; ok {
		close(ch)
		delete(h.subs, id)
	}
}

// SubCount 返回当前订阅数（INFO connected_slaves 用）。
func (h *Hub) SubCount() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return int64(len(h.subs))
}

// Close 关闭全部订阅（测试清理与关机用）；与 Unsubscribe 混用安全。
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, ch := range h.subs {
		close(ch)
		delete(h.subs, id)
	}
}
