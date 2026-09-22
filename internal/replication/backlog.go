// Package replication 实现 Redrock 的主从复制：Backlog 环缓冲保存近期
// 写操作（raw KV 粒度，TTL 随 entry 字节原样保留），RDB 编解码做全量传输。
// 线上帧沿用 Redis 风格：+FULLRESYNC / +CONTINUE + ["OP",...] 数组。
package replication

import (
	"errors"
	"sync"
)

// ErrStaleOffset 表示请求的 offset 已滑出 backlog，只能全量同步。
var ErrStaleOffset = errors.New("replication: offset out of backlog range")

// Op 是一次存储变更：Del=true 表删除，否则表写入 Value（完整 entry 字节）。
type Op struct {
	Offset int64
	Del    bool
	Key    []byte
	Value  []byte
}

// Backlog 是定长环缓冲，按 Offset 单调递增追加。并发安全。
type Backlog struct {
	mu   sync.Mutex
	max  int
	ops  []Op
	base int64
	next int64
}

// NewBacklog 返回容量为 max 条的 Backlog；max<=0 时用 1024。
func NewBacklog(max int) *Backlog {
	if max <= 0 {
		max = 1024
	}
	return &Backlog{max: max, ops: make([]Op, 0, max)}
}

// Append 追加一条操作并分配 Offset（从 1 起）。
func (b *Backlog) Append(op Op) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	op.Offset = b.next
	op.Key = append([]byte(nil), op.Key...)
	op.Value = append([]byte(nil), op.Value...)
	b.ops = append(b.ops, op)
	if len(b.ops) > b.max {
		n := len(b.ops) - b.max
		b.ops = append([]Op(nil), b.ops[n:]...)
		b.base += int64(n)
	}
	return b.next
}

// Since 返回 Offset 之后（不含）的全部操作拷贝；offset 已滑出返回 ErrStaleOffset。
func (b *Backlog) Since(offset int64) ([]Op, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if offset < b.base {
		return nil, ErrStaleOffset
	}
	out := make([]Op, 0)
	for _, op := range b.ops {
		if op.Offset > offset {
			cp := op
			cp.Key = append([]byte(nil), op.Key...)
			cp.Value = append([]byte(nil), op.Value...)
			out = append(out, cp)
		}
	}
	return out, nil
}

// Latest 返回当前最大 Offset（无操作时为 0）。
func (b *Backlog) Latest() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.next
}
