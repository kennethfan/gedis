package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/kennethfan/gedis/internal/replication"
)

// Pebble 是基于 Pebble（纯 Go LSM 引擎）的 Store 实现。零值不可用，用 New 构造。
//
// 写入默认 NoSync（WAL 仍写，仅 fsync 策略宽松）；fsync=always 语义由
// Ticket #4 的持久化层通过 pebble.Sync 实现。
type Pebble struct {
	path string
	opts Options
	db   *pebble.DB

	mu            sync.Mutex
	flushInterval time.Duration
	flushDone     chan struct{}
	flushExited   chan struct{}

	lruMu     sync.Mutex
	lru       map[string]lruEntry
	usedBytes atomic.Int64
	maxBytes  atomic.Int64
	policy    string
	evicted   atomic.Int64
}

// New 返回绑定到 path 目录的 Pebble 存储（默认选项）。Open 之前不可读写。
func New(path string) *Pebble {
	return NewWithOptions(path, DefaultOptions())
}

// Open 打开（不存在则创建）path 下的 Pebble。AppendOnly=false 时关闭 WAL。
func (p *Pebble) Open() error {
	db, err := pebble.Open(p.path, &pebble.Options{DisableWAL: !p.opts.AppendOnly})
	if err != nil {
		return fmt.Errorf("storage: open pebble at %s: %w", p.path, err)
	}
	p.db = db
	if p.opts.Fsync == FsyncEverysec {
		p.startFlusher()
	}
	return nil
}

// startFlusher 启动定时刷盘：每 interval 把 memtable 落盘（everysec 语义）。
// interval<=0 时用 1 秒。Close 时停。
func (p *Pebble) startFlusher() {
	interval := p.flushInterval
	if interval <= 0 {
		interval = time.Second
	}
	p.flushDone = make(chan struct{})
	p.flushExited = make(chan struct{})
	go func() {
		defer close(p.flushExited)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-p.flushDone:
				return
			case <-ticker.C:
				p.mu.Lock()
				db := p.db
				p.mu.Unlock()
				if db != nil {
					_ = db.Flush()
				}
			}
		}
	}()
}

// Close 关闭 DB 并停掉刷盘（等在飞的刷盘结束后再关）。重复 Close 安全。
func (p *Pebble) Close() error {
	if p.flushDone != nil {
		close(p.flushDone)
		<-p.flushExited
		p.flushDone = nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.db == nil {
		return nil
	}
	if !p.opts.AppendOnly {
		if err := p.db.Flush(); err != nil {
			return fmt.Errorf("storage: flush on close: %w", err)
		}
	}
	err := p.db.Close()
	p.db = nil
	return err
}

// Get 读取 key，不存在返回 ErrNotFound。
func (p *Pebble) Get(ctx context.Context, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	v, closer, err := p.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("storage: get %q: %w", key, err)
	}
	defer func() { _ = closer.Close() }()
	out := make([]byte, len(v))
	copy(out, v)
	p.trackGet(key)
	return out, nil
}

// Set 写入 key。超限时先驱逐再写；无可驱逐 key 返回 ErrOOM 且不写入。
func (p *Pebble) Set(ctx context.Context, key, value []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.evictForDelta(ctx, p.deltaFor(string(key), int64(len(key)+len(value)))); err != nil {
		return err
	}
	if err := p.db.Set(key, value, p.writeOptions()); err != nil {
		return fmt.Errorf("storage: set %q: %w", key, err)
	}
	p.trackSet(key, value)
	if p.opts.Hub != nil {
		p.opts.Hub.Publish(replication.Op{Key: key, Value: value})
	}
	return nil
}

// Delete 删除 key，不存在也不报错。
func (p *Pebble) Delete(ctx context.Context, key []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := p.db.Delete(key, p.writeOptions()); err != nil {
		return fmt.Errorf("storage: delete %q: %w", key, err)
	}
	p.trackDelete(key)
	if p.opts.Hub != nil {
		p.opts.Hub.Publish(replication.Op{Del: true, Key: key})
	}
	return nil
}

// Scan 返回带 prefix 前缀的全部 key 拷贝（有序）。KEYS 命令用；阻塞式全量扫描。
func (p *Pebble) Scan(ctx context.Context, prefix []byte) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iter, err := p.db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, fmt.Errorf("storage: scan %q: %w", prefix, err)
	}
	defer func() { _ = iter.Close() }()
	var out [][]byte
	for iter.SeekGE(prefix); iter.Valid(); iter.Next() {
		k := iter.Key()
		if !bytes.HasPrefix(k, prefix) {
			break
		}
		cp := make([]byte, len(k))
		copy(cp, k)
		out = append(out, cp)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("storage: scan %q: %w", prefix, err)
	}
	return out, nil
}
