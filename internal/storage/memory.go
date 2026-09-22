package storage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
)

// ErrOOM 在内存超限且无可驱逐 key 时返回（message 照抄 Redis 文案）。
var ErrOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'")

// evictSampleN 是每次驱逐的采样数（对标 Redis 默认 5）。
const evictSampleN = 5

type lruEntry struct {
	at      uint32
	size    int64
	expires bool
}

func nowSec() uint32 {
	return uint32(time.Now().Unix())
}

// UsedBytes 返回估算已用字节（全部 key+value 长度之和）。
func (p *Pebble) UsedBytes() int64 {
	return p.usedBytes.Load()
}

// MaxBytes 返回内存上限，0 表不限。
func (p *Pebble) MaxBytes() int64 {
	return p.maxBytes.Load()
}

// SetMaxBytes 设置内存上限（0 表不限）；CONFIG SET maxmemory 用。
func (p *Pebble) SetMaxBytes(n int64) {
	if n < 0 {
		n = 0
	}
	p.maxBytes.Store(n)
}

// Policy 返回当前驱逐策略。
func (p *Pebble) Policy() string {
	p.lruMu.Lock()
	defer p.lruMu.Unlock()
	if p.policy == "" {
		return "allkeys-lru"
	}
	return p.policy
}

// SetPolicy 切换驱逐策略，仅接受 allkeys-lru / volatile-lru。
func (p *Pebble) SetPolicy(policy string) error {
	switch strings.ToLower(policy) {
	case "allkeys-lru", "volatile-lru":
		p.lruMu.Lock()
		p.policy = strings.ToLower(policy)
		p.lruMu.Unlock()
		return nil
	default:
		return fmt.Errorf("storage: unknown eviction policy %q", policy)
	}
}

// EvictedCount 返回累计驱逐 key 数。
func (p *Pebble) EvictedCount() int64 {
	return p.evicted.Load()
}

func (p *Pebble) trackSet(key, value []byte) {
	expires := false
	if e, err := datastruct.Decode(value); err == nil {
		expires = e.Expiry != 0
	}
	size := int64(len(key) + len(value))
	p.lruMu.Lock()
	old, ok := p.lru[string(key)]
	if ok {
		p.usedBytes.Add(size - old.size)
	} else {
		p.usedBytes.Add(size)
	}
	if p.lru == nil {
		p.lru = make(map[string]lruEntry)
	}
	p.lru[string(key)] = lruEntry{at: nowSec(), size: size, expires: expires}
	p.lruMu.Unlock()
}

func (p *Pebble) trackGet(key []byte) {
	p.lruMu.Lock()
	if e, ok := p.lru[string(key)]; ok {
		e.at = nowSec()
		p.lru[string(key)] = e
	}
	p.lruMu.Unlock()
}

func (p *Pebble) trackDelete(key []byte) {
	p.lruMu.Lock()
	if e, ok := p.lru[string(key)]; ok {
		p.usedBytes.Add(-e.size)
		delete(p.lru, string(key))
	}
	p.lruMu.Unlock()
}

// evictIfNeeded 在超限时循环驱逐；无可驱逐 key 返回 ErrOOM。
func (p *Pebble) evictIfNeeded(ctx context.Context) error {
	return p.evictForDelta(ctx, 0)
}

// deltaFor 计算写入 key+value 后的净增字节（覆盖则减旧值）。
func (p *Pebble) deltaFor(key string, size int64) int64 {
	p.lruMu.Lock()
	defer p.lruMu.Unlock()
	if e, ok := p.lru[key]; ok {
		return size - e.size
	}
	return size
}

// evictForDelta 在写入前预留 delta 字节：循环驱逐直到容下；
// 无可驱逐 key 返回 ErrOOM 且不写入（调用方须先调后写）。
func (p *Pebble) evictForDelta(ctx context.Context, delta int64) error {
	if max := p.maxBytes.Load(); max > 0 {
		for p.usedBytes.Load()+delta > max {
			if !p.evictOne(ctx) {
				return ErrOOM
			}
		}
	}
	return nil
}

// evictOne 采样驱逐一个最久未访问 key；volatile-lru 只看带过期时间的。
// 成功返回 true；无候选返回 false。删除走完整 Delete 路径
// （核算一致 + 发布到 hub，复本同步驱逐）。
func (p *Pebble) evictOne(ctx context.Context) bool {
	p.lruMu.Lock()
	policy := p.policy
	if policy == "" {
		policy = "allkeys-lru"
	}
	type candidate struct {
		key string
		at  uint32
	}
	cands := make([]candidate, 0, evictSampleN)
	for k, e := range p.lru {
		if policy == "volatile-lru" && !e.expires {
			continue
		}
		if len(cands) >= evictSampleN {
			break
		}
		cands = append(cands, candidate{key: k, at: e.at})
	}
	if len(cands) == 0 {
		p.lruMu.Unlock()
		return false
	}
	victim := cands[0]
	for _, c := range cands[1:] {
		if c.at < victim.at {
			victim = c
		}
	}
	p.lruMu.Unlock()
	if err := p.Delete(ctx, []byte(victim.key)); err != nil {
		return false
	}
	p.evicted.Add(1)
	return true
}
