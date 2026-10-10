package storage

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
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
	expAt   int64 // 过期时刻（UnixNano），0 表无过期
	freq    uint8 // LFU 简化计数：新 key=1，每次访问 +1，封顶 255，无衰减
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

// SetPolicy 切换驱逐策略，仅接受 Redis 八种合法策略；
// 未知策略报错且不改动已存策略。
func (p *Pebble) SetPolicy(policy string) error {
	pol := strings.ToLower(policy)
	switch pol {
	case "noeviction", "allkeys-random", "volatile-random", "volatile-ttl",
		"allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu":
	default:
		return fmt.Errorf("storage: unknown eviction policy %q", policy)
	}
	p.lruMu.Lock()
	p.policy = pol
	p.lruMu.Unlock()
	return nil
}

// EvictedCount 返回累计驱逐 key 数。
func (p *Pebble) EvictedCount() int64 {
	return p.evicted.Load()
}

// SetEvictHook 注册逐出回调（raw key）；nil 清除。
func (p *Pebble) SetEvictHook(fn func(string)) {
	p.lruMu.Lock()
	p.evictHook = fn
	p.lruMu.Unlock()
}

// ObjectStats 返回 rawKey 的空闲秒数与 LFU 计数；不刷新访问时钟
// （OBJECT 读取自身不得污染统计）。未跟踪的 key 返回 ok=false。
func (p *Pebble) ObjectStats(ctx context.Context, rawKey []byte) (idleSec uint64, freq uint8, ok bool) {
	p.lruMu.Lock()
	defer p.lruMu.Unlock()
	e, exists := p.lru[string(rawKey)]
	if !exists {
		return 0, 0, false
	}
	return uint64(nowSec() - e.at), e.freq, true
}

func (p *Pebble) trackSet(key, value []byte) {
	expires := false
	var expAt int64
	if e, err := datastruct.Decode(value); err == nil {
		expires = e.Expiry != 0
		if expires {
			expAt = e.Expiry
		}
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
	freq := uint8(1)
	if ok {
		freq = old.freq
		if freq < 255 {
			freq++
		}
	}
	p.lru[string(key)] = lruEntry{at: nowSec(), size: size, expires: expires, expAt: expAt, freq: freq}
	p.lruMu.Unlock()
}

func (p *Pebble) trackGet(key []byte) {
	p.lruMu.Lock()
	if e, ok := p.lru[string(key)]; ok {
		e.at = nowSec()
		if e.freq < 255 {
			e.freq++
		}
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

// evictOne 按当前策略驱逐一个 key：noeviction 直接放弃；volatile-ttl 全表
// 逐最早到期者；*-random 水塘抽样等概率选一；*-lru/-lfu 采样 evictSampleN
// 个候选取最久未访问 / 最低频次（同频比 at）。volatile-* 先滤掉无 TTL key。
// 成功返回 true；无候选返回 false。删除走完整 Delete 路径
// （核算一致 + 发布到 hub，复本同步驱逐）。
func (p *Pebble) evictOne(ctx context.Context) bool {
	p.lruMu.Lock()
	policy := p.policy
	if policy == "" {
		policy = "allkeys-lru"
	}
	if policy == "noeviction" {
		p.lruMu.Unlock()
		return false
	}
	volatileOnly := strings.HasPrefix(policy, "volatile-")
	type candidate struct {
		key  string
		at   uint32
		freq uint8
	}
	var victimKey string
	var found bool
	switch {
	case policy == "volatile-ttl":
		var minExp int64
		for k, e := range p.lru {
			if !e.expires {
				continue
			}
			if !found || e.expAt < minExp {
				victimKey, minExp, found = k, e.expAt, true
			}
		}
	case policy == "allkeys-random" || policy == "volatile-random":
		// 水塘抽样：候选整体等概率选一
		n := 0
		for k, e := range p.lru {
			if volatileOnly && !e.expires {
				continue
			}
			n++
			if rand.Intn(n) == 0 {
				victimKey, found = k, true
			}
		}
	default: // allkeys/volatile 的 -lru / -lfu
		cands := make([]candidate, 0, evictSampleN)
		for k, e := range p.lru {
			if volatileOnly && !e.expires {
				continue
			}
			cands = append(cands, candidate{key: k, at: e.at, freq: e.freq})
			if len(cands) >= evictSampleN {
				break
			}
		}
		if len(cands) == 0 {
			p.lruMu.Unlock()
			return false
		}
		v := cands[0]
		if strings.HasSuffix(policy, "-lfu") {
			for _, c := range cands[1:] {
				if c.freq < v.freq || (c.freq == v.freq && c.at < v.at) {
					v = c
				}
			}
		} else {
			for _, c := range cands[1:] {
				if c.at < v.at {
					v = c
				}
			}
		}
		victimKey, found = v.key, true
	}
	p.lruMu.Unlock()
	if !found {
		return false
	}
	if err := p.Delete(ctx, []byte(victimKey)); err != nil {
		return false
	}
	p.evicted.Add(1)
	p.lruMu.Lock()
	hook := p.evictHook
	p.lruMu.Unlock()
	if hook != nil {
		hook(victimKey)
	}
	return true
}
