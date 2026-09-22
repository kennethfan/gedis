package commands

import (
	"context"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
)

// expireSweepInterval 是后台清扫周期（对标 Redis hz 10）。
const expireSweepInterval = 100 * time.Millisecond

// Expirer 定期扫描删除过期 key（主动删除）；读路径的被动删除在 lookupRaw。
// 删除走 store，经 hub 复制到复本。stats 为 nil 时不计数。
type Expirer struct {
	kv       KV
	stats    *network.Stats
	interval time.Duration

	mu      sync.Mutex
	stop    chan struct{}
	running bool
}

// NewExpirer 返回绑定 store 的清扫器，Start 前不工作。
func NewExpirer(kv KV, stats *network.Stats) *Expirer {
	return &Expirer{kv: kv, stats: stats, interval: expireSweepInterval}
}

// SweepOnce 做一次全量扫描，返回删除的过期 key 数。
func (e *Expirer) SweepOnce() (int, error) {
	ctx := context.Background()
	_, raws, err := allUserKeys(ctx, e.kv)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixNano()
	var n int
	for _, k := range raws {
		v, err := e.kv.Get(ctx, k)
		if err != nil {
			if isNotFound(err) {
				continue
			}
			return n, err
		}
		entry, err := datastruct.Decode(v)
		if err != nil {
			return n, err
		}
		if entry.Expiry == 0 || now < entry.Expiry {
			continue
		}
		if err := e.kv.Delete(ctx, k); err != nil {
			return n, err
		}
		n++
		e.stats.IncExpired()
	}
	return n, nil
}

// Start 启动后台清扫；重复调用先停旧循环。
func (e *Expirer) Start() {
	e.Stop()
	e.mu.Lock()
	e.stop = make(chan struct{})
	e.running = true
	stop := e.stop
	e.mu.Unlock()
	go e.loop(stop)
}

// Stop 停止后台清扫；重复调用安全。
func (e *Expirer) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.running {
		return
	}
	e.running = false
	close(e.stop)
}

func (e *Expirer) loop(stop <-chan struct{}) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			_, _ = e.SweepOnce()
		}
	}
}
