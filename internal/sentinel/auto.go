package sentinel

import (
	"fmt"
	"net"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

// Publisher 是服务端事件发布口；*PubSubRegistry 的 Publish 方法
// （返回送达订阅者数 int64）直接满足本接口，T8 接线时透传。
type Publisher interface {
	Publish(channel string, msg protocol.Value) int64
}

// InCooldown 报告 name 是否处于 failover 冷却期内：timeout<=0 永不
// 冷却；从未 failover 过（lastFailover 零值）不冷却；否则距上次
// failover 不足 timeout 即冷却。
func (r *Registry) InCooldown(name string, timeout time.Duration) bool {
	if timeout <= 0 {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, found := r.masters[name]
	if !found || st.lastFailover.IsZero() {
		return false
	}
	return time.Since(st.lastFailover) < timeout
}

// AutoTick 是自动转移单步：非 leader 直接 nil；ODOWN 不成立直接
// nil；冷却中直接 nil；选从失败（无健康从）返回 ErrNoHealthySlave
// 且不翻转当前主（abort）；成功则 failoverTo 并经 pub 发射
// +switch-master（pub 为 nil 时跳过发射）。
func (r *Registry) AutoTick(name string, isLeader bool, infos []SlaveInfo, timeout time.Duration, pub Publisher) error {
	if !isLeader {
		return nil
	}
	if !r.IsObjectivelyDown(name, 0) {
		return nil
	}
	if r.InCooldown(name, timeout) {
		return nil
	}
	target, err := r.SelectSlave(name, infos)
	if err != nil {
		return err
	}
	oldHost, oldPort, _ := r.GetMasterAddr(name)
	if err := r.failoverTo(name, target); err != nil {
		return err
	}
	if pub != nil {
		newHost, newPort, _ := r.GetMasterAddr(name)
		pub.Publish("+switch-master", protocol.BulkOf(
			fmt.Sprintf("%s %s %d %s %d", name, oldHost, oldPort, newHost, newPort)))
	}
	return nil
}

// failoverTo 是切换内核（原 Failover 后半段）：提升 target 为主，
// 旧主降为其从，翻转当前主缓存并记 lastFailover。旧主降级失败不
// 回滚（缓存已翻，返回 nil）。
func (r *Registry) failoverTo(name, target string) error {
	r.mu.RLock()
	st, found := r.masters[name]
	r.mu.RUnlock()
	if !found {
		return ErrUnknownMaster
	}
	st.failMu.Lock()
	defer st.failMu.Unlock()

	r.mu.RLock()
	old := st.current
	r.mu.RUnlock()

	if err := sendReplicaof(target, "NO", "ONE"); err != nil {
		return fmt.Errorf("promote %s: %w", target, err)
	}
	th, tp, _ := net.SplitHostPort(target)
	if err := sendReplicaof(old, th, tp); err != nil {
		r.mu.Lock()
		st.current = target
		st.lastFailover = time.Now()
		r.mu.Unlock()
		return nil
	}
	r.mu.Lock()
	st.current = target
	st.lastFailover = time.Now()
	r.mu.Unlock()
	return nil
}
