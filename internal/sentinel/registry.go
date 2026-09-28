package sentinel

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

// ErrUnknownMaster 在 failover 未知 master 名时返回。
var ErrUnknownMaster = errors.New("sentinel: unknown master")

// ErrNoHealthySlave 在 failover 时无健康 slave 可提升时返回；此时
// 当前主缓存不翻转，也不向任何节点发送 REPLICAOF。
var ErrNoHealthySlave = errors.New("sentinel: no healthy slave")

const replicaofTimeout = 2 * time.Second

type masterState struct {
	spec         NodeSpec
	current      string
	down         map[string]bool
	sdownCount   map[string]int
	currentEpoch uint64
	votedEpoch   uint64
	votedRunID   string
	lastFailover time.Time
	failMu       sync.Mutex
}

// Registry 是静态 masters 注册表：master 名 → 主地址/slaves 列表/
// 当前主缓存/down 标记。并发安全；同 name 的 failover 串行。
type Registry struct {
	mu        sync.RWMutex
	masters   map[string]*masterState
	downAfter time.Duration
	// Peers 是 gossip 发现的对端哨兵表；NewRegistry 初始化为空表。
	Peers *PeerTable
}

// NewRegistry 由校验过的 specs 构造注册表；当前主初始为配置主。
func NewRegistry(specs []NodeSpec, downAfter time.Duration) *Registry {
	r := &Registry{masters: make(map[string]*masterState, len(specs)), downAfter: downAfter, Peers: NewPeerTable()}
	for _, s := range specs {
		r.masters[s.Name] = &masterState{spec: s, current: s.MasterAddr, down: make(map[string]bool), sdownCount: make(map[string]int)}
	}
	return r
}

// GetMasterAddr 返回 name 当前主的 host/port；未知 name 时 ok=false。
// 全 down 时仍返回最后缓存主（对齐真机行为，不返回空）。
func (r *Registry) GetMasterAddr(name string) (host string, port int, ok bool) {
	r.mu.RLock()
	st, found := r.masters[name]
	r.mu.RUnlock()
	if !found {
		return "", 0, false
	}
	r.mu.RLock()
	cur := st.current
	r.mu.RUnlock()
	h, p, err := net.SplitHostPort(cur)
	if err != nil {
		return "", 0, false
	}
	n, err := strconv.Atoi(p)
	if err != nil {
		return "", 0, false
	}
	return h, n, true
}

// Slaves 返回 name 的 slave 地址列表（静态，顺序同配置）。
func (r *Registry) Slaves(name string) ([]string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, found := r.masters[name]
	if !found {
		return nil, false
	}
	return append([]string(nil), st.spec.Slaves...), true
}

// MasterAddr 返回 name 的配置主地址。
func (r *Registry) MasterAddr(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, found := r.masters[name]
	if !found {
		return "", false
	}
	return st.spec.MasterAddr, true
}

// Names 返回全部被监控的 master 名。
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.masters))
	for n := range r.masters {
		out = append(out, n)
	}
	return out
}

// DownAfterMs 返回探活间隔毫秒（SENTINEL masters 展示用）。
func (r *Registry) DownAfterMs() int64 {
	return r.downAfter.Milliseconds()
}

// IsDown 报告 addr 是否被标 down；未知地址返回 false。
func (r *Registry) IsDown(addr string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, st := range r.masters {
		if st.down[addr] {
			return true
		}
	}
	return false
}

// SetDown 直设 addr 的 down 标记（探活调用；测试可直调模拟）。
func (r *Registry) SetDown(addr string, down bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, st := range r.masters {
		if addr == st.spec.MasterAddr || containsAddr(st.spec.Slaves, addr) {
			st.down[addr] = down
		}
	}
}

// Failover 对 name 执行一次手动切换：选首个非 down slave 并调
// failoverTo（提升+降旧主+翻缓存+记 lastFailover）。
// 无健康 slave 时 abort：不翻转当前主，不发送 REPLICAOF。
// 目标即当前主时短路 nil（幂等，不记 lastFailover）。
func (r *Registry) Failover(name string) error {
	r.mu.RLock()
	st, found := r.masters[name]
	r.mu.RUnlock()
	if !found {
		return ErrUnknownMaster
	}

	r.mu.RLock()
	slaves := append([]string(nil), st.spec.Slaves...)
	down := make(map[string]bool, len(st.down))
	for a, d := range st.down {
		down[a] = d
	}
	r.mu.RUnlock()

	target := ""
	for _, s := range slaves {
		if !down[s] {
			target = s
			break
		}
	}
	if target == "" {
		return ErrNoHealthySlave
	}
	if ch, cp, ok := r.GetMasterAddr(name); ok && net.JoinHostPort(ch, strconv.Itoa(cp)) == target {
		return nil
	}
	return r.failoverTo(name, target)
}

func sendReplicaof(addr string, args ...string) error {
	var err error
	for i := 0; i < 2; i++ {
		if err = dialAndSend(addr, args); err == nil {
			return nil
		}
	}
	return err
}

func dialAndSend(addr string, args []string) error {
	conn, err := net.DialTimeout("tcp", addr, replicaofTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(replicaofTimeout))
	elems := make([]protocol.Value, 0, len(args)+1)
	elems = append(elems, protocol.BulkOf("REPLICAOF"))
	for _, a := range args {
		elems = append(elems, protocol.BulkOf(a))
	}
	if _, err := conn.Write(protocol.ArrayOf(elems...).Append(nil)); err != nil {
		return err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "+") {
		return fmt.Errorf("unexpected reply %q", strings.TrimSpace(line))
	}
	return nil
}

func containsAddr(list []string, addr string) bool {
	for _, a := range list {
		if a == addr {
			return true
		}
	}
	return false
}
