package sentinel

import (
	"bufio"
	"net"
	"strings"
	"time"
)

// ProbeOnce 对全部被监控地址做一次 DialTimeout+PING 探活并经 RecordProbe
// 更新 SDOWN 计数与 down 标记；主地址取有效主 current（failover 后跟随
// 新主，旧主不再被反复标 down）；downAfter<=0 时跳过（只报配置主，不探活）。
func (r *Registry) ProbeOnce() {
	if r.downAfter <= 0 {
		return
	}
	type target struct {
		master string
		addr   string
	}
	r.mu.RLock()
	var targets []target
	for name, st := range r.masters {
		cur := st.current
		if cur == "" {
			cur = st.spec.MasterAddr
		}
		targets = append(targets, target{name, cur})
		for _, s := range st.spec.Slaves {
			targets = append(targets, target{name, s})
		}
	}
	r.mu.RUnlock()
	timeout := r.downAfter / 2
	if timeout <= 0 {
		timeout = time.Second
	}
	for _, t := range targets {
		r.RecordProbe(t.addr, pingOK(t.addr, timeout))
	}
}

// StartProbeLoop 按 downAfter 周期探活，直到 stop 关闭；downAfter<=0
// 直接返回。
func (r *Registry) StartProbeLoop(stop <-chan struct{}) {
	if r.downAfter <= 0 {
		return
	}
	tk := time.NewTicker(r.downAfter)
	defer tk.Stop()
	for {
		select {
		case <-stop:
			return
		case <-tk.C:
			r.ProbeOnce()
		}
	}
}

func pingOK(addr string, timeout time.Duration) bool {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		return false
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return false
	}
	return strings.HasPrefix(line, "+")
}
