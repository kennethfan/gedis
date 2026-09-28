package sentinel

// RecordProbe 记录一次探活结果：失败累计 SDOWN 计数并标 down，成功清零并清 down。
// 未知地址（非任何 master 主/从）直接忽略。
func (r *Registry) RecordProbe(addr string, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, st := range r.masters {
		if addr != st.spec.MasterAddr && !containsAddr(st.spec.Slaves, addr) {
			continue
		}
		if ok {
			st.sdownCount[addr] = 0
			st.down[addr] = false
			continue
		}
		st.sdownCount[addr]++
		if st.sdownCount[addr] >= 1 {
			st.down[addr] = true
		}
	}
}

// IsSubjectivelyDown 报告 addr 是否主观下线（SDOWN 阈值 1：单次失败即标）。
func (r *Registry) IsSubjectivelyDown(addr string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, st := range r.masters {
		if st.sdownCount[addr] >= 1 {
			return true
		}
	}
	return false
}
