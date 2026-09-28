package sentinel

// AdoptMaster 按对端 hello 收养新主：epoch 更大直接收养；epoch 相等
// 且本 epoch 投给了该 hello 的发送者（其赢得选举）也收养。收养翻转
// current。返回 current 是否变化。
func (r *Registry) AdoptMaster(name, addr string, epoch uint64, runID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, found := r.masters[name]
	if !found {
		return false
	}
	if epoch < st.currentEpoch {
		return false
	}
	if epoch == st.currentEpoch && (st.votedRunID != runID || addr == st.current) {
		return false
	}
	if addr == st.current {
		return false
	}
	st.current = addr
	return true
}

// ObserveMaster 无条件收养观测到的有效主（调用方已确认目标 role
// 为 master，本次切换中止，不记 lastFailover、不发事件）。返回是否变化。
func (r *Registry) ObserveMaster(name, addr string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, found := r.masters[name]
	if !found || addr == st.current {
		return false
	}
	st.current = addr
	return true
}
