package sentinel

// Quorum 返回 name 的 ODOWN 判定阈值；未知 name 或未设置（<=0，走
// config.Specs() 之外的直接构造）时返回 1，保持最小版旧展示一致。
func (r *Registry) Quorum(name string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, found := r.masters[name]
	if !found || st.spec.Quorum <= 0 {
		return 1
	}
	return st.spec.Quorum
}

// IsObjectivelyDown 按 quorum 判定 name 是否客观下线：自己 SDOWN 算一票，
// 再加 peerDowns（调用方传入问询到的对端 down 票数）。未知 name 返回 false。
func (r *Registry) IsObjectivelyDown(name string, peerDowns int) bool {
	r.mu.RLock()
	st, found := r.masters[name]
	if !found {
		r.mu.RUnlock()
		return false
	}
	q := st.spec.Quorum
	if q <= 0 {
		q = 1
	}
	sdown := st.sdownCount[st.current] >= 1
	r.mu.RUnlock()
	votes := peerDowns
	if sdown {
		votes++
	}
	return votes >= q
}
