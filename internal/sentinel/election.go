package sentinel

// hasMajority 是多数派纯判定：赞成票过半即当选。
func hasMajority(grants, total int) bool { return grants > total/2 }

// VotedFor 返回 name 当前 epoch 已投票对象；未知 name 返回 found=false。
func (r *Registry) VotedFor(name string) (runID string, epoch uint64, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st, ok := r.masters[name]
	if !ok {
		return "", 0, false
	}
	return st.votedRunID, st.currentEpoch, true
}
// HandleVote 处理一次拉票请求：候选 epoch 更大则跟进并投票；更小直接拒绝；
// 相等时只允许投已投对象（重复）或首投，投第二人拒绝。返回（是否赞成，本地 epoch）。
func (r *Registry) HandleVote(master string, candEpoch uint64, candRunID string) (bool, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, found := r.masters[master]
	if !found {
		return false, 0
	}
	if candEpoch < st.currentEpoch {
		return false, st.currentEpoch
	}
	if candEpoch > st.currentEpoch {
		st.currentEpoch = candEpoch
		st.votedEpoch = candEpoch
		st.votedRunID = candRunID
		return true, st.currentEpoch
	}
	if st.votedEpoch == candEpoch && st.votedRunID != "" && st.votedRunID != candRunID {
		return false, st.currentEpoch
	}
	st.votedEpoch = candEpoch
	st.votedRunID = candRunID
	return true, st.currentEpoch
}
