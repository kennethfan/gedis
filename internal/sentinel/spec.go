// Package sentinel 是自动转移版哨兵（M7 #42 + 自动转移）：静态 masters
// 注册表 + 周期探活 + SDOWN/ODOWN 判定 + epoch 投票 + hello gossip
// 对端发现 + 手动/自动 FAILOVER。
package sentinel

// NodeSpec 是单个被监控 master 的静态描述，由 config.Specs() 展开校验后传入。
// Quorum 为 ODOWN 判定阈值（含自己一票）。
type NodeSpec struct {
	Name       string
	MasterAddr string
	Slaves     []string
	Quorum     int
}
