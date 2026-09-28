// Package sentinel 是最小发现版哨兵（M7 #42）：静态 masters 注册表 +
// 周期探活 + 手动 FAILOVER。不做哨兵间 gossip/选举。
package sentinel

// NodeSpec 是单个被监控 master 的静态描述，由 config.Specs() 展开校验后传入。
// Quorum 为 ODOWN 判定阈值（含自己一票）。
type NodeSpec struct {
	Name       string
	MasterAddr string
	Slaves     []string
	Quorum     int
}
