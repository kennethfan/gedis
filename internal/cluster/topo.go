package cluster

import (
	"fmt"
	"net"
	"sort"
)

// Topology 是静态拓扑的运行时视图（见 #40 配置形状决议）。
// 叶子包：零内部依赖；ranges 展开由配置加载侧负责（实现阶段）。
type Topology struct {
	Enabled bool
	self    int
	nodes   []TopoNode
	owned   [NumSlots]bool
	owner   [NumSlots]int
}

// TopoNode 是静态拓扑中的一个服务节点（均为 master，无 gossip）。
type TopoNode struct {
	ID     string
	Addr   string
	Ranges [][2]int
}

// NodeSpec 是 Build 的输入：ID 为空时由 Addr 派生，Ranges 已展开。
type NodeSpec struct {
	ID     string
	Addr   string
	Ranges [][2]int
}

// NewTopology 构造静态拓扑；enabled=false 时 owned 全空（拒绝一切槽位）。
// ranges 为闭区间 [start, end] 列表，越界/倒置区间直接忽略。
func NewTopology(enabled bool, ranges [][2]int) *Topology {
	t := &Topology{Enabled: enabled}
	if !enabled {
		return t
	}
	for i := range t.owner {
		t.owner[i] = -1
	}
	t.nodes = []TopoNode{{Ranges: ranges}}
	for _, r := range ranges {
		for s := r[0]; s <= r[1] && s < NumSlots; s++ {
			if s >= 0 {
				t.owned[s] = true
				t.owner[s] = 0
			}
		}
	}
	return t
}

// FullTopology 构造持有全部槽位的单节点拓扑（#40：slots 缺席即全量）。
func FullTopology() *Topology {
	t := &Topology{Enabled: true}
	for s := range t.owned {
		t.owned[s] = true
		t.owner[s] = 0
	}
	t.nodes = []TopoNode{{Ranges: [][2]int{{0, NumSlots - 1}}}}
	return t
}

// Build 由配置 specs 构造拓扑：specs 为空即单节点全槽（self=serverAddr 全量）。
// selfAddr 必须命中某个节点 Addr（host 0.0.0.0/空时只比端口），否则 fail-fast。
func Build(selfAddr string, specs []NodeSpec) (*Topology, error) {
	if len(specs) == 0 {
		specs = []NodeSpec{{Addr: selfAddr, Ranges: [][2]int{{0, NumSlots - 1}}}}
	}
	t := &Topology{Enabled: true}
	for i := range t.owner {
		t.owner[i] = -1
	}
	selfHost, selfPort, err := splitAddr(selfAddr)
	if err != nil {
		return nil, fmt.Errorf("cluster: invalid server addr %q", selfAddr)
	}
	self := -1
	for _, s := range specs {
		if s.Addr == "" {
			return nil, fmt.Errorf("cluster: node missing addr")
		}
		h, p, err := splitAddr(s.Addr)
		if err != nil {
			return nil, fmt.Errorf("cluster: invalid node addr %q", s.Addr)
		}
		match := h == selfHost && p == selfPort
		if (selfHost == "" || selfHost == "0.0.0.0") && p == selfPort {
			match = true
		}
		id := s.ID
		if id == "" {
			id = DeriveID(s.Addr)
		}
		ranges := append([][2]int(nil), s.Ranges...)
		sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
		t.nodes = append(t.nodes, TopoNode{ID: id, Addr: s.Addr, Ranges: ranges})
		idx := len(t.nodes) - 1
		if match && self < 0 {
			self = idx
		}
		for _, r := range ranges {
			for slot := r[0]; slot <= r[1] && slot < NumSlots; slot++ {
				if slot < 0 {
					continue
				}
				if t.owner[slot] >= 0 {
					return nil, fmt.Errorf("cluster: slot %d claimed twice", slot)
				}
				t.owned[slot] = true
				t.owner[slot] = idx
			}
		}
	}
	if self < 0 {
		return nil, fmt.Errorf("cluster: no [[cluster.nodes]] entry matches server %q", selfAddr)
	}
	t.self = self
	return t, nil
}

func splitAddr(addr string) (string, string, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil || h == "" && p == "" {
		return "", "", fmt.Errorf("bad addr")
	}
	return h, p, nil
}

// Owns 报告本节点是否持有该槽位；静态拓扑下结果恒定。
func (t *Topology) Owns(slot int) bool {
	if t == nil || !t.Enabled || slot < 0 || slot >= NumSlots {
		return false
	}
	return t.owned[slot] && t.owner[slot] == t.self
}

// OwnerAddr 返回持有该槽位的节点 Addr；无归属返回 ""（调用方报 CLUSTERDOWN）。
func (t *Topology) OwnerAddr(slot int) string {
	if t == nil || !t.Enabled || slot < 0 || slot >= NumSlots {
		return ""
	}
	if idx := t.owner[slot]; idx >= 0 && idx < len(t.nodes) {
		return t.nodes[idx].Addr
	}
	return ""
}

// SelfID 返回本节点 ID；disabled/空拓扑返回 ""。
func (t *Topology) SelfID() string {
	if t == nil || !t.Enabled || len(t.nodes) == 0 {
		return ""
	}
	return t.nodes[t.self].ID
}

// SelfAddr 返回本节点 Addr。
func (t *Topology) SelfAddr() string {
	if t == nil || !t.Enabled || len(t.nodes) == 0 {
		return ""
	}
	return t.nodes[t.self].Addr
}

// Nodes 返回全部静态节点（master 顺序即配置顺序）。
func (t *Topology) Nodes() []TopoNode {
	if t == nil {
		return nil
	}
	return t.nodes
}

// AssignedCount 返回已分配槽位数（INFO slots_assigned/ok）。
func (t *Topology) AssignedCount() int {
	if t == nil || !t.Enabled {
		return 0
	}
	n := 0
	for _, o := range t.owner {
		if o >= 0 {
			n++
		}
	}
	return n
}
