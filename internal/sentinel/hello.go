package sentinel

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

// Hello 是哨兵间 gossip 的 _hello_ 载荷：本哨兵地址+runid+epoch，
// 以及本哨兵视角下的被监控 master（名/主地址/master-epoch）。
// 线上格式为逗号分隔 8 段。
type Hello struct {
	IP, Port, RunID string
	Epoch           uint64
	Master, MasterIP string
	MasterPort      int
	MasterEpoch     uint64
}

// Encode 把 Hello 压成逗号分隔 8 段。
func (h Hello) Encode() string {
	return fmt.Sprintf("%s,%s,%s,%d,%s,%s,%d,%d", h.IP, h.Port, h.RunID, h.Epoch, h.Master, h.MasterIP, h.MasterPort, h.MasterEpoch)
}

// ParseHello 解析 Encode 产出的 8 段字符串；段数/数字非法时报错。
func ParseHello(s string) (Hello, error) {
	p := strings.Split(s, ",")
	if len(p) != 8 {
		return Hello{}, fmt.Errorf("bad hello %q", s)
	}
	ep, err := strconv.ParseUint(p[3], 10, 64)
	if err != nil {
		return Hello{}, err
	}
	mp, err := strconv.Atoi(p[6])
	if err != nil {
		return Hello{}, err
	}
	mep, err := strconv.ParseUint(p[7], 10, 64)
	if err != nil {
		return Hello{}, err
	}
	return Hello{IP: p[0], Port: p[1], RunID: p[2], Epoch: ep, Master: p[4], MasterIP: p[5], MasterPort: mp, MasterEpoch: mep}, nil
}

// PeerTable 是已知对端哨兵表：key=RunID，Upsert 仅当 epoch>=旧值覆盖。
// 并发安全。
type PeerTable struct {
	mu    sync.Mutex
	peers map[string]Hello
}

// NewPeerTable 构造空对端表。
func NewPeerTable() *PeerTable {
	return &PeerTable{peers: make(map[string]Hello)}
}

// Upsert 登记对端 hello；同 RunID 仅当新 epoch>=旧 epoch 才覆盖。
func (p *PeerTable) Upsert(h Hello) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.peers[h.RunID]; ok && h.Epoch < old.Epoch {
		return
	}
	p.peers[h.RunID] = h
}

// Addrs 返回全部已知对端的 IP:Port；IP/Port 缺失者不列入。
func (p *PeerTable) Addrs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []string
	for _, h := range p.peers {
		if h.IP == "" || h.Port == "" {
			continue
		}
		out = append(out, h.IP+":"+h.Port)
	}
	return out
}

// SeedPeers 把种子哨兵地址记为占位对端（RunID 暂用 addr 本身）；
// gossip 到真实 hello 后同地址会有双条目，调用方去重。非法地址忽略。
func (p *PeerTable) SeedPeers(addrs []string) {
	for _, a := range addrs {
		h, port, err := net.SplitHostPort(a)
		if err != nil || h == "" || port == "" {
			continue
		}
		p.Upsert(Hello{IP: h, Port: port, RunID: a})
	}
}

// NewRunID 生成本哨兵的 16 字节 crypto/rand hex 运行标识。
func NewRunID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("sentinel: NewRunID rand: %v", err))
	}
	return hex.EncodeToString(b[:])
}
