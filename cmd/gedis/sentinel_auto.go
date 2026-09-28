package main

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	"github.com/kennethfan/gedis/internal/commands"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/sentinel"
)

const (
	sentinelHelloInterval = 2 * time.Second
	sentinelHelloChannel  = "__sentinel__:hello"
)

// masterEdge 记每 master 的边沿事件状态（单 goroutine 内读写）。
type masterEdge struct {
	sdown bool
	odown bool
}

// voteRound 向 peers 拉取一轮 IS-MASTER-DOWN-BY-ADDR：返回 down 票数与
// 投给本哨兵的票数（含自票；自票经 HandleVote 落本地 epoch）。
func voteRound(reg *sentinel.Registry, name, masterHost string, masterPort int, epoch uint64, runID string, peers []string) (downs, grants int) {
	if granted, _ := reg.HandleVote(name, epoch, runID); granted {
		grants = 1
	}
	for _, p := range peers {
		down, leader, _, err := sentinel.QueryPeer(p, masterHost, masterPort, epoch, runID, name)
		if err != nil {
			continue
		}
		if down {
			downs++
		}
		if leader == runID {
			grants++
		}
	}
	return downs, grants
}

// peersExceptSelf 去重对端地址并排除自身。
func peersExceptSelf(addrs []string, self string) []string {
	seen := map[string]bool{self: true}
	var out []string
	for _, a := range addrs {
		if !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// helloStep 发布本哨兵 hello，并抓取各对端 hello 入表 + 尝试收养新主。
func helloStep(reg *sentinel.Registry, selfHost, selfPort, runID string, pub *commands.PubSubRegistry) {
	for _, name := range reg.Names() {
		mh, mp, ok := reg.GetMasterAddr(name)
		if !ok {
			continue
		}
		_, epoch, _ := reg.VotedFor(name)
		h := sentinel.Hello{IP: selfHost, Port: selfPort, RunID: runID, Epoch: epoch,
			Master: name, MasterIP: mh, MasterPort: mp, MasterEpoch: epoch}
		pub.Publish(sentinelHelloChannel, protocol.BulkOf(h.Encode()))
	}
	for _, p := range peersExceptSelf(reg.Peers.Addrs(), net.JoinHostPort(selfHost, selfPort)) {
		h, err := sentinel.FetchHello(p, 3*time.Second)
		if err != nil {
			continue
		}
		reg.Peers.Upsert(h)
		if h.Master != "" && h.MasterIP != "" {
			reg.AdoptMaster(h.Master, net.JoinHostPort(h.MasterIP, strconv.Itoa(h.MasterPort)), h.MasterEpoch, h.RunID)
		}
	}
}

// autoStep 对单 master 走一轮“ODOWN→选举→执行”；边沿事件经 pub 发射。
func autoStep(reg *sentinel.Registry, name, runID string, peers []string, timeout time.Duration, pub *commands.PubSubRegistry, edge *masterEdge) {
	curHost, curPort, ok := reg.GetMasterAddr(name)
	if !ok {
		return
	}
	curAddr := net.JoinHostPort(curHost, strconv.Itoa(curPort))
	_, epoch, _ := reg.VotedFor(name)
	cand := epoch + 1
	downs, _ := voteRound(reg, name, curHost, curPort, cand, runID, peers)
	odown := reg.IsObjectivelyDown(name, downs)
	sdown := reg.IsSubjectivelyDown(curAddr)
	if sdown && !edge.sdown {
		pub.Publish("+sdown", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	}
	edge.sdown = sdown
	if odown && !edge.odown {
		pub.Publish("+odown", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	}
	edge.odown = odown
	if !odown || reg.InCooldown(name, timeout) {
		return
	}
	pub.Publish("+try-failover", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	time.Sleep(time.Duration(100+rand.IntN(300)) * time.Millisecond)
	downs2, grants2 := voteRound(reg, name, curHost, curPort, cand, runID, peers)
	if !reg.IsObjectivelyDown(name, downs2) {
		return
	}
	if grants2*2 <= 1+len(peers) {
		return
	}
	pub.Publish("+elected-leader", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	slaves, _ := reg.Slaves(name)
	var infos []sentinel.SlaveInfo
	if fetched, err := sentinel.FetchSlaveInfos(curAddr, slaves); err == nil {
		infos = fetched
	}
	if err := reg.AutoTick(name, true, downs2, infos, timeout, pub); err != nil {
		slog.Warn("sentinel autotick failed", "master", name, "err", err)
	}
}

// runSentinelLoops 起 hello/auto 两个后台循环，随 stop 一并退出。
func runSentinelLoops(reg *sentinel.Registry, selfAddr, runID string, downAfter, timeout time.Duration, pub *commands.PubSubRegistry, stop <-chan struct{}) {
	selfHost, selfPort, _ := net.SplitHostPort(selfAddr)
	autoInterval := downAfter
	if autoInterval <= 0 {
		autoInterval = time.Second
	}
	helloT := time.NewTicker(sentinelHelloInterval)
	autoT := time.NewTicker(autoInterval)
	go func() {
		defer helloT.Stop()
		for {
			select {
			case <-stop:
				return
			case <-helloT.C:
				helloStep(reg, selfHost, selfPort, runID, pub)
			}
		}
	}()
	go func() {
		defer autoT.Stop()
		edges := map[string]*masterEdge{}
		for {
			select {
			case <-stop:
				return
			case <-autoT.C:
				peers := peersExceptSelf(reg.Peers.Addrs(), selfAddr)
				for _, name := range reg.Names() {
					edge, found := edges[name]
					if !found {
						edge = &masterEdge{}
						edges[name] = edge
					}
					autoStep(reg, name, runID, peers, timeout, pub, edge)
				}
			}
		}
	}()
}
