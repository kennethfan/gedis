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

// voteRound 向 peers 并行拉取一轮 IS-MASTER-DOWN-BY-ADDR：返回 down 票数与
// 投给本哨兵的票数（含自票；自票经 HandleVote 落本地 epoch）。
func voteRound(reg *sentinel.Registry, name, masterHost string, masterPort int, epoch uint64, runID string, peers []string) (downs, grants int) {
	if granted, _ := reg.HandleVote(name, epoch, runID); granted {
		grants = 1
	}
	type res struct {
		down bool
		lead string
		ok   bool
	}
	ch := make(chan res, len(peers))
	for _, p := range peers {
		go func(addr string) {
			down, leader, _, err := sentinel.QueryPeer(addr, masterHost, masterPort, epoch, runID, name)
			if err != nil {
				ch <- res{}
				return
			}
			ch <- res{down: down, lead: leader, ok: true}
		}(p)
	}
	for range peers {
		r := <-ch
		if !r.ok {
			continue
		}
		if r.down {
			downs++
		}
		if r.lead == runID {
			grants++
		}
	}
	return downs, grants
}

// peersExceptSelf 去重对端地址并排除自身（归一化后比较）。
func peersExceptSelf(addrs []string, self string) []string {
	nself := sentinel.NormalizeAddr(self)
	seen := map[string]bool{nself: true}
	var out []string
	for _, a := range addrs {
		na := sentinel.NormalizeAddr(a)
		if !seen[na] {
			seen[na] = true
			out = append(out, na)
		}
	}
	return out
}

// helloStep 发布本哨兵 hello，并并行抓取各对端 hello 入表 + 尝试收养新主。
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
	peers := peersExceptSelf(reg.Peers.Addrs(), net.JoinHostPort(selfHost, selfPort))
	type fetched struct {
		h  sentinel.Hello
		ok bool
	}
	ch := make(chan fetched, len(peers))
	for _, p := range peers {
		go func(addr string) {
			h, err := sentinel.FetchHello(addr, 3*time.Second)
			if err != nil {
				ch <- fetched{}
				return
			}
			ch <- fetched{h: h, ok: true}
		}(p)
	}
	for range peers {
		f := <-ch
		if !f.ok {
			continue
		}
		reg.Peers.Upsert(f.h)
		if f.h.Master != "" && f.h.MasterIP != "" {
			reg.AdoptMaster(f.h.Master, net.JoinHostPort(f.h.MasterIP, strconv.Itoa(f.h.MasterPort)), f.h.MasterEpoch, f.h.RunID)
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
	slaves, _ := reg.Slaves(name)
	if promoted, ok := reg.FindPromotedMaster(name, slaves); ok {
		reg.ObserveMaster(name, promoted)
		return
	}
	time.Sleep(time.Duration(100+rand.IntN(300)) * time.Millisecond)
	downs2, grants2 := voteRound(reg, name, curHost, curPort, cand, runID, peers)
	if !reg.IsObjectivelyDown(name, downs2) {
		return
	}
	if grants2*2 <= 1+len(peers) {
		return
	}
	pub.Publish("+try-failover", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	pub.Publish("+elected-leader", protocol.BulkOf(fmt.Sprintf("master %s %s %d", name, curHost, curPort)))
	var infos []sentinel.SlaveInfo
	if fetched, err := sentinel.FetchSlaveInfos(curAddr, slaves); err == nil {
		infos = fetched
	}
	if err := reg.AutoTick(name, true, downs2, infos, timeout, pub); err != nil {
		slog.Warn("sentinel autotick failed", "master", name, "err", err)
	}
}

// runSentinelLoops 起 hello/auto 两个后台循环，随 stop 一并退出。
// downAfter<=0 时只跑 hello 发现，auto 选举执行全程静默（不空转）。
func runSentinelLoops(reg *sentinel.Registry, selfAddr, runID string, downAfter, timeout time.Duration, pub *commands.PubSubRegistry, stop <-chan struct{}) {
	selfHost, selfPort, _ := net.SplitHostPort(selfAddr)
	helloT := time.NewTicker(sentinelHelloInterval)
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
	if downAfter <= 0 {
		return
	}
	autoT := time.NewTicker(downAfter)
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
