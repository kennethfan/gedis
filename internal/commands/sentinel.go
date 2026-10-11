package commands

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/sentinel"
)

// RegisterSentinel 在独立哨兵 Router 上注册 SENTINEL/INFO；PING 由
// DefaultRouter 自带，SUBSCRIBE 由 main.go 另调 RegisterPubSub 挂载。
// pub 为 nil 时 failover 成功不发射 +switch-master。
var sentinelMeta = []acl.Meta{
	{Name: "SENTINEL", Category: "admin", Keys: acl.KeySpec{First: -1}},
	{Name: "INFO", Category: "dangerous", ReadOnly: true, Keys: acl.KeySpec{First: -1}},
}

func RegisterSentinel(r *network.Router, reg *sentinel.Registry, selfAddr string, pub *PubSubRegistry, runID string) {
	for _, m := range sentinelMeta {
		acl.RegisterMeta(m)
	}
	h := &sentinelHandler{reg: reg, selfAddr: selfAddr, pub: pub, runID: runID}
	r.Register("SENTINEL", h.sentinel)
	r.Register("INFO", h.info)
}

type sentinelHandler struct {
	reg      *sentinel.Registry
	selfAddr string
	pub      *PubSubRegistry
	runID    string
	// cfg/dbg 存 SENTINEL CONFIG/DEBUG 的内存态（静态拓扑：只存不联动，
	// 文档声明偏差）。懒初始化以兼容测试直构 handler。
	mu  sync.Mutex
	cfg map[string]string
	dbg map[string]string
}

func (h *sentinelHandler) sentinel(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return errValueStr("ERR wrong number of arguments for 'sentinel' command")
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	rest := args[1:]
	switch strings.ToUpper(sub) {
	case "MASTERS":
		if len(rest) != 0 {
			return sentinelArgErr(sub)
		}
		return h.masters()
	case "MASTER":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.master(rest)
	case "SLAVES":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.slaves(rest)
	case "REPLICAS":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.slaves(rest)
	case "MYID":
		if len(rest) != 0 {
			return sentinelArgErr(sub)
		}
		return protocol.BulkOf(h.runID)
	case "CKQUORUM":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.ckquorum(rest)
	case "SENTINELS":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.sentinels(rest)
	case "GET-MASTER-ADDR-BY-NAME":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.getMasterAddr(rest)
	case "FAILOVER":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.failover(rest)
	case "IS-MASTER-DOWN-BY-ADDR":
		if len(rest) != 4 {
			return sentinelArgErr(sub)
		}
		return h.isMasterDownByAddr(rest)
	case "RESET", "REMOVE", "SET", "MONITOR":
		return errValueStr("ERR Static sentinel topology does not support SENTINEL " + strings.ToUpper(sub))
	case "HELP":
		if len(rest) != 0 {
			return sentinelArgErr(sub)
		}
		return sentinelHelp()
	case "INFO-CACHE":
		if len(rest) != 1 {
			return sentinelArgErr(sub)
		}
		return h.infoCache(rest)
	case "PENDING-SCRIPTS":
		if len(rest) != 0 {
			return sentinelArgErr(sub)
		}
		return protocol.ArrayOf()
	case "SIMULATE-FAILURE":
		return h.simulateFailure(sub, rest)
	case "CONFIG":
		return h.sentinelConfig(sub, rest)
	case "DEBUG":
		return h.sentinelDebug(sub, rest)
	case "FLUSHCONFIG":
		if len(rest) != 0 {
			return sentinelArgErr(sub)
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try SENTINEL HELP.", string(args[0].Bulk)))
	}
}

func sentinelArgErr(sub string) protocol.Value {
	return errValueStr(fmt.Sprintf("ERR wrong number of arguments for 'sentinel|%s' command", strings.ToLower(sub)))
}

// infoCache INFO-CACHE：已知 master 回 [INFO bulk]，未知回空数组（7.2.6 探针）。
func (h *sentinelHandler) infoCache(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if _, found := h.reg.MasterAddr(name); !found {
		return protocol.ArrayOf()
	}
	return protocol.ArrayOf(protocol.BulkOf(h.sentinelInfoBulk()))
}

func (h *sentinelHandler) sentinelInfoBulk() string {
	return fmt.Sprintf("# Sentinel\r\nsentinel_masters:%d\r\n", len(h.reg.Names()))
}

// simulateFailure SIMULATE-FAILURE：无参报 arity；HELP 回小写 flag 表；
// 带 flag 一律诚实错——绝不真 crash（显性偏差，文档声明）。
func (h *sentinelHandler) simulateFailure(sub string, rest []protocol.Value) protocol.Value {
	if len(rest) == 0 {
		return sentinelArgErr(sub)
	}
	if len(rest) == 1 {
		if s, ok := argString(rest[0]); ok && strings.EqualFold(s, "HELP") {
			return protocol.ArrayOf(
				protocol.BulkOf("crash-after-election"),
				protocol.BulkOf("crash-after-promotion"),
			)
		}
	}
	return errValueStr("ERR SENTINEL SIMULATE-FAILURE crash flags not supported on this engine (refuses to crash)")
}

// sentinelConfig CONFIG：内存 map 存取。GET 未知参回空 entry（非报错）；
// SET 未知参回 Invalid argument；裸 CONFIG 报 arity（7.2.6 探针）。
func (h *sentinelHandler) sentinelConfig(sub string, rest []protocol.Value) protocol.Value {
	if len(rest) == 0 {
		return sentinelArgErr(sub)
	}
	op, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.cfg == nil {
		h.cfg = map[string]string{}
	}
	switch strings.ToUpper(op) {
	case "GET":
		if len(rest) < 2 {
			return sentinelArgErr(sub)
		}
		out := []protocol.Value{}
		for _, p := range rest[1:] {
			name, ok := argString(p)
			if !ok {
				return errValueStr("ERR syntax error")
			}
			if !sentinelConfigKnown(name) {
				continue
			}
			out = append(out, protocol.ArrayOf(protocol.BulkOf(name), protocol.BulkOf(h.cfg[name])))
		}
		return protocol.ArrayOf(out...)
	case "SET":
		if len(rest) < 3 || len(rest)%2 == 0 {
			return sentinelArgErr(sub)
		}
		for i := 1; i < len(rest); i += 2 {
			name, ok := argString(rest[i])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			if !sentinelConfigKnown(name) {
				return errValueStr(fmt.Sprintf("ERR Invalid argument '%s' to SENTINEL CONFIG SET", string(rest[i].Bulk)))
			}
			val, ok := argString(rest[i+1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			h.cfg[name] = val
		}
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	default:
		return sentinelArgErr(sub)
	}
}

// sentinelConfigKnown CONFIG SET 可设参数白名单（探针见 announce-ip；
// 余者按哨兵常用参数收，未知一律 Invalid argument）。
func sentinelConfigKnown(name string) bool {
	switch strings.ToLower(name) {
	case "announce-ip", "announce-port", "announce-hostnames",
		"resolve-hostnames", "deny-scripts-reconfig":
		return true
	}
	return false
}

// sentinelDebugDefaults DEBUG 默认表（7.2.6 探针逐字；静态值，不联动定时器）。
var sentinelDebugDefaults = [][2]string{
	{"INFO-PERIOD", "10000"},
	{"PING-PERIOD", "1000"},
	{"ASK-PERIOD", "1000"},
	{"PUBLISH-PERIOD", "2000"},
	{"DEFAULT-DOWN-AFTER", "30000"},
	{"DEFAULT-FAILOVER-TIMEOUT", "180000"},
	{"TILT-TRIGGER", "2000"},
	{"TILT-PERIOD", "30000"},
	{"SLAVE-RECONF-TIMEOUT", "10000"},
	{"MIN-LINK-RECONNECT-PERIOD", "15000"},
	{"ELECTION-TIMEOUT", "10000"},
	{"SCRIPT-MAX-RUNTIME", "60000"},
	{"SCRIPT-RETRY-DELAY", "30000"},
}

// sentinelDebug DEBUG：裸回默认表；SET 已知参存内存回 OK；未知参或不成
// 对回 Unknown option（7.2.6 探针，原 token 回显）。
func (h *sentinelHandler) sentinelDebug(sub string, rest []protocol.Value) protocol.Value {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.dbg == nil {
		h.dbg = map[string]string{}
		for _, kv := range sentinelDebugDefaults {
			h.dbg[kv[0]] = kv[1]
		}
	}
	if len(rest) == 0 {
		out := []protocol.Value{}
		for _, kv := range sentinelDebugDefaults {
			v := h.dbg[kv[0]]
			out = append(out, protocol.BulkOf(kv[0]), protocol.BulkOf(v))
		}
		return protocol.ArrayOf(out...)
	}
	if len(rest)%2 != 0 {
		last, _ := argString(rest[len(rest)-1])
		return errValueStr(fmt.Sprintf("ERR Unknown option or number of arguments for SENTINEL DEBUG '%s'", last))
	}
	for i := 0; i < len(rest); i += 2 {
		name, ok := argString(rest[i])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		key := strings.ToUpper(name)
		if _, known := h.dbg[key]; !known {
			return errValueStr(fmt.Sprintf("ERR Unknown option or number of arguments for SENTINEL DEBUG '%s'", name))
		}
		val, ok := argString(rest[i+1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		h.dbg[key] = val
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *sentinelHandler) masters() protocol.Value {
	out := []protocol.Value{}
	for _, name := range h.reg.Names() {
		addr, ok := h.reg.MasterAddr(name)
		if !ok {
			continue
		}
		out = append(out, h.masterEntry(name, addr))
	}
	return protocol.ArrayOf(out...)
}

func (h *sentinelHandler) master(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	addr, found := h.reg.MasterAddr(name)
	if !found {
		return errValueStr("ERR No such master with that name")
	}
	return h.masterEntry(name, addr)
}

func (h *sentinelHandler) masterEntry(name, addr string) protocol.Value {
	host, port, _ := net.SplitHostPort(addr)
	flags := "master"
	if h.reg.IsDown(addr) {
		flags = "master,o-down"
	}
	return protocol.ArrayOf(
		protocol.BulkOf("name"), protocol.BulkOf(name),
		protocol.BulkOf("ip"), protocol.BulkOf(host),
		protocol.BulkOf("port"), protocol.BulkOf(port),
		protocol.BulkOf("quorum"), protocol.BulkOf(strconv.Itoa(h.reg.Quorum(name))),
		protocol.BulkOf("down-after-milliseconds"), protocol.BulkOf(strconv.FormatInt(h.reg.DownAfterMs(), 10)),
		protocol.BulkOf("flags"), protocol.BulkOf(flags),
	)
}

func (h *sentinelHandler) slaves(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	addrs, found := h.reg.Slaves(name)
	if !found {
		return errValueStr("ERR No such master with that name")
	}
	out := []protocol.Value{}
	for _, addr := range addrs {
		host, port, _ := net.SplitHostPort(addr)
		flags := "slave"
		if h.reg.IsDown(addr) {
			flags = "slave,o-down"
		}
		out = append(out, protocol.ArrayOf(
			protocol.BulkOf("name"), protocol.BulkOf(addr),
			protocol.BulkOf("ip"), protocol.BulkOf(host),
			protocol.BulkOf("port"), protocol.BulkOf(port),
			protocol.BulkOf("flags"), protocol.BulkOf(flags),
		))
	}
	return protocol.ArrayOf(out...)
}

func (h *sentinelHandler) sentinels(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if _, found := h.reg.MasterAddr(name); !found {
		return errValueStr("ERR No such master with that name")
	}
	host, port, _ := net.SplitHostPort(h.selfAddr)
	out := []protocol.Value{protocol.ArrayOf(
		protocol.BulkOf("name"), protocol.BulkOf(h.selfAddr),
		protocol.BulkOf("ip"), protocol.BulkOf(host),
		protocol.BulkOf("port"), protocol.BulkOf(port),
		protocol.BulkOf("flags"), protocol.BulkOf("sentinel"),
	)}
	for _, addr := range h.reg.Peers.Addrs() {
		ph, pp, _ := net.SplitHostPort(addr)
		out = append(out, protocol.ArrayOf(
			protocol.BulkOf("name"), protocol.BulkOf(addr),
			protocol.BulkOf("ip"), protocol.BulkOf(ph),
			protocol.BulkOf("port"), protocol.BulkOf(pp),
			protocol.BulkOf("flags"), protocol.BulkOf("sentinel"),
		))
	}
	return protocol.ArrayOf(out...)
}

// ckquorum 按静态拓扑计票：自己 + 已发现对端数达 quorum 即 OK。
// 文案为 gedis 自定（server 模式真机无此分支可 diff）。
func (h *sentinelHandler) ckquorum(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	if _, found := h.reg.MasterAddr(name); !found {
		return errValueStr("ERR No such master with that name")
	}
	need := h.reg.Quorum(name)
	have := 1 + len(h.reg.Peers.Addrs())
	if have >= need {
		return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
	}
	return errValueStr(fmt.Sprintf("ERR quorum for master '%s' is not reachable", name))
}

func (h *sentinelHandler) getMasterAddr(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	host, port, found := h.reg.GetMasterAddr(name)
	if !found {
		return errValueStr("ERR No such master with that name")
	}
	return protocol.ArrayOf(protocol.BulkOf(host), protocol.BulkOf(strconv.Itoa(port)))
}

func (h *sentinelHandler) failover(rest []protocol.Value) protocol.Value {
	name, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	oldHost, oldPort, found := h.reg.GetMasterAddr(name)
	if !found {
		return errValueStr("ERR No such master with that name")
	}
	if err := h.reg.Failover(name); err != nil {
		if errors.Is(err, sentinel.ErrUnknownMaster) {
			return errValueStr("ERR No such master with that name")
		}
		if errors.Is(err, sentinel.ErrNoHealthySlave) {
			return errValueStr("ERR No healthy slave")
		}
		return errValueStr("ERR " + err.Error())
	}
	newHost, newPort, _ := h.reg.GetMasterAddr(name)
	if h.pub != nil {
		h.pub.Publish("+switch-master", protocol.BulkOf(
			fmt.Sprintf("%s %s %d %s %d", name, oldHost, oldPort, newHost, newPort)))
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *sentinelHandler) isMasterDownByAddr(rest []protocol.Value) protocol.Value {
	ip, ok := argString(rest[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	port, ok := argString(rest[1])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	epochStr, ok := argString(rest[2])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	candEpoch, err := strconv.ParseUint(epochStr, 10, 64)
	if err != nil {
		return errValueStr("ERR invalid epoch")
	}
	candRunID, ok := argString(rest[3])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	addr := net.JoinHostPort(ip, port)
	name := ""
	for _, n := range h.reg.Names() {
		host, p, found := h.reg.GetMasterAddr(n)
		if found && host == ip && strconv.Itoa(p) == port {
			name = n
			break
		}
	}
	if name == "" {
		return errValueStr("ERR No such master with that address")
	}
	flag := "0"
	if h.reg.IsSubjectivelyDown(addr) {
		flag = "1"
	}
	granted, epoch := h.reg.HandleVote(name, candEpoch, candRunID)
	leaderRunID, leaderEpoch := "*", "0"
	if granted {
		leaderRunID, leaderEpoch = candRunID, strconv.FormatUint(epoch, 10)
	} else if votedRunID, votedEpoch, found := h.reg.VotedFor(name); found && votedRunID != "" {
		leaderRunID, leaderEpoch = votedRunID, strconv.FormatUint(votedEpoch, 10)
	}
	return protocol.ArrayOf(protocol.BulkOf(flag), protocol.BulkOf(leaderRunID), protocol.BulkOf(leaderEpoch))
}

// sentinelHelpLines SENTINEL HELP 全文（7.2.6-sentinel 探针逐字）。
var sentinelHelpLines = []string{
	"SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
	"CKQUORUM <master-name>",
	"    Check if the current Sentinel configuration is able to reach the quorum",
	"    needed to failover a master and the majority needed to authorize the",
	"    failover.",
	"CONFIG SET param value [param value ...]",
	"    Set a global Sentinel configuration parameter.",
	"CONFIG GET <param> [param param param ...]",
	"    Get global Sentinel configuration parameter.",
	"DEBUG [<param> <value> ...]",
	"    Show a list of configurable time parameters and their values (milliseconds).",
	"    Or update current configurable parameters values (one or more).",
	"GET-MASTER-ADDR-BY-NAME <master-name>",
	"    Return the ip and port number of the master with that name.",
	"FAILOVER <master-name>",
	"    Manually failover a master node without asking for agreement from other",
	"    Sentinels",
	"FLUSHCONFIG",
	"    Force Sentinel to rewrite its configuration on disk, including the current",
	"    Sentinel state.",
	"INFO-CACHE <master-name>",
	"    Return last cached INFO output from masters and all its replicas.",
	"IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
	"    Check if the master specified by ip:port is down from current Sentinel's",
	"    point of view.",
	"MASTER <master-name>",
	"    Show the state and info of the specified master.",
	"MASTERS",
	"    Show a list of monitored masters and their state.",
	"MONITOR <name> <ip> <port> <quorum>",
	"    Start monitoring a new master with the specified name, ip, port and quorum.",
	"MYID",
	"    Return the ID of the Sentinel instance.",
	"PENDING-SCRIPTS",
	"    Get pending scripts information.",
	"REMOVE <master-name>",
	"    Remove master from Sentinel's monitor list.",
	"REPLICAS <master-name>",
	"    Show a list of replicas for this master and their state.",
	"RESET <pattern>",
	"    Reset masters for specific master name matching this pattern.",
	"SENTINELS <master-name>",
	"    Show a list of Sentinel instances for this master and their state.",
	"SET <master-name> <option> <value> [<option> <value> ...]",
	"    Set configuration parameters for certain masters.",
	"SIMULATE-FAILURE [CRASH-AFTER-ELECTION] [CRASH-AFTER-PROMOTION] [HELP]",
	"    Simulate a Sentinel crash.",
	"HELP",
	"    Print this help.",
}

func sentinelHelp() protocol.Value {
	out := make([]protocol.Value, 0, len(sentinelHelpLines))
	for _, s := range sentinelHelpLines {
		out = append(out, protocol.Value{Kind: protocol.KindSimpleString, S: s})
	}
	return protocol.ArrayOf(out...)
}

func (h *sentinelHandler) info(_ context.Context, args []protocol.Value) protocol.Value {	if len(args) > 1 {
		return errValueStr("ERR wrong number of arguments for 'info' command")
	}
	if len(args) == 1 {
		sec, ok := argString(args[0])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		if !strings.EqualFold(sec, "sentinel") {
			return errValueStr(fmt.Sprintf("ERR unknown section '%s'", string(args[0].Bulk)))
		}
	}
	return protocol.BulkOf(fmt.Sprintf("# Sentinel\r\nsentinel_masters:%d\r\n", len(h.reg.Names())))
}
