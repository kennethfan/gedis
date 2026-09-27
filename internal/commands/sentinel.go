package commands

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"

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

func RegisterSentinel(r *network.Router, reg *sentinel.Registry, selfAddr string, pub *PubSubRegistry) {
	for _, m := range sentinelMeta {
		acl.RegisterMeta(m)
	}
	h := &sentinelHandler{reg: reg, selfAddr: selfAddr, pub: pub}
	r.Register("SENTINEL", h.sentinel)
	r.Register("INFO", h.info)
}

type sentinelHandler struct {
	reg      *sentinel.Registry
	selfAddr string
	pub      *PubSubRegistry
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
	case "RESET", "REMOVE", "SET":
		return errValueStr("ERR Static sentinel topology does not support SENTINEL " + strings.ToUpper(sub))
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
		protocol.BulkOf("quorum"), protocol.BulkOf("1"),
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
	return protocol.ArrayOf(protocol.ArrayOf(
		protocol.BulkOf("name"), protocol.BulkOf(h.selfAddr),
		protocol.BulkOf("ip"), protocol.BulkOf(host),
		protocol.BulkOf("port"), protocol.BulkOf(port),
		protocol.BulkOf("flags"), protocol.BulkOf("sentinel"),
	))
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

func (h *sentinelHandler) info(_ context.Context, args []protocol.Value) protocol.Value {
	if len(args) > 1 {
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
