package commands

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// 连接层命令：HELLO/SELECT/QUIT/ECHO/RESET/COMMAND。
// 全部无 key、非写，WriteCommandSet 不登记；key 路由走 cluster.KeysOf 默认直通。

var connMeta = []acl.Meta{
	{Name: "HELLO", Category: "connection", Keys: acl.KeySpec{First: -1}},
	{Name: "SELECT", Category: "connection", Keys: acl.KeySpec{First: -1}},
	{Name: "QUIT", Category: "connection", Keys: acl.KeySpec{First: -1}},
	{Name: "ECHO", Category: "connection", Keys: acl.KeySpec{First: -1}},
	{Name: "RESET", Category: "connection", Keys: acl.KeySpec{First: -1}},
	{Name: "COMMAND", Category: "connection", Keys: acl.KeySpec{First: -1}},
}

// ConnState 是单连接的可变状态：RESP 协议版本与连接名。
// 读写只发生在该连接的 handler goroutine（server 每连接单 goroutine），
// 但测试与关闭路径并发触碰，故仍加锁。
type ConnState struct {
	Proto int
	Name  string
}

// ConnRegistry 按连接存 ConnState（仿 AuthRegistry conn-keyed 模式，
// main.go 经 OnConnClose 清理）。
type ConnRegistry struct {
	mu sync.Mutex
	m  map[net.Conn]*ConnState
}

func NewConnRegistry() *ConnRegistry {
	return &ConnRegistry{m: make(map[net.Conn]*ConnState)}
}

func (c *ConnRegistry) state(conn net.Conn) *ConnState {
	st, ok := c.m[conn]
	if !ok {
		st = &ConnState{Proto: 2}
		c.m[conn] = st
	}
	return st
}

func (c *ConnRegistry) SetProto(conn net.Conn, ver int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state(conn).Proto = ver
}

// ProtoOf 返回连接的 RESP 版本；无记录（单测直调）默认 2。
func (c *ConnRegistry) ProtoOf(conn net.Conn) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state(conn).Proto
}

func (c *ConnRegistry) SetName(conn net.Conn, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state(conn).Name = name
}

func (c *ConnRegistry) NameOf(conn net.Conn) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state(conn).Name
}

// Reset 清空连接名（RESET 语义子集：认证与协议版本保留，
// MULTI 丢弃/退订/UNWATCH 待各 registry 暴露 Clear 后补齐）。
func (c *ConnRegistry) Reset(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state(conn).Name = ""
}

func (c *ConnRegistry) ConnClosed(conn net.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.m, conn)
}

type connHandler struct {
	router *network.Router
	store  AuthStore
	auth   *AuthRegistry
	conns  *ConnRegistry
}

// RegisterConn 注册连接层命令，返回连接状态表（main 装配 OnConnClose 用）。
func RegisterConn(r *network.Router, st AuthStore, auth *AuthRegistry) *ConnRegistry {
	for _, m := range connMeta {
		acl.RegisterMeta(m)
	}
	c := NewConnRegistry()
	h := &connHandler{router: r, store: st, auth: auth, conns: c}
	r.Register("HELLO", h.hello)
	r.Register("SELECT", h.select_)
	r.Register("QUIT", h.quit)
	r.Register("ECHO", h.echo)
	r.Register("RESET", h.reset)
	r.Register("COMMAND", h.command)
	return c
}

// helloVersion 解析 HELLO 首参：bulk "2"/"3" 或 integer 2/3。
func helloVersion(v protocol.Value) (int, bool) {
	switch v.Kind {
	case protocol.KindBulkString:
		s := string(v.Bulk)
		if s == "2" || s == "3" {
			n, _ := strconv.Atoi(s)
			return n, true
		}
	case protocol.KindInteger:
		if v.I == 2 || v.I == 3 {
			return int(v.I), true
		}
	}
	return 0, false
}

func isAuthWord(v protocol.Value) bool {
	s, ok := argString(v)
	return ok && strings.EqualFold(s, "AUTH")
}

// helloReply 按版本返回握手信息：2 走数组形（真机 HELLO 2 形状），
// 3 走 map 形（RESP3 `%` 编码，客户端已声明 RESP3，直接回 RESP3 合法）。
// version 字段取 7.4.0，与 INFO redis_version 的兼容声明一致。
func helloReply(ver int) protocol.Value {
	fields := []protocol.Value{
		protocol.BulkOf("server"), protocol.BulkOf("gedis"),
		protocol.BulkOf("version"), protocol.BulkOf("7.4.0"),
		protocol.BulkOf("proto"), {Kind: protocol.KindInteger, I: int64(ver)},
		protocol.BulkOf("mode"), protocol.BulkOf("standalone"),
		protocol.BulkOf("role"), protocol.BulkOf("master"),
		protocol.BulkOf("modules"), {Kind: protocol.KindArray},
	}
	if ver == 3 {
		pairs := make([]protocol.Pair, 0, len(fields)/2)
		for i := 0; i+1 < len(fields); i += 2 {
			pairs = append(pairs, protocol.Pair{K: fields[i], V: fields[i+1]})
		}
		return protocol.Value{Kind: protocol.KindMap, Pairs: pairs}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: fields}
}

func (h *connHandler) hello(ctx context.Context, args []protocol.Value) protocol.Value {
	ver := 3
	rest := args
	if len(rest) > 0 {
		if v, ok := helloVersion(rest[0]); ok {
			ver = v
			rest = rest[1:]
		} else if !isAuthWord(rest[0]) {
			return errValueStr("NOPROTO unsupported protocol version")
		}
	}
	name := ""
	for len(rest) > 0 {
		kw, ok := argString(rest[0])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(kw) {
		case "AUTH":
			if len(rest) < 3 {
				return errValueStr("ERR wrong number of arguments for 'hello' command")
			}
			user, ok1 := argString(rest[1])
			pass, ok2 := argString(rest[2])
			if !ok1 || !ok2 {
				return errValueStr("ERR syntax error")
			}
			if !h.store.Authenticate(user, pass) {
				return errValueStr("WRONGPASS invalid username-password pair or user is disabled.")
			}
			if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
				h.auth.Authenticate(conn, user)
			}
			rest = rest[3:]
		case "SETNAME":
			if len(rest) < 2 {
				return errValueStr("ERR wrong number of arguments for 'hello' command")
			}
			nm, ok := argString(rest[1])
			if !ok {
				return errValueStr("ERR syntax error")
			}
			name = nm
			rest = rest[2:]
		default:
			return errValueStr("ERR syntax error")
		}
	}
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		h.conns.SetProto(conn, ver)
		if name != "" {
			h.conns.SetName(conn, name)
		}
	}
	return helloReply(ver)
}

// select_：单库引擎。SELECT 0 回 OK，其余报越界（不断连，对齐主流单库 clone）。
func (h *connHandler) select_(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'select' command")
	}
	s, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR value is not an integer or out of range")
	}
	idx, err := strconv.Atoi(s)
	if err != nil {
		return errValueStr("ERR value is not an integer or out of range")
	}
	if idx != 0 {
		return errValueStr("ERR DB index is out of range")
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *connHandler) quit(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'quit' command")
	}
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		network.RequestClose(conn)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *connHandler) echo(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'echo' command")
	}
	switch args[0].Kind {
	case protocol.KindBulkString:
		out := make([]byte, len(args[0].Bulk))
		copy(out, args[0].Bulk)
		return protocol.Value{Kind: protocol.KindBulkString, Bulk: out}
	case protocol.KindInteger:
		return protocol.BulkOf(strconv.FormatInt(args[0].I, 10))
	default:
		return errValueStr("ERR syntax error")
	}
}

func (h *connHandler) reset(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'reset' command")
	}
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		h.conns.Reset(conn)
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "RESET"}
}

func (h *connHandler) command(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) == 0 {
		return h.commandInfo(nil)
	}
	sub, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR syntax error")
	}
	switch strings.ToUpper(sub) {
	case "COUNT":
		if len(args) != 1 {
			return errValueStr("ERR wrong number of arguments for 'command|count' command")
		}
		return protocol.Value{Kind: protocol.KindInteger, I: int64(len(h.router.Commands()))}
	case "INFO":
		names := make([]string, 0, len(args)-1)
		for _, a := range args[1:] {
			s, ok := argString(a)
			if !ok {
				return errValueStr("ERR syntax error")
			}
			names = append(names, s)
		}
		return h.commandInfo(names)
	case "GETKEYS":
		if len(args) < 2 {
			return errValueStr("ERR wrong number of arguments for 'command|getkeys' command")
		}
		name, ok := argString(args[1])
		if !ok {
			return errValueStr("ERR syntax error")
		}
		upper := strings.ToUpper(name)
		if _, ok := acl.LookupMeta(upper); !ok {
			return errValueStr(fmt.Sprintf("ERR unknown command '%s'", name))
		}
		strs := make([]string, 0, len(args)-2)
		for _, a := range args[2:] {
			s, ok := argString(a)
			if !ok {
				return errValueStr("ERR syntax error")
			}
			strs = append(strs, s)
		}
		keys := acl.ExtractKeys(upper, strs)
		out := make([]protocol.Value, 0, len(keys))
		for _, k := range keys {
			out = append(out, protocol.BulkOf(k))
		}
		return protocol.Value{Kind: protocol.KindArray, Elems: out}
	default:
		return errValueStr(fmt.Sprintf("ERR unknown subcommand '%s'. Try COMMAND HELP.", sub))
	}
}

// commandInfo 生成 COMMAND INFO 条目：nil 表全部（按名排序保证确定性），
// 否则按给定名逐个（未知名给 null，与真机一致）。DOCS 在 Phase 2 交付前走
// 上方 default 的 unknown-subcommand 诚实报错。
func (h *connHandler) commandInfo(names []string) protocol.Value {
	all := names == nil
	if all {
		names = h.router.Commands()
		sort.Strings(names)
	}
	out := make([]protocol.Value, 0, len(names))
	for _, n := range names {
		out = append(out, commandEntry(strings.ToUpper(n)))
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

// commandEntry 单条目：[name arity flags firstkey lastkey step aclcats]。
// flags 只从已知事实拼（readonly），不虚构 loading/stale 等。
func commandEntry(name string) protocol.Value {
	m, ok := acl.LookupMeta(name)
	if !ok {
		return protocol.Value{Kind: protocol.KindNull}
	}
	arity := 0
	if a, ok := commandArity[name]; ok {
		arity = a
	}
	var flags []protocol.Value
	if m.ReadOnly {
		flags = append(flags, protocol.BulkOf("readonly"))
	}
	if flags == nil {
		flags = []protocol.Value{}
	}
	first, last, step := int64(0), int64(0), int64(0)
	if m.Keys.First >= 0 {
		first = int64(m.Keys.First)
		last = int64(m.Keys.Last)
		step = int64(m.Keys.Step)
	}
	cats := []protocol.Value{protocol.BulkOf("@" + m.Category)}
	return protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf(strings.ToLower(name)),
		{Kind: protocol.KindInteger, I: int64(arity)},
		{Kind: protocol.KindArray, Elems: flags},
		{Kind: protocol.KindInteger, I: first},
		{Kind: protocol.KindInteger, I: last},
		{Kind: protocol.KindInteger, I: step},
		{Kind: protocol.KindArray, Elems: cats},
	}}
}
