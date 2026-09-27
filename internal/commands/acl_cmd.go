package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// ACLSaver 持久化钩子（Task5 挂载写 aclfile；nil 时 SAVE 报错）。
type ACLSaver func() error

// ACLStore 是 ACL 用户管理命令消费的用户表接口，由 internal/acl.Store 实现。
type ACLStore interface {
	SetUser(name string, rules ...string) error
	DelUser(name string) error
	Names() []string
	GetUser(name string) (acl.Snapshot, bool)
	UserExists(name string) bool
	Check(user, cmd string, keys, channels []string) error
	AclLog() []acl.LogEntry
	ResetLog()
}

// RegisterACL 注册 ACL（list/users/setuser/deluser/getuser/whoami/save）。
var aclCmdMeta = []acl.Meta{
	{Name: "ACL", Category: "connection", Keys: acl.KeySpec{First: -1}},
}

func RegisterACL(r *network.Router, st ACLStore, reg *AuthRegistry, saver ACLSaver) {
	for _, m := range aclCmdMeta {
		acl.RegisterMeta(m)
	}
	r.Register("ACL", func(ctx context.Context, args []protocol.Value) protocol.Value {
		if len(args) == 0 {
			return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl' command"}
		}
		sub, ok := argString(args[0])
		if !ok {
			return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl' command"}
		}
		rest := args[1:]
		switch strings.ToUpper(sub) {
		case "LIST":
			if len(rest) != 0 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|list' command"}
			}
			elems := make([]protocol.Value, 0)
			for _, n := range st.Names() {
				snap, _ := st.GetUser(n)
				elems = append(elems, protocol.BulkOf(describeUser(n, snap)))
			}
			return protocol.ArrayOf(elems...)
		case "USERS":
			if len(rest) != 0 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|users' command"}
			}
			elems := make([]protocol.Value, 0)
			for _, n := range st.Names() {
				elems = append(elems, protocol.BulkOf(n))
			}
			return protocol.ArrayOf(elems...)
		case "SETUSER":
			if len(rest) < 1 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|setuser' command"}
			}
			name, ok := argString(rest[0])
			if !ok {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|setuser' command"}
			}
			rules := make([]string, 0, len(rest)-1)
			for _, a := range rest[1:] {
				s, ok := argString(a)
				if !ok {
					return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|setuser' command"}
				}
				rules = append(rules, s)
			}
			if err := st.SetUser(name, rules...); err != nil {
				return protocol.Value{Kind: protocol.KindError, S: "ERR " + err.Error()}
			}
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		case "DELUSER":
			if len(rest) < 1 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|deluser' command"}
			}
			cur := currentUser(ctx, reg)
			names := make([]string, 0, len(rest))
			for _, a := range rest {
				s, ok := argString(a)
				if !ok {
					return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|deluser' command"}
				}
				if s == cur && cur != "" {
					return protocol.Value{Kind: protocol.KindError, S: "ERR Cannot delete the current user"}
				}
				names = append(names, s)
			}
			var n int64
			for _, name := range names {
				existed := st.UserExists(name)
				if err := st.DelUser(name); err != nil {
					return protocol.Value{Kind: protocol.KindError, S: "ERR " + err.Error()}
				}
				if existed {
					n++
				}
			}
			return protocol.Value{Kind: protocol.KindInteger, I: n}
		case "GETUSER":
			if len(rest) != 1 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|getuser' command"}
			}
			name, ok := argString(rest[0])
			if !ok {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|getuser' command"}
			}
			snap, ok := st.GetUser(name)
			if !ok {
				return protocol.Value{Kind: protocol.KindArray}
			}
			return describeGetUser(snap)
		case "WHOAMI":
			if len(rest) != 0 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|whoami' command"}
			}
			if cur := currentUser(ctx, reg); cur != "" {
				return protocol.BulkOf(cur)
			}
			return protocol.BulkOf("default")
		case "SAVE":
			if len(rest) != 0 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|save' command"}
			}
			if saver == nil {
				return protocol.Value{Kind: protocol.KindError, S: "ERR There is no ACL file configured to save the users to."}
			}
			if err := saver(); err != nil {
				return protocol.Value{Kind: protocol.KindError, S: "ERR " + err.Error()}
			}
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		case "DRYRUN":
			if len(rest) < 2 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|dryrun' command"}
			}
			username, ok := argString(rest[0])
			if !ok {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|dryrun' command"}
			}
			if !st.UserExists(username) {
				return protocol.Value{Kind: protocol.KindError, S: "ERR User '" + username + "' not found"}
			}
			cmdArgs := make([]string, 0, len(rest)-1)
			for _, a := range rest[1:] {
				s, ok := argString(a)
				if !ok {
					return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|dryrun' command"}
				}
				cmdArgs = append(cmdArgs, s)
			}
			cmdName := strings.ToUpper(cmdArgs[0])
			params := cmdArgs[1:]
			if err := st.Check(username, cmdName, acl.ExtractKeys(cmdName, params), dryrunChannels(cmdName, params)); err != nil {
				return protocol.BulkOf(dryrunReason(username, err))
			}
			return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
		case "LOG":
			if len(rest) > 1 {
				return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|log' command"}
			}
			if len(rest) == 1 {
				s, ok := argString(rest[0])
				if !ok {
					return protocol.Value{Kind: protocol.KindError, S: "ERR wrong number of arguments for 'acl|log' command"}
				}
				if strings.EqualFold(s, "RESET") {
					st.ResetLog()
					return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
				}
				n, err := strconv.Atoi(s)
				if err != nil || n < 0 {
					return protocol.Value{Kind: protocol.KindError, S: "ERR value is not an integer or out of range"}
				}
				return renderLog(st.AclLog(), n)
			}
			return renderLog(st.AclLog(), 10)
		default:
			return protocol.Value{Kind: protocol.KindError, S: "ERR unknown subcommand '" + sub + "'. Try ACL HELP."}
		}
	})
}

func currentUser(ctx context.Context, reg *AuthRegistry) string {
	if name, ok := network.UserFromContext(ctx); ok && name != "" {
		return name
	}
	if reg == nil {
		return ""
	}
	if conn, ok := network.ConnFromContext(ctx); ok && conn != nil {
		return reg.UserOf(conn)
	}
	return ""
}

func describeUser(name string, snap acl.Snapshot) string {
	parts := []string{"user", name}
	if snap.On {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}
	if snap.NoPass {
		parts = append(parts, "nopass")
	} else if snap.PassHash != "" {
		parts = append(parts, "#"+snap.PassHash)
	}
	parts = append(parts, snap.Rules...)
	for _, k := range snap.Keys {
		parts = append(parts, "~"+k)
	}
	for _, c := range snap.Chans {
		parts = append(parts, "&"+c)
	}
	for _, sel := range snap.Selectors {
		parts = append(parts, sel.Inline())
	}
	return strings.Join(parts, " ")
}

func describeGetUser(snap acl.Snapshot) protocol.Value {
	keys := make([]protocol.Value, 0, len(snap.Keys))
	for _, k := range snap.Keys {
		keys = append(keys, protocol.BulkOf("~"+k))
	}
	chans := make([]protocol.Value, 0, len(snap.Chans))
	for _, c := range snap.Chans {
		chans = append(chans, protocol.BulkOf("&"+c))
	}
	flags := []protocol.Value{}
	if snap.On {
		flags = append(flags, protocol.BulkOf("on"))
	} else {
		flags = append(flags, protocol.BulkOf("off"))
	}
	if snap.NoPass {
		flags = append(flags, protocol.BulkOf("nopass"))
	}
	passwords := []protocol.Value{}
	if snap.PassHash != "" {
		passwords = append(passwords, protocol.BulkOf(snap.PassHash))
	}
	return protocol.ArrayOf(
		protocol.BulkOf("flags"), protocol.ArrayOf(flags...),
		protocol.BulkOf("passwords"), protocol.ArrayOf(passwords...),
		protocol.BulkOf("commands"), protocol.BulkOf(strings.Join(snap.Rules, " ")),
		protocol.BulkOf("keys"), protocol.ArrayOf(keys...),
		protocol.BulkOf("channels"), protocol.ArrayOf(chans...),
		protocol.BulkOf("selectors"), renderSelectors(snap.Selectors),
	)
}

// renderSelectors 把括号组渲染为 map 数组（commands/keys/channels），贴近真机 GETUSER 形状。
func renderSelectors(sels []acl.SelectorView) protocol.Value {
	elems := make([]protocol.Value, 0, len(sels))
	for _, s := range sels {
		keys := make([]protocol.Value, 0, len(s.Keys))
		for _, k := range s.Keys {
			keys = append(keys, protocol.BulkOf(k))
		}
		chans := make([]protocol.Value, 0, len(s.Chans))
		for _, c := range s.Chans {
			chans = append(chans, protocol.BulkOf(c))
		}
		elems = append(elems, protocol.Value{Kind: protocol.KindMap, Pairs: []protocol.Pair{
			{K: protocol.BulkOf("commands"), V: protocol.BulkOf(strings.Join(s.Commands, " "))},
			{K: protocol.BulkOf("keys"), V: protocol.ArrayOf(keys...)},
			{K: protocol.BulkOf("channels"), V: protocol.ArrayOf(chans...)},
		}})
	}
	return protocol.ArrayOf(elems...)
}

// dryrunReason 把 Check 拒绝改写为 DRYRUN 文案（真机 7.2.6：去 NOPERM 前缀，
// key/channel 逐名）。非 Denial 错误透传原文。
func dryrunReason(username string, err error) string {
	d, ok := acl.AsDenial(err)
	if !ok {
		return err.Error()
	}
	switch d.Kind {
	case acl.DenyKey:
		return fmt.Sprintf("User %s has no permissions to access the '%s' key", username, d.Name)
	case acl.DenyChannel:
		return fmt.Sprintf("User %s has no permissions to access the '%s' channel", username, d.Name)
	default:
		return strings.TrimPrefix(d.Error(), "NOPERM ")
	}
}

// dryrunChannels 与 Router extractChannels 同规则（DRYRUN 不经过 Router 门）。
func dryrunChannels(cmd string, params []string) []string {
	switch cmd {
	case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE":
		return params
	case "PUBLISH":
		if len(params) > 0 {
			return params[:1]
		}
	}
	return nil
}

// renderLog 取最新 n 条拒绝记录渲染（自研紧凑形状：time/client/cmd/reason）。
func renderLog(log []acl.LogEntry, n int) protocol.Value {
	if n > len(log) {
		n = len(log)
	}
	elems := make([]protocol.Value, 0, n)
	for _, e := range log[:n] {
		elems = append(elems, protocol.ArrayOf(
			protocol.BulkOf(strconv.FormatInt(e.Time, 10)),
			protocol.BulkOf(e.Client),
			protocol.BulkOf(e.Cmd),
			protocol.BulkOf(e.Reason),
		))
	}
	return protocol.ArrayOf(elems...)
}
