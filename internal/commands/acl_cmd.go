package commands

import (
	"context"
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
}

// RegisterACL 注册 ACL（list/users/setuser/deluser/getuser/whoami/save）。
func RegisterACL(r *network.Router, st ACLStore, reg *AuthRegistry, saver ACLSaver) {
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
	return strings.Join(parts, " ")
}

func describeGetUser(snap acl.Snapshot) protocol.Value {
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
		protocol.BulkOf("keys"), protocol.ArrayOf(),
		protocol.BulkOf("channels"), protocol.ArrayOf(),
	)
}
