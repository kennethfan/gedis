package commands

import (
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openACLCatSetup(t testing.TB) *network.Router {
	t.Helper()
	r := network.NewRouter()
	st := acl.NewStore()
	authReg := RegisterAuth(r, st)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{AppendOnly: false})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	RegisterStrings(r, store)
	RegisterACL(r, st, authReg, nil, nil)
	RegisterConn(r, st, authReg)
	return r
}

// Given: ACL 已注册
// When: ACL CAT / CAT connection / CAT 大小写
// Then: 类别表含 connection；成员含 acl
func Test_ACL_when_Cat(t *testing.T) {
	r := openACLCatSetup(t)
	got := dispatch(r, "ACL", "CAT")
	require.Equal(t, protocol.KindArray, got.Kind)
	found := false
	for _, e := range got.Elems {
		if string(e.Bulk) == "connection" {
			found = true
		}
	}
	require.True(t, found, "CAT must list connection: %v", got)

	got = dispatch(r, "ACL", "CAT", "CONNECTION")
	require.Equal(t, protocol.KindArray, got.Kind)
	found = false
	for _, e := range got.Elems {
		if string(e.Bulk) == "acl" {
			found = true
		}
	}
	require.True(t, found, "CAT connection must contain acl: %v", got)
}

// Given: ACL 已注册
// When: CAT 未知类别 / 多参
// Then: Unknown category / CAT suburban 错
func Test_ACL_when_CatRejects(t *testing.T) {
	r := openACLCatSetup(t)
	got := dispatch(r, "ACL", "CAT", "nosuchcat")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR Unknown category 'nosuchcat'", got.S)

	got = dispatch(r, "ACL", "CAT", "a", "b")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR unknown subcommand or wrong number of arguments for 'CAT'. Try ACL HELP.", got.S)
}

// Given: ACL 已注册
// When: GENPASS [bits]
// Then: 默认 64hex；16→4 位；10→3 位；非法 bits 报错
func Test_ACL_when_Genpass(t *testing.T) {
	r := openACLCatSetup(t)
	got := dispatch(r, "ACL", "GENPASS")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Len(t, got.Bulk, 64)

	got = dispatch(r, "ACL", "GENPASS", "16")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Len(t, got.Bulk, 4)

	got = dispatch(r, "ACL", "GENPASS", "10")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Len(t, got.Bulk, 3)

	for _, bits := range []string{"0", "-5", "4097"} {
		got = dispatch(r, "ACL", "GENPASS", bits)
		require.Equal(t, protocol.KindError, got.Kind)
		require.Equal(t, "ERR ACL GENPASS argument must be the number of bits for the output password, a positive number up to 4096", got.S)
	}
	got = dispatch(r, "ACL", "GENPASS", "abc")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR value is not an integer or out of range", got.S)

	got = dispatch(r, "ACL", "GENPASS", "a", "b")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR unknown subcommand or wrong number of arguments for 'GENPASS'. Try ACL HELP.", got.S)
}

// Given: ACL 已注册
// When: HELP / LOAD（无 loader / 有 loader / 多参）
// Then: 首行 HELP 文案；无文件长诚实错；loader OK
func Test_ACL_when_HelpLoad(t *testing.T) {
	r := openACLCatSetup(t)
	got := dispatch(r, "ACL", "HELP")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.True(t, len(got.Elems) > 0)
	require.Equal(t, "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", got.Elems[0].S)
	require.Equal(t, "    Print this help.", got.Elems[len(got.Elems)-1].S)

	got = dispatch(r, "ACL", "HELP", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR wrong number of arguments for 'acl|help' command", got.S)

	got = dispatch(r, "ACL", "LOAD")
	require.Equal(t, protocol.KindError, got.Kind)
	require.True(t, strings.HasPrefix(got.S, "ERR This Redis instance is not configured to use an ACL file."), got.S)

	got = dispatch(r, "ACL", "LOAD", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR wrong number of arguments for 'acl|load' command", got.S)

	r2 := network.NewRouter()
	st := acl.NewStore()
	authReg := RegisterAuth(r2, st)
	RegisterACL(r2, st, authReg, nil, func() error { return nil })
	got = dispatch(r2, "ACL", "LOAD")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
}

// Given: COMMAND 已注册且 SET/GET 有元数据
// When: GETKEYSANDFLAGS
// Then: 写命令 RW access update；读命令 RO access；未知命令 Invalid；无参 arity 错
func Test_Command_when_GetKeysAndFlags(t *testing.T) {
	r := openACLCatSetup(t)
	got := dispatch(r, "COMMAND", "GETKEYSANDFLAGS", "SET", "k", "v")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Len(t, got.Elems[0].Elems, 2)
	require.Equal(t, "k", string(got.Elems[0].Elems[0].Bulk))
	require.Len(t, got.Elems[0].Elems[1].Elems, 1)
	require.Equal(t, "RW access update", string(got.Elems[0].Elems[1].Elems[0].Bulk))

	got = dispatch(r, "COMMAND", "GETKEYSANDFLAGS", "GET", "k")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, "RO access", string(got.Elems[0].Elems[1].Elems[0].Bulk))

	got = dispatch(r, "COMMAND", "GETKEYSANDFLAGS", "BOGUS", "k")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR Invalid command specified", got.S)

	got = dispatch(r, "COMMAND", "GETKEYSANDFLAGS")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR wrong number of arguments for 'command|getkeysandflags' command", got.S)
}
