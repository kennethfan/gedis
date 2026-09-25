package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WM: bit 基础运算（32位有符号：band/bor/bxor/bnot/shift/rotate/bswap/tobit/tohex）
func Test_Lua_when_BitOps(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return bit.band(5,3)", "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "return bit.bor(5,3)", "0"))
	require.Equal(t, intVal(6), dispatchLua(r, c, "EVAL", "return bit.bxor(5,3)", "0"))
	require.Equal(t, intVal(-6), dispatchLua(r, c, "EVAL", "return bit.bnot(5)", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "return bit.lshift(1,3)", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "return bit.rshift(8,2)", "0"))
	require.Equal(t, intVal(-2), dispatchLua(r, c, "EVAL", "return bit.arshift(-8,2)", "0"))
	require.Equal(t, intVal(2147483647), dispatchLua(r, c, "EVAL", "return bit.rshift(-1,1)", "0"))
	require.Equal(t, intVal(-1), dispatchLua(r, c, "EVAL", "return bit.arshift(-1,1)", "0"))
	require.Equal(t, intVal(16777216), dispatchLua(r, c, "EVAL", "return bit.bswap(1)", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return bit.tobit(1)", "0"))
	require.Equal(t, intVal(-1), dispatchLua(r, c, "EVAL", "return bit.tobit(-1)", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return bit.band(7,3,1)", "0"))
	require.Equal(t, bulk("000000ff"), dispatchLua(r, c, "EVAL", "return bit.tohex(255)", "0"))
	require.Equal(t, bulk("ffffffff"), dispatchLua(r, c, "EVAL", "return bit.tohex(-1)", "0"))
	require.Equal(t, bulk("000000FF"), dispatchLua(r, c, "EVAL", "return bit.tohex(255,-8)", "0"))
}

// WM: bit 错误文案逐字（缺参/错参报 bad argument #N）
func Test_Lua_when_BitErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errOf := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	s := "return bit.tobit()"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'tobit' (number expected, got no value) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return bit.band('a',1)"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'band' (number expected, got string) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return bit.lshift(1)"
	require.Equal(t, "ERR user_script:1: bad argument #2 to 'lshift' (number expected, got no value) script: "+sha(s)+", on @user_script:1.", errOf(s))
}

// WM: pcall 错误形态（A式直调：无位前缀+函数名'?'；B'式经Lua闭包：有位+真名）
func Test_Lua_when_BitPcallForm(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk("bad argument #1 to '?' (number expected, got no value)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(bit.tobit); return e", "0"))
	require.Equal(t, bulk("bad argument #2 to '?' (number expected, got no value)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(bit.lshift,1); return e", "0"))
	require.Equal(t, bulk("user_script:1: bad argument #1 to 'tobit' (number expected, got no value)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(function() return bit.tobit() end); return e", "0"))
}
