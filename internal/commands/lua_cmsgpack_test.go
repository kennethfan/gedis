package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WM: cmsgpack pack/unpack 往返（对标 lua-cmsgpack 0.4.0：int/str/bool/nil/array）
func Test_Lua_when_CmsgpackRoundtrip(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack(7))", "0"))
	require.Equal(t, bulk("hi"), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack('hi'))", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack(true))", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack(nil))", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "local a=cmsgpack.unpack(cmsgpack.pack({1,2})); return a[2]", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return #cmsgpack.pack(7)", "0"))
	require.Equal(t, bulk("cmsgpack"), dispatchLua(r, c, "EVAL", "return cmsgpack._NAME", "0"))
	require.Equal(t, bulk("lua-cmsgpack 0.4.0"), dispatchLua(r, c, "EVAL", "return cmsgpack._VERSION", "0"))
}

// WM: cmsgpack unpack_one/unpack_limit 双返回（位置,值；读尽返回 -1）
func Test_Lua_when_CmsgpackOne(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_one(cmsgpack.pack(7)); return v", "0"))
	require.Equal(t, intVal(-1), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_one(cmsgpack.pack(7)); return p", "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_one(cmsgpack.pack(7)..cmsgpack.pack(8)); return v", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_one(cmsgpack.pack(7)..cmsgpack.pack(8)); return p", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_one(cmsgpack.pack(7)..cmsgpack.pack(8), 1); return v", "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "local p,v=cmsgpack.unpack_limit(cmsgpack.pack(7)..cmsgpack.pack(8), 10, 0); return v", "0"))
}

// WM: cmsgpack 错误文案逐字（缺参/错参报 bad argument #N）
func Test_Lua_when_CmsgpackErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errOf := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	s := "return cmsgpack.pack()"
	require.Equal(t, "ERR user_script:1: bad argument #0 to 'pack' (MessagePack pack needs input.) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack()"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'unpack' (string expected, got no value) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack_limit(cmsgpack.pack(7))"
	require.Equal(t, "ERR user_script:1: bad argument #2 to 'unpack_limit' (number expected, got no value) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack_one(cmsgpack.pack(7), 9)"
	require.Equal(t, "ERR user_script:1: Start offset 9 greater than input length 1. script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack_one(cmsgpack.pack(7), -1)"
	require.Equal(t, "ERR user_script:1: Invalid request to unpack with offset of -1 and limit of 1. script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack(string.sub(cmsgpack.pack(1000),1,2))"
	require.Equal(t, "ERR user_script:1: Missing bytes in input. script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack(string.char(0xc1))"
	require.Equal(t, "ERR user_script:1: Bad data format in input. script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return cmsgpack.unpack(string.char(0xd4,0x01,0x02))"
	require.Equal(t, "ERR user_script:1: Bad data format in input. script: "+sha(s)+", on @user_script:1.", errOf(s))
}

// WM: pcall 错误形态（A式直调：无位前缀+函数名'?'）
func Test_Lua_when_CmsgpackPcallForm(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk("bad argument #0 to '?' (MessagePack pack needs input.)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(cmsgpack.pack); return e", "0"))
	require.Equal(t, bulk("bad argument #1 to '?' (string expected, got no value)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(cmsgpack.unpack); return e", "0"))
}

// WM: cmsgpack 0.4.0 语义（未知类型→nil、嵌套16层截断、多值unpack、limit计值数、offset/limit/number参数折叠）
func Test_Lua_when_CmsgpackCompat(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return #cmsgpack.pack(pcall)", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack(pcall))==nil and 1 or 0", "0"))
	require.Equal(t, intVal(3), dispatchLua(r, c, "EVAL", "local a,b=cmsgpack.unpack(cmsgpack.pack(1)..cmsgpack.pack(2)); return a+b", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "local o,v=cmsgpack.unpack_limit(cmsgpack.pack(7)..cmsgpack.pack(8),10,1); return v", "0"))
	require.Equal(t, intVal(-1), dispatchLua(r, c, "EVAL", "local o,v=cmsgpack.unpack_limit(cmsgpack.pack(7)..cmsgpack.pack(8),10,1); return o", "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "local o,v=cmsgpack.unpack_one(cmsgpack.pack(7),0,5); return v", "0"))
	require.Equal(t, intVal(49), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(123)", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "local o,v=cmsgpack.unpack_one(cmsgpack.pack(7)..cmsgpack.pack(8),'1'); return v", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatchLua(r, c, "EVAL", "return cmsgpack.unpack('')", "0"))
	require.Equal(t, intVal(5), dispatchLua(r, c, "EVAL", "return #(cmsgpack.pack(2^63))", "0"))
	require.Equal(t, intVal(9), dispatchLua(r, c, "EVAL", "return #(cmsgpack.pack(-2^63-1))", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack(2^70))==2^70 and 1 or 0", "0"))
	require.Equal(t, intVal(3), dispatchLua(r, c, "EVAL", "local o,a,b=cmsgpack.unpack_limit(cmsgpack.pack(1)..cmsgpack.pack(2)..cmsgpack.pack(3),2,0); return a+b", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "local o,a,b=cmsgpack.unpack_limit(cmsgpack.pack(1)..cmsgpack.pack(2)..cmsgpack.pack(3),2,0); return o", "0"))
	require.Equal(t, intVal(16), dispatchLua(r, c, "EVAL", "local t={}; for i=1,20 do t={t} end; local u=cmsgpack.unpack(cmsgpack.pack(t)); local d=0; while type(u)=='table' do u=u[1]; d=d+1 end; return d", "0"))
	require.Equal(t, bulk("bob"), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack(cmsgpack.pack({name='bob'})).name", "0"))
	require.Equal(t, intVal(3), dispatchLua(r, c, "EVAL", "local t=cmsgpack.unpack(cmsgpack.pack({1,2,x=3})); return t.x", "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", "local o=cmsgpack.unpack_limit(cmsgpack.pack(7),0,0); return o", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "local o,a=cmsgpack.unpack_limit(cmsgpack.pack(1)..cmsgpack.pack(2),0,0); return a", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack_limit(cmsgpack.pack(1000)..cmsgpack.pack(2),0,1)", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "return cmsgpack.unpack_limit(cmsgpack.pack(1)..cmsgpack.pack(2)..cmsgpack.pack(3),0,2)", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "local n=select('#',cmsgpack.unpack_limit(cmsgpack.pack(1)..cmsgpack.pack(2)..cmsgpack.pack(3),0,2)); return n", "0"))
}
