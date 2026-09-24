package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WM: struct pack/unpack 端序与往返（对标真机 7.2.6 struct 库：>/</! 前缀、变长 i/I/c）
func Test_Lua_when_StructRoundtrip(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk("\x00\x00\x00\x01"), dispatchLua(r, c, "EVAL", "return struct.pack('>I4', 1)", "0"))
	require.Equal(t, bulk("\x01\x00\x00\x00"), dispatchLua(r, c, "EVAL", "return struct.pack('<I4', 1)", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "local s=struct.pack('<I4>I4',1,2); return string.byte(s,1)", "0"))
	require.Equal(t, intVal(0), dispatchLua(r, c, "EVAL", "local s=struct.pack('<I4>I4',1,2); return string.byte(s,5)", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "return #(struct.pack('!4bI', 65, 1))", "0"))
	require.Equal(t, intVal(0x01020304), dispatchLua(r, c, "EVAL", "return struct.unpack('>I4', struct.pack('>I4', 0x01020304))", "0"))
	require.Equal(t, intVal(4), dispatchLua(r, c, "EVAL", "return struct.size('>I4')", "0"))
	require.Equal(t, intVal(6), dispatchLua(r, c, "EVAL", "return struct.size('>I4I2')", "0"))
	require.Equal(t, bulk("hello\x00"), dispatchLua(r, c, "EVAL", "return struct.pack('>s', 'hello')", "0"))
	require.Equal(t, bulk("hi"), dispatchLua(r, c, "EVAL", "return struct.unpack('s', struct.pack('s','hi'))", "0"))
	require.Equal(t, bulk("abc"), dispatchLua(r, c, "EVAL", "return struct.pack('>c3', 'abcdef')", "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", "return struct.unpack('>f', struct.pack('>f', 1.5)) == 1.5 and 1 or 0", "0"))
	require.Equal(t, intVal(-2), dispatchLua(r, c, "EVAL", "return struct.unpack('>i4', struct.pack('>i4', -2))", "0"))
	require.Equal(t, intVal(65535), dispatchLua(r, c, "EVAL", "return struct.unpack('>I2', struct.pack('>I2', -1))", "0"))
}

// WM: struct unpack 多返回（值... + 下个位置 1-based；c0 吞掉前一个数值型长度）
func Test_Lua_when_StructUnpackPos(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, intVal(3), dispatchLua(r, c, "EVAL", "local a,b=struct.unpack('>I2','ab'); return b", "0"))
	require.Equal(t, intVal(5), dispatchLua(r, c, "EVAL", "local a,b,c=struct.unpack('>I2I2','abcd'); return c", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "local a,b=struct.unpack('>x','a'); return a", "0"))
	require.Equal(t, bulk("abc"), dispatchLua(r, c, "EVAL", "local a,b=struct.unpack('>Bc0', string.char(3)..'abcdef'); return a", "0"))
	require.Equal(t, intVal(5), dispatchLua(r, c, "EVAL", "local a,b=struct.unpack('>Bc0', string.char(3)..'abcdef'); return b", "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL", "local a,b,c=struct.unpack('>BBc0', string.char(2)..string.char(3)..'abcdef'); return a", "0"))
	require.Equal(t, bulk("abc"), dispatchLua(r, c, "EVAL", "local a,b,c=struct.unpack('>BBc0', string.char(2)..string.char(3)..'abcdef'); return b", "0"))
	require.Equal(t, intVal(6), dispatchLua(r, c, "EVAL", "local a,b,c=struct.unpack('>BBc0', string.char(2)..string.char(3)..'abcdef'); return c", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "return struct.size('!4bI')", "0"))
	require.Equal(t, intVal(8), dispatchLua(r, c, "EVAL", "return struct.size('!4c2I')", "0"))
}

// WM: struct 错误文案逐字（invalid 选项 #1、缺参类型 #N、too short、c0/对齐无前缀）
func Test_Lua_when_StructErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errOf := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	s := "return struct.pack()"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'pack' (string expected, got no value) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.pack('>X', 1)"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'pack' (invalid format option 'X') script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.pack('>I4')"
	require.Equal(t, "ERR user_script:1: bad argument #2 to 'pack' (number expected, got nil) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.pack('>I4', 'x')"
	require.Equal(t, "ERR user_script:1: bad argument #2 to 'pack' (number expected, got string) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.pack('c3', 'ab')"
	require.Equal(t, "ERR user_script:1: bad argument #3 to 'pack' (string too short) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.unpack('>I4', 'ab')"
	require.Equal(t, "ERR user_script:1: bad argument #2 to 'unpack' (data string too short) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.unpack('>I4','abcd',0)"
	require.Equal(t, "ERR user_script:1: bad argument #3 to 'unpack' (offset must be 1 or greater) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.unpack('>s','hi')"
	require.Equal(t, "ERR user_script:1: unfinished string in data script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.unpack('c0','abcdef',1)"
	require.Equal(t, "ERR user_script:1: format 'c0' needs a previous size script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.size('s')"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'size' (option 's' has no fixed size) script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "return struct.pack('!0I', 1)"
	require.Equal(t, "ERR user_script:1: alignment 0 is not a power of 2 script: "+sha(s)+", on @user_script:1.", errOf(s))
	s = "struct.pack=1"
	require.Equal(t, "ERR user_script:1: Attempt to modify a readonly table script: "+sha(s)+", on @user_script:1.", errOf(s))
}

// WM: pcall 错误形态（A式直调：无位前缀+函数名'?'；纯消息类无前缀；B'式有位+真名）
func Test_Lua_when_StructPcallForm(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk("bad argument #1 to '?' (string expected, got no value)"), dispatchLua(r, c, "EVAL", "local _,e=pcall(struct.pack); return e", "0"))
	require.Equal(t, bulk("unfinished string in data"), dispatchLua(r, c, "EVAL", "local _,e=pcall(struct.unpack,'>s','hi'); return e", "0"))
	require.Equal(t, bulk("alignment 0 is not a power of 2"), dispatchLua(r, c, "EVAL", "local _,e=pcall(struct.pack,'!0I',1); return e", "0"))
	require.Equal(t, bulk("user_script:1: bad argument #1 to 'pack' (invalid format option 'X')"), dispatchLua(r, c, "EVAL", "local _,e=pcall(function() return struct.pack('>X',1) end); return e", "0"))
}
