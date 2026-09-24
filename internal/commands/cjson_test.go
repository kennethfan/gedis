package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// WM: encode 基本映射（引号/数字14位/bool/空表/数组/unicode原样/转义）
func Test_Lua_when_CjsonEncode(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk(`"hello"`), dispatchLua(r, c, "EVAL", "return cjson.encode('hello')", "0"))
	require.Equal(t, bulk(`42`), dispatchLua(r, c, "EVAL", "return cjson.encode(42)", "0"))
	require.Equal(t, bulk(`true`), dispatchLua(r, c, "EVAL", "return cjson.encode(true)", "0"))
	require.Equal(t, bulk(`{}`), dispatchLua(r, c, "EVAL", "return cjson.encode({})", "0"))
	require.Equal(t, bulk(`[1,2,3]`), dispatchLua(r, c, "EVAL", "return cjson.encode({1,2,3})", "0"))
	require.Equal(t, bulk(`0.33333333333333`),
		dispatchLua(r, c, "EVAL", "return cjson.encode(1/3)", "0"))
	require.Equal(t, bulk(`9.007199254741e+15`),
		dispatchLua(r, c, "EVAL", "return cjson.encode(9007199254740993)", "0"))
	require.Equal(t, bulk(`"héllo"`),
		dispatchLua(r, c, "EVAL", "return cjson.encode('héllo')", "0"))
	require.Equal(t, bulk(`"a\nb"`),
		dispatchLua(r, c, "EVAL", `return cjson.encode('a\nb')`, "0"))
}

// WM: encode 容器语义（sparse洞→null/混合键→对象数字键字符串化/嵌套）
func Test_Lua_when_CjsonEncodeTables(t *testing.T) {
	r, c := openLuaSetup(t)
	bulk := func(s string) protocol.Value { return protocol.BulkOf(s) }
	require.Equal(t, bulk(`[1,null,3]`),
		dispatchLua(r, c, "EVAL", "local t={1,2,3} t[2]=nil return cjson.encode(t)", "0"))
	require.Equal(t, bulk(`{"a":1}`),
		dispatchLua(r, c, "EVAL", "return cjson.encode({a=1})", "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", "local s=cjson.encode({[2]='x',a='y'}) return (s:sub(1,1)=='{' and s:find('\"2\":\"x\"') ~= nil and s:find('\"a\":\"y\"') ~= nil) and 1 or 0", "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", "local s=cjson.encode({1,2,a=3}) return (s:find('\"a\":3') ~= nil and s:find('\"1\":1') ~= nil) and 1 or 0", "0"))
	require.Equal(t, bulk(`{"a":[1,true,"x"]}`),
		dispatchLua(r, c, "EVAL", "return cjson.encode({a={1,true,'x'}})", "0"))
}

// WM: encode 错误文案逐字（NaN/Inf/bool键/function值/无参）
func Test_Lua_when_CjsonEncodeErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errOf := func(script string) protocol.Value {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind)
		return got
	}
	sha := func(script string) string { return sha1Hex(script) }
	s := "return cjson.encode(0/0)"
	require.Equal(t, "ERR user_script:1: Cannot serialise number: must not be NaN or Inf script: "+sha(s)+", on @user_script:1.", errOf(s).S)
	s = "return cjson.encode(1/0)"
	require.Equal(t, "ERR user_script:1: Cannot serialise number: must not be NaN or Inf script: "+sha(s)+", on @user_script:1.", errOf(s).S)
	s = "return cjson.encode({[true]=1})"
	require.Equal(t, "ERR user_script:1: Cannot serialise boolean: table key must be a number or string script: "+sha(s)+", on @user_script:1.", errOf(s).S)
	s = "return cjson.encode({tostring})"
	require.Equal(t, "ERR user_script:1: Cannot serialise function: type not supported script: "+sha(s)+", on @user_script:1.", errOf(s).S)
	s = "return cjson.encode()"
	require.Equal(t, "ERR user_script:1: bad argument #1 to 'encode' (expected 1 argument) script: "+sha(s)+", on @user_script:1.", errOf(s).S)
}

// WM: decode 往返（标量/对象/数组/嵌套null/重复键后赢/数字转字符串）
func Test_Lua_when_CjsonDecode(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.BulkOf("hi"),
		dispatchLua(r, c, "EVAL", `return cjson.decode('"hi"')`, "0"))
	require.Equal(t, intVal(42),
		dispatchLua(r, c, "EVAL", `return cjson.decode('42')`, "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", `return cjson.decode('true')`, "0"))
	require.Equal(t, intVal(8),
		dispatchLua(r, c, "EVAL", `local t=cjson.decode('{"a":[1,2],"b":null}') return t.a[2]+6`, "0"))
	require.Equal(t, protocol.BulkOf("userdata"),
		dispatchLua(r, c, "EVAL", `return type(cjson.decode('null'))`, "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchLua(r, c, "EVAL", `return cjson.decode('null')`, "0"))
	require.Equal(t, intVal(2),
		dispatchLua(r, c, "EVAL", `local t=cjson.decode('{"a":1,"a":2}') return t.a`, "0"))
	require.Equal(t, intVal(42),
		dispatchLua(r, c, "EVAL", `return cjson.decode(42)`, "0"))
	require.Equal(t, protocol.BulkOf("中"),
		dispatchLua(r, c, "EVAL", `return cjson.decode('"\\u4e2d"')`, "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", `local s=cjson.encode(cjson.decode('{"x":[1,null,"a"]}')) return s=='{"x":[1,null,"a"]}' and 1 or 0`, "0"))
}

// WM: decode 错误文案逐字（token/字符号/框架）
func Test_Lua_when_CjsonDecodeErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errMsg := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind, script)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	cases := []struct{ script, msg string }{
		{`return cjson.decode('x')`, "Expected value but found invalid token at character 1"},
		{`return cjson.decode('')`, "Expected value but found T_END at character 1"},
		{`return cjson.decode('{"a":1')`, "Expected comma or object end but found T_END at character 7"},
		{`return cjson.decode('{"a" 1}')`, "Expected colon but found T_NUMBER at character 6"},
		{`return cjson.decode('[1 2]')`, "Expected comma or array end but found T_NUMBER at character 4"},
		{`return cjson.decode('12x')`, "Expected the end but found invalid token at character 3"},
		{`return cjson.decode('"abc')`, "Expected value but found unexpected end of string at character 5"},
		{`return cjson.decode('"a\\qb"')`, "Expected value but found invalid escape code at character 3"},
		{`return cjson.decode('"\\u12xz"')`, "Expected value but found invalid unicode escape code at character 2"},
		{`return cjson.decode('nul')`, "Expected value but found invalid token at character 1"},
		{`return cjson.decode('-x')`, "Expected value but found invalid number at character 1"},
		{`return cjson.decode()`, "bad argument #1 to 'decode' (expected 1 argument)"},
		{`return cjson.decode(true)`, "bad argument #1 to 'decode' (string expected, got boolean)"},
	}
	for _, tc := range cases {
		require.Equal(t, "ERR user_script:1: "+tc.msg+" script: "+sha(tc.script)+", on @user_script:1.", errMsg(tc.script), tc.script)
	}
}

// WM: null 单例（type/encode/==/decode恒等）
func Test_Lua_when_CjsonNull(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.BulkOf("userdata"),
		dispatchLua(r, c, "EVAL", "return type(cjson.null)", "0"))
	require.Equal(t, protocol.BulkOf("null"),
		dispatchLua(r, c, "EVAL", "return cjson.encode(cjson.null)", "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", "return (cjson.decode('null') == cjson.null) and 1 or 0", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchLua(r, c, "EVAL", "return cjson.null", "0"))
}

// WM: new() 实例（编解码同行为/null共享）
func Test_Lua_when_CjsonNew(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.BulkOf(`[1,2]`),
		dispatchLua(r, c, "EVAL", "local j=cjson.new() return j.encode({1,2})", "0"))
	require.Equal(t, intVal(1),
		dispatchLua(r, c, "EVAL", "local j=cjson.new() return (j.null == cjson.null) and 1 or 0", "0"))
	require.Equal(t, protocol.BulkOf("cjson"),
		dispatchLua(r, c, "EVAL", "return cjson._NAME", "0"))
	require.Equal(t, protocol.BulkOf("2.1.0"),
		dispatchLua(r, c, "EVAL", "return cjson._VERSION", "0"))
}

// WM: encode(nil) 回 Lua nil；decode('{}') 编回 '{}'
func Test_Lua_when_CjsonEdge(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchLua(r, c, "EVAL", "return cjson.encode(nil)", "0"))
	require.Equal(t, protocol.BulkOf("{}"),
		dispatchLua(r, c, "EVAL", "return cjson.encode(cjson.decode('{}'))", "0"))
	require.Equal(t, protocol.BulkOf(`"a/b"`),
		dispatchLua(r, c, "EVAL", `return cjson.encode('a/b')`, "0"))
	require.Equal(t, protocol.BulkOf("a/b"),
		dispatchLua(r, c, "EVAL", `return cjson.decode('"a\\/b"')`, "0"))
	require.Equal(t, protocol.BulkOf("Expected value but found invalid token at character 1"),
		dispatchLua(r, c, "EVAL", `local ok, r = pcall(cjson.decode, 'x') return r`, "0"))
}

// WM: token 命名（括号点名/布尔前缀/数字尝试）与 + 号数字（探针逐字）
func Test_Lua_when_CjsonTokenNames(t *testing.T) {
	r, c := openLuaSetup(t)
	errMsg := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind, script)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	cases := []struct{ script, msg string }{
		{`return cjson.decode('[1,]')`, "Expected value but found T_ARR_END at character 4"},
		{`return cjson.decode('[}]')`, "Expected value but found T_OBJ_END at character 2"},
		{`return cjson.decode(']')`, "Expected value but found T_ARR_END at character 1"},
		{`return cjson.decode('}')`, "Expected value but found T_OBJ_END at character 1"},
		{`return cjson.decode('[nulx]')`, "Expected value but found invalid token at character 2"},
		{`return cjson.decode('[.5]')`, "Expected value but found invalid token at character 2"},
		{`return cjson.decode('[+.]')`, "Expected value but found invalid number at character 2"},
		{`return cjson.decode('[-]')`, "Expected value but found invalid number at character 2"},
		{`return cjson.decode('{"a":1,}')`, "Expected object key string but found T_OBJ_END at character 8"},
		{`return cjson.decode('{"a":1,]')`, "Expected object key string but found T_ARR_END at character 8"},
		{`return cjson.decode('[1}]')`, "Expected comma or array end but found T_OBJ_END at character 3"},
		{`return cjson.decode('[1 truex]')`, "Expected comma or array end but found T_BOOLEAN at character 4"},
		{`return cjson.decode('[1 tru]')`, "Expected comma or array end but found invalid token at character 4"},
		{`return cjson.decode('[1 null]')`, "Expected comma or array end but found T_NULL at character 4"},
		{`return cjson.decode('[1 nul]')`, "Expected comma or array end but found invalid token at character 4"},
		{`return cjson.decode('[1 -]')`, "Expected comma or array end but found invalid number at character 4"},
		{`return cjson.decode('[1 -x]')`, "Expected comma or array end but found invalid number at character 4"},
		{`return cjson.decode('[1 +]')`, "Expected comma or array end but found invalid number at character 4"},
		{`return cjson.decode('[1 12x]')`, "Expected comma or array end but found T_NUMBER at character 4"},
		{`return cjson.decode('[1 +2]')`, "Expected comma or array end but found T_NUMBER at character 4"},
		{`return cjson.decode('{"a" null}')`, "Expected colon but found T_NULL at character 6"},
		{`return cjson.decode('{"a" tru}')`, "Expected colon but found invalid token at character 6"},
		{`return cjson.decode('{"a" 12x}')`, "Expected colon but found T_NUMBER at character 6"},
		{`return cjson.decode('1 2')`, "Expected the end but found T_NUMBER at character 3"},
		{`return cjson.decode('1 x')`, "Expected the end but found invalid token at character 3"},
	}
	for _, tc := range cases {
		require.Equal(t, "ERR user_script:1: "+tc.msg+" script: "+sha(tc.script)+", on @user_script:1.", errMsg(tc.script), tc.script)
	}
	require.Equal(t, intVal(2),
		dispatchLua(r, c, "EVAL", `return cjson.decode('[+2]')[1]`, "0"))
	require.Equal(t, intVal(0),
		dispatchLua(r, c, "EVAL", `return cjson.decode('[+.5]')[1]`, "0"))
	require.Equal(t, intVal(100),
		dispatchLua(r, c, "EVAL", `return cjson.decode('[1.e2]')[1]`, "0"))
	require.Equal(t, intVal(50),
		dispatchLua(r, c, "EVAL", `return cjson.decode('[+.5e2]')[1]`, "0"))
	require.Equal(t, intVal(2),
		dispatchLua(r, c, "EVAL", `return cjson.decode('+2')`, "0"))
}

// WM: 配置函数默认值/getter/setter/跨 EVAL 持久/new() 隔离
func Test_Lua_when_CjsonSettings(t *testing.T) {
	r, c := openLuaSetup(t)
	require.Equal(t, intVal(1000), dispatchLua(r, c, "EVAL", `return cjson.encode_max_depth()`, "0"))
	require.Equal(t, intVal(1000), dispatchLua(r, c, "EVAL", `return cjson.decode_max_depth()`, "0"))
	require.Equal(t, intVal(14), dispatchLua(r, c, "EVAL", `return cjson.encode_number_precision()`, "0"))
	require.Equal(t, protocol.BulkOf("false/2/10"), dispatchLua(r, c, "EVAL",
		`local a,b,cc=cjson.encode_sparse_array(); return tostring(a)..'/'..b..'/'..cc`, "0"))
	require.Equal(t, intVal(1000), dispatchLua(r, c, "EVAL", `return cjson.encode_max_depth(nil)`, "0"))

	// setter 回设定值，且跨 EVAL 持久（同一 Registry）。
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", `return cjson.encode_max_depth(7)`, "0"))
	require.Equal(t, intVal(7), dispatchLua(r, c, "EVAL", `return cjson.encode_max_depth()`, "0"))
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", `return cjson.encode_sparse_array(true)`, "0"))
	require.Equal(t, protocol.BulkOf("true/2/10"), dispatchLua(r, c, "EVAL",
		`local a,b,cc=cjson.encode_sparse_array(); return tostring(a)..'/'..b..'/'..cc`, "0"))

	// new() 实例恒为出厂默认（不继承全局修改）。
	require.Equal(t, intVal(1000), dispatchLua(r, c, "EVAL", `return cjson.new().encode_max_depth()`, "0"))
}

// WM: 配置函数错误文案（与真机逐字，sha 外层组装）
func Test_Lua_when_CjsonSettingsErrors(t *testing.T) {
	r, c := openLuaSetup(t)
	errMsg := func(script string) string {
		got := dispatchLua(r, c, "EVAL", script, "0")
		require.Equal(t, protocol.KindError, got.Kind, script)
		return got.S
	}
	sha := func(script string) string { return sha1Hex(script) }
	cases := []struct{ script, msg string }{
		{`return cjson.encode_max_depth(0)`, "bad argument #1 to 'encode_max_depth' (expected integer between 1 and 2147483647)"},
		{`return cjson.encode_max_depth('x')`, "bad argument #1 to 'encode_max_depth' (number expected, got string)"},
		{`return cjson.encode_max_depth(true)`, "bad argument #1 to 'encode_max_depth' (number expected, got boolean)"},
		{`return cjson.encode_max_depth(5,6)`, "bad argument #2 to 'encode_max_depth' (found too many arguments)"},
		{`return cjson.encode_number_precision(15)`, "bad argument #1 to 'encode_number_precision' (expected integer between 1 and 14)"},
		{`return cjson.encode_number_precision(0)`, "bad argument #1 to 'encode_number_precision' (expected integer between 1 and 14)"},
		{`return cjson.encode_sparse_array('x')`, "invalid option 'x'"},
		{`return cjson.encode_sparse_array(1)`, "invalid option '1'"},
		{`return cjson.encode_sparse_array({})`, "bad argument #1 to 'encode_sparse_array' (string expected, got table)"},
		{`return cjson.encode_sparse_array(true,'x')`, "bad argument #2 to 'encode_sparse_array' (number expected, got string)"},
		{`return cjson.encode_sparse_array(true,2,-1)`, "bad argument #1 to 'encode_sparse_array' (expected integer between 0 and 2147483647)"},
		{`return cjson.encode_sparse_array(true,2,10,4)`, "bad argument #4 to 'encode_sparse_array' (found too many arguments)"},
	}
	for _, tc := range cases {
		require.Equal(t, "ERR user_script:1: "+tc.msg+" script: "+sha(tc.script)+", on @user_script:1.", errMsg(tc.script), tc.script)
	}
}

// WM: 配置实际生效（精度/depth 稀疏门/decode 深度/sparse 开关）
func Test_Lua_when_CjsonSettingsEffect(t *testing.T) {
	r, c := openLuaSetup(t)
	// 精度：默认 14，置 4 后 1/3 出 4 位。
	require.Equal(t, protocol.BulkOf("0.33333333333333"),
		dispatchLua(r, c, "EVAL", `return cjson.encode(1/3)`, "0"))
	require.Equal(t, intVal(4), dispatchLua(r, c, "EVAL", `return cjson.encode_number_precision(4)`, "0"))
	require.Equal(t, protocol.BulkOf("0.3333"),
		dispatchLua(r, c, "EVAL", `return cjson.encode(1/3)`, "0"))

	// encode 深度门：max1 下 {{}} 报 nesting(2)。
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", `return cjson.encode_max_depth(1)`, "0"))
	got := dispatchLua(r, c, "EVAL", `return cjson.encode({{}})`, "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Cannot serialise, excessive nesting (2)")

	// decode 深度门：decmax1 下 {"a":{}} 报 (2)@6。
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", `return cjson.decode_max_depth(1)`, "0"))
	got = dispatchLua(r, c, "EVAL", `return cjson.decode('{"a":{}}')`, "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Found too many nested data structures (2) at character 6")
}

// WM: sparse 模型（拒绝/convert 回退/D 例外/ratio0 禁用）
func Test_Lua_when_CjsonSparseModel(t *testing.T) {
	r, c := openLuaSetup(t)
	m100 := `{[1]=1,[100]=2}`
	// 默认 (false,2,10)：m100 拒绝。
	got := dispatchLua(r, c, "EVAL", `return cjson.encode(`+m100+`)`, "0")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "Cannot serialise table: excessively sparse array")
	// convert=true：回退对象（经 decode 断言值，避开键序）。
	require.Equal(t, intVal(1), dispatchLua(r, c, "EVAL", `return cjson.encode_sparse_array(true)`, "0"))
	require.Equal(t, intVal(2), dispatchLua(r, c, "EVAL",
		`return cjson.decode(cjson.encode(`+m100+`))['100']`, "0"))
	// razor-edge：depth≤5 且 max==10 → 回退对象。
	r2, c2 := openLuaSetup(t)
	require.Equal(t, intVal(5), dispatchLua(r2, c2, "EVAL", `return cjson.encode_max_depth(5)`, "0"))
	require.Equal(t, intVal(1), dispatchLua(r2, c2, "EVAL",
		`return cjson.decode(cjson.encode(`+m100+`))['1']`, "0"))
	// depth6 → 拒绝。
	require.Equal(t, intVal(6), dispatchLua(r2, c2, "EVAL", `return cjson.encode_max_depth(6)`, "0"))
	got = dispatchLua(r2, c2, "EVAL", `return cjson.encode(`+m100+`)`, "0")
	require.Equal(t, protocol.KindError, got.Kind)
	// ratio0 禁用 sparse：m100 照填数组（setter 回 false→nil）。
	r3, c3 := openLuaSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString}, dispatchLua(r3, c3, "EVAL", `return cjson.encode_sparse_array(false,0,10)`, "0"))
	require.Equal(t, intVal(2), dispatchLua(r3, c3, "EVAL",
		`return cjson.decode(cjson.encode(`+m100+`))[100]`, "0"))
}
