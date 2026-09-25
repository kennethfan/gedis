package commands

import (
	"fmt"
	"strconv"

	lua "github.com/yuin/gopher-lua"
)

// libCallPos 复刻 luaL_where(1)：调用方为 Lua/main 帧时返回 "src:line: " 前缀，
// 直接被 pcall 调用（无 Lua 调用方）时返回空串。gopher 的 L.Where 不可用
// （直调时返回 "[G]:"），故直接读调用栈。
func libCallPos(L *lua.LState) string {
	st, ok := L.GetStack(1)
	if !ok {
		return ""
	}
	// GetInfo 原地填充 st（返回值恒为 LNil），what 取 "Sl"（S=帧类型，l=行号）。
	if _, err := L.GetInfo("Sl", st, lua.LNil); err != nil {
		return ""
	}
	if st.What != "Lua" && st.What != "main" {
		return ""
	}
	return st.Source + ":" + strconv.Itoa(st.CurrentLine) + ": "
}

// libArgError 抛出 bad argument 错误：有位时 "位+bad argument #N to '真名' (detail)"，
// 无位时 "bad argument #N to '?' (detail)"，对标 luaL_argerror 的 '?' 形态。
// errfunc/level 0 抛出，run() 未捕获路径零改动。
func libArgError(L *lua.LState, fname string, n int, detail string) {
	if pos := libCallPos(L); pos != "" {
		L.Error(lua.LString(fmt.Sprintf("%sbad argument #%d to '%s' (%s)", pos, n, fname, detail)), 0)
		return
	}
	L.Error(lua.LString(fmt.Sprintf("bad argument #%d to '?' (%s)", n, detail)), 0)
}

// libErrorf 抛出纯消息错误：有位时加位前缀，否则裸消息（对标 luaL_error）。
func libErrorf(L *lua.LState, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	L.Error(lua.LString(libCallPos(L)+msg), 0)
}
