package commands

import (
	"context"
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func dispatchTxn(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

func openTxnSetup(t testing.TB) (*network.Router, *TxnRegistry, net.Conn, net.Conn) {
	t.Helper()
	hub := replication.NewHub(1024)
	store := storage.NewWithOptions(t.TempDir(), storage.Options{Hub: hub})
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	RegisterStrings(r, store)
	RegisterList(r, store, nil)
	reg := RegisterTxn(r, hub)
	ca, _ := net.Pipe()
	cb, _ := net.Pipe()
	t.Cleanup(func() { ca.Close(); cb.Close() })
	return r, reg, ca, cb
}

// Given: 空库 + 新连接
// When: MULTI → SET k v（排队）→ EXEC
// Then: +OK / +QUEUED / [OK]，EXEC 后 GET k 得 v
func Test_Txn_when_MultiExecQueue(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchTxn(r, ca, "MULTI"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "QUEUED"},
		dispatchTxn(r, ca, "SET", "k", "v"))
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, "OK", got.Elems[0].S)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
}

// Given: 已 MULTI
// When: 再次 MULTI
// Then: 嵌套报错
func Test_Txn_when_NestedMulti(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "MULTI")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "MULTI calls can not be nested")
}

// Given: 未 MULTI
// When: EXEC / DISCARD
// Then: 各自报错
func Test_Txn_when_ExecDiscardWithoutMulti(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	require.Contains(t, dispatchTxn(r, ca, "EXEC").S, "EXEC without MULTI")
	require.Contains(t, dispatchTxn(r, ca, "DISCARD").S, "DISCARD without MULTI")
}

// Given: MULTI 后排队 SET
// When: DISCARD 后再 EXEC
// Then: DISCARD +OK；EXEC 报错；key 未写入
func Test_Txn_when_DiscardClearsQueue(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "v")
	require.Equal(t, "OK", dispatchTxn(r, ca, "DISCARD").S)
	require.Contains(t, dispatchTxn(r, ca, "EXEC").S, "EXEC without MULTI")
	got := dispatch(r, "GET", "k")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: MULTI 后排队未知命令
// When: EXEC
// Then: EXECABORT，队列未执行
func Test_Txn_when_QueueErrorThenExecAbort(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "NOSUCHCMD", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	got = dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "EXECABORT")
	require.Nil(t, dispatch(r, "GET", "x").Bulk)
}

// Given: MULTI 后无排队
// When: EXEC
// Then: 空数组
func Test_Txn_when_EmptyExec(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Empty(t, got.Elems)
}

// Given: 连接 A 已 MULTI
// When: 连接 B 直接 SET
// Then: B 不受影响立即执行；A 的队列对 B 不可见
func Test_Txn_when_SessionsIsolatedByConn(t *testing.T) {
	r, _, ca, cb := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "queued")
	require.Equal(t, "OK", dispatchTxn(r, cb, "SET", "k", "direct").S)
	require.Equal(t, protocol.BulkOf("direct"), dispatch(r, "GET", "k"))
	got := dispatchTxn(r, ca, "EXEC")
	require.Len(t, got.Elems, 1)
	require.Equal(t, protocol.BulkOf("queued"), dispatch(r, "GET", "k"))
}

// Given: MULTI 后排队参数个数错误的命令
// When: 排队期直接报错并污染会话，EXEC → EXECABORT
// Then: 队列未执行
func Test_Txn_when_ArityErrorAtQueueTime(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "SET", "onlykey")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments for 'set' command")
	got = dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "EXECABORT")
	require.Nil(t, dispatch(r, "GET", "onlykey").Bulk)
}

// Given: MULTI 后排队精确个数违规的命令（GET 带 2 参数）
// When: 排队期报错；DISCARD 后会话干净
// Then: 后续命令不受影响
func Test_Txn_when_ExactArityViolated(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "GET", "a", "b")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments for 'get' command")
	require.Equal(t, "OK", dispatchTxn(r, ca, "DISCARD").S)
	require.Equal(t, "OK", dispatchTxn(r, ca, "SET", "k", "v").S)
}

// Given: MULTI 后排队满足最小个数的命令（MSET 单键值对）
// When: EXEC
// Then: 正常放行执行
func Test_Txn_when_MinArityPasses(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	require.Equal(t, "QUEUED", dispatchTxn(r, ca, "MSET", "ak", "av").S)
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, "OK", got.Elems[0].S)
	require.Equal(t, protocol.BulkOf("av"), dispatch(r, "GET", "ak"))
}
// Given: MULTI 后排队对 string 键的 LPUSH（类型错误只能在回放期发现）
// When: EXEC
// Then: 错误落进结果数组对应位置，事务继续；后继命令照常执行
func Test_Txn_when_RuntimeErrorLandsInArray(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatch(r, "SET", "str", "v")
	dispatchTxn(r, ca, "MULTI")
	require.Equal(t, "QUEUED", dispatchTxn(r, ca, "LPUSH", "str", "x").S)
	dispatchTxn(r, ca, "SET", "k", "v")
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 2)
	require.Equal(t, protocol.KindError, got.Elems[0].Kind)
	require.Contains(t, got.Elems[0].S, "WRONGTYPE")
	require.Equal(t, "OK", got.Elems[1].S)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
}

// Given: MULTI 后连接断开（服务端清理）
// When: 同一连接重建会话后 EXEC
// Then: 旧队列已丢弃，EXEC 报错
func Test_Txn_when_ConnClosedDiscardsSession(t *testing.T) {
	r, reg, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "v")
	reg.ConnClosed(ca)
	require.Contains(t, dispatchTxn(r, ca, "EXEC").S, "EXEC without MULTI")
	require.Nil(t, dispatch(r, "GET", "k").Bulk)
}

// Given: WATCH k 后无人碰 k
// When: MULTI → SET → EXEC
// Then: 正常提交，返回单元素数组
func Test_Watch_when_NoTouchExecutes(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	require.Equal(t, "OK", dispatchTxn(r, ca, "WATCH", "k").S)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "v")
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Len(t, got.Elems, 1)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
}

// Given: WATCH k/lst 后另一连接改了 k 与 lst（string 走 s: 前缀，list 走 l: 前缀）
// When: MULTI → SET → EXEC
// Then: 回 NullArray（*-1），事务未执行
func Test_Watch_when_CrossConnTouchAborts(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	require.Equal(t, "OK", dispatchTxn(r, ca, "WATCH", "k", "lst").S)
	dispatch(r, "SET", "k", "other")
	dispatch(r, "RPUSH", "lst", "x")
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "mine")
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Nil(t, got.Elems)
	require.Equal(t, protocol.BulkOf("other"), dispatch(r, "GET", "k"))
}

// Given: WATCH 后被改，但中途 UNWATCH
// When: MULTI → SET → EXEC
// Then: 正常提交
func Test_Watch_when_UnwatchClears(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "WATCH", "k")
	dispatch(r, "SET", "k", "other")
	require.Equal(t, "OK", dispatchTxn(r, ca, "UNWATCH").S)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "mine")
	got := dispatchTxn(r, ca, "EXEC")
	require.Len(t, got.Elems, 1)
	require.Equal(t, protocol.BulkOf("mine"), dispatch(r, "GET", "k"))
}

// Given: MULTI 内 WATCH
// When: 报错后继续排队 → EXEC
// Then: WATCH 被拒但事务不污染，正常提交
func Test_Watch_when_InsideMultiRejectedWithoutDirty(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "WATCH", "k")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR WATCH inside MULTI is not allowed", got.S)
	dispatchTxn(r, ca, "SET", "a", "b")
	got = dispatchTxn(r, ca, "EXEC")
	require.Len(t, got.Elems, 1)
	require.Equal(t, "OK", got.Elems[0].S)
}

// Given: WATCH 后 DISCARD（清 watch）再重开事务
// When: 第二次 MULTI → SET → EXEC
// Then: 第一次改动不再导致中止，正常提交
func Test_Watch_when_DiscardClears(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "WATCH", "k")
	dispatch(r, "SET", "k", "other")
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "x")
	require.Equal(t, "OK", dispatchTxn(r, ca, "DISCARD").S)
	dispatchTxn(r, ca, "MULTI")
	dispatchTxn(r, ca, "SET", "k", "mine")
	got := dispatchTxn(r, ca, "EXEC")
	require.Len(t, got.Elems, 1)
	require.Equal(t, protocol.BulkOf("mine"), dispatch(r, "GET", "k"))
}

// Given: WATCH 后裸 EXEC 失败（无 MULTI）
// When: 他人改 k 后再 MULTI → EXEC
// Then: watch 保留，中止回 NullArray
func Test_Watch_when_FailedExecKeepsWatch(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	dispatchTxn(r, ca, "WATCH", "k")
	require.Contains(t, dispatchTxn(r, ca, "EXEC").S, "EXEC without MULTI")
	dispatch(r, "SET", "k", "other")
	dispatchTxn(r, ca, "MULTI")
	got := dispatchTxn(r, ca, "EXEC")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Nil(t, got.Elems)
}

// Given: 从未 WATCH
// When: UNWATCH
// Then: +OK
func Test_Watch_when_BareUnwatch(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	require.Equal(t, "OK", dispatchTxn(r, ca, "UNWATCH").S)
}

// Given: WATCH 无参 / UNWATCH 带参
// Then: arity 文案与真 Redis 一致（小写命令名）
func Test_Watch_when_ArityChecked(t *testing.T) {
	r, _, ca, _ := openTxnSetup(t)
	got := dispatchTxn(r, ca, "WATCH")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR wrong number of arguments for 'watch' command", got.S)
	got = dispatchTxn(r, ca, "UNWATCH", "x")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR wrong number of arguments for 'unwatch' command", got.S)
}
