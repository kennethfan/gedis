package commands

import (
	"context"
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func dispatchTxn(r *network.Router, conn net.Conn, args ...string) protocol.Value {
	ctx := network.ContextWithConn(context.Background(), conn)
	return r.Dispatch(ctx, cmd(args...))
}

func openTxnSetup(t testing.TB) (*network.Router, *TxnRegistry, net.Conn, net.Conn) {
	t.Helper()
	r, store := openTestSetup(t)
	RegisterList(r, store, nil)
	reg := RegisterTxn(r)
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
