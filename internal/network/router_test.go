package network

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 空 Router，注册 PING handler
// When: Dispatch PING / ping（大小写不敏感）
// Then: 返回 PONG
func Test_Router_when_DispatchPing(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)

	for _, name := range []string{"PING", "ping", "Ping"} {
		got := r.Dispatch(context.Background(), protocol.ArrayOf(protocol.BulkOf(name)))
		require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "PONG"}, got)
	}
}

// Given: 注册过的 Router
// When: Dispatch 未知命令 / 空数组 / 非数组
// Then: 返回 Error，不 panic
func Test_Router_when_Unknown(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)

	got := r.Dispatch(context.Background(), protocol.ArrayOf(protocol.BulkOf("NOPE")))
	require.Equal(t, protocol.KindError, got.Kind)

	got = r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray})
	require.Equal(t, protocol.KindError, got.Kind)

	got = r.Dispatch(context.Background(), protocol.BulkOf("PING"))
	require.Equal(t, protocol.KindError, got.Kind)
}

// Given: 注册 PING 带参数
// When: Dispatch PING <msg>
// Then: 原样返回 msg（Redis 语义）
func Test_Router_when_PingWithArg(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)

	got := r.Dispatch(context.Background(), protocol.ArrayOf(
		protocol.BulkOf("PING"), protocol.BulkOf("hello")))
	require.Equal(t, protocol.BulkOf("hello"), got)
}
