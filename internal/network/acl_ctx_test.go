package network

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/protocol"
)

// 真机 7.2.6 语义：default nopass 时未认证连接按 default 身份放行；
// 仅 default 要求认证（设口令/off）时未认证连接收 NOAUTH。
func TestDispatchAuthGate(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)
	r.Register("GET", func(_ context.Context, _ []protocol.Value) protocol.Value {
		return protocol.Value{Kind: protocol.KindSimpleString, S: "v"}
	})
	st := acl.NewStore()
	if err := st.SetUser("alice", "on", ">pw", "+@all", "-GET"); err != nil {
		t.Fatal(err)
	}
	r.SetAuthorizer(st)

	// default nopass：未认证 PING 按 default 放行。
	reply := r.Dispatch(context.Background(), protocol.ArrayOf(protocol.BulkOf("PING")))
	if reply.Kind != protocol.KindSimpleString || reply.S != "PONG" {
		t.Fatalf("want PONG, got %+v", reply)
	}
	// alice 认证身份：GET 越权 → NOPERM 带用户名（真机同形）。
	reply = r.Dispatch(ContextWithUser(context.Background(), "alice"), protocol.ArrayOf(protocol.BulkOf("GET")))
	if reply.Kind != protocol.KindError || reply.S != "NOPERM User alice has no permissions to run the 'get' command" {
		t.Fatalf("want NOPERM User alice, got %+v", reply)
	}
	// default 改口令后：未认证 PING → NOAUTH。
	if err := st.SetUser("default", "on", ">secret", "+@all"); err != nil {
		t.Fatal(err)
	}
	reply = r.Dispatch(context.Background(), protocol.ArrayOf(protocol.BulkOf("PING")))
	if reply.Kind != protocol.KindError || reply.S != "NOAUTH Authentication required." {
		t.Fatalf("want NOAUTH, got %+v", reply)
	}
	// 认证为 default 后放行。
	reply = r.Dispatch(ContextWithUser(context.Background(), "default"), protocol.ArrayOf(protocol.BulkOf("PING")))
	if reply.Kind != protocol.KindSimpleString || reply.S != "PONG" {
		t.Fatalf("want PONG, got %+v", reply)
	}
}
