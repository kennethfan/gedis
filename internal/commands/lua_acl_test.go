package commands

import (
	"context"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func TestLuaRespectsACL(t *testing.T) {
	r, _ := openLuaSetup(t)
	st := acl.NewStore()
	if err := st.SetUser("alice", "on", ">pw", "+@all", "~app:*"); err != nil {
		t.Fatal(err)
	}
	r.SetAuthorizer(st)
	reg := RegisterAuth(r, st)
	RegisterACL(r, st, reg, nil)
	ctx := network.ContextWithUser(context.Background(), "alice")
	v := r.Dispatch(ctx, protocol.ArrayOf(
		protocol.BulkOf("EVAL"),
		protocol.BulkOf(`return redis.call("SET","sys:x","1")`),
		protocol.BulkOf("0"),
	))
	if v.Kind != protocol.KindError || !strings.Contains(v.S, "ACL failure in script: No permissions to access a key") {
		t.Fatalf("want script ACL-failure abort, got %+v", v)
	}
}
