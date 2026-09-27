package commands

import (
	"context"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

func openDryRun(t testing.TB) (*network.Router, *acl.Store) {
	t.Helper()
	st := acl.NewStore()
	if err := st.SetUser("alice", "on", ">pw", "+@all", "~app:*"); err != nil {
		t.Fatal(err)
	}
	acl.RegisterMeta(acl.Meta{Name: "SET", Category: "string", Keys: acl.KeySpec{First: 0, Last: 0}})
	r := network.NewRouter()
	reg := RegisterAuth(r, st)
	RegisterACL(r, st, reg, nil)
	r.SetAuthorizer(st)
	return r, st
}

func dryrun(t testing.TB, r *network.Router, args ...string) protocol.Value {
	t.Helper()
	elems := []protocol.Value{protocol.BulkOf("ACL"), protocol.BulkOf("DRYRUN")}
	for _, a := range args {
		elems = append(elems, protocol.BulkOf(a))
	}
	return r.Dispatch(network.ContextWithUser(context.Background(), "alice"), protocol.ArrayOf(elems...))
}

func TestDryRunAllowAndDeny(t *testing.T) {
	r, st := openDryRun(t)
	v := dryrun(t, r, "alice", "SET", "app:a", "1")
	if v.Kind != protocol.KindSimpleString || v.S != "OK" {
		t.Fatalf("DRYRUN allow should be +OK, got %+v", v)
	}
	v = dryrun(t, r, "alice", "SET", "sys:a", "1")
	if v.Kind != protocol.KindBulkString || string(v.Bulk) != "User alice has no permissions to access the 'sys:a' key" {
		t.Fatalf("DRYRUN deny should be per-key reason bulk, got %+v", v)
	}
	if len(st.AclLog()) != 0 {
		t.Fatalf("DRYRUN must not log, got %d entries", len(st.AclLog()))
	}
}

func TestLogRecordsDenialAndReset(t *testing.T) {
	r, st := openDryRun(t)
	acl.RegisterMeta(acl.Meta{Name: "SET", Category: "string", Keys: acl.KeySpec{First: 0, Last: 0}})
	ctx := network.ContextWithUser(context.Background(), "alice")
	v := r.Dispatch(ctx, protocol.ArrayOf(protocol.BulkOf("SET"), protocol.BulkOf("sys:a"), protocol.BulkOf("1")))
	if v.Kind != protocol.KindError || !strings.HasPrefix(v.S, "NOPERM") {
		t.Fatalf("key denial should be NOPERM, got %+v", v)
	}
	if len(st.AclLog()) != 1 {
		t.Fatalf("denial must log one entry, got %d", len(st.AclLog()))
	}
	v = r.Dispatch(ctx, protocol.ArrayOf(protocol.BulkOf("ACL"), protocol.BulkOf("LOG"), protocol.BulkOf("RESET")))
	if v.Kind != protocol.KindSimpleString || v.S != "OK" {
		t.Fatalf("LOG RESET should be +OK, got %+v", v)
	}
	if len(st.AclLog()) != 0 {
		t.Fatalf("RESET must clear log, got %d entries", len(st.AclLog()))
	}
}
