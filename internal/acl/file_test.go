package acl

import "testing"

func TestACLFileRoundTrip(t *testing.T) {
	st := NewStore()
	if err := st.SetUser("alice", "on", ">secret", "+@all", "-GET"); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/users.acl"
	if err := Save(path, st); err != nil {
		t.Fatal(err)
	}
	st2 := NewStore()
	if err := Load(path, st2); err != nil {
		t.Fatal(err)
	}
	if !st2.Authenticate("alice", "secret") || !st2.CanRun("alice", "SET") || st2.CanRun("alice", "GET") {
		t.Fatal("round-trip semantics changed")
	}
}
