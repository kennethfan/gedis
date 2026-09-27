package acl

import "testing"

func TestSetUserCommandOrderMatters(t *testing.T) {
	st := NewStore()
	if err := st.SetUser("alice", "on", ">secret", "+@all", "-GET"); err != nil {
		t.Fatal(err)
	}
	if !st.Authenticate("alice", "secret") {
		t.Fatal("auth should pass")
	}
	if !st.CanRun("alice", "SET") {
		t.Fatal("SET should be allowed")
	}
	if st.CanRun("alice", "GET") {
		t.Fatal("GET should be denied: -GET after +@all")
	}
	if err := st.SetUser("bob", "on", ">pw", "-GET", "+@all"); err != nil {
		t.Fatal(err)
	}
	if !st.CanRun("bob", "GET") {
		t.Fatal("GET should be allowed: +@all after -GET")
	}
}
