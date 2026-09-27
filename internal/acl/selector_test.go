package acl

import "testing"

func TestSelectorOr(t *testing.T) {
	st := NewStore()
	if err := st.SetUser("alice", "on", ">pw", "-@all", "(", "+GET", "~cache:*", ")"); err != nil {
		t.Fatal(err)
	}
	if err := st.Check("alice", "GET", []string{"cache:a"}, nil); err != nil {
		t.Fatalf("selector should allow: %v", err)
	}
	if err := st.Check("alice", "GET", []string{"other:a"}, nil); err == nil {
		t.Fatal("outside selector should deny")
	}
	if err := st.Check("alice", "SET", []string{"cache:a"}, nil); err == nil {
		t.Fatal("SET not in selector should deny")
	}
}
