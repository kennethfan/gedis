package acl

import "testing"

func TestCheckKeyPattern(t *testing.T) {
	st := NewStore()
	if err := st.SetUser("alice", "on", ">pw", "+@all", "~app:*"); err != nil {
		t.Fatal(err)
	}
	if err := st.Check("alice", "SET", []string{"app:a"}, nil); err != nil {
		t.Fatalf("app:a should pass: %v", err)
	}
	if err := st.Check("alice", "SET", []string{"sys:a"}, nil); err == nil ||
		err.Error() != "NOPERM No permissions to access a key" {
		t.Fatalf("sys:a should NOPERM-keys, got %v", err)
	}
	if err := st.Check("alice", "GET", nil, nil); err != nil {
		t.Fatalf("no-key cmd should pass: %v", err)
	}
	if err := st.SetUser("ch", "on", ">pw", "+@all", "~app:*", "&news:*"); err != nil {
		t.Fatal(err)
	}
	if err := st.Check("ch", "PUBLISH", nil, []string{"sports"}); err == nil ||
		err.Error() != "NOPERM No permissions to access a channel" {
		t.Fatalf("sports should NOPERM-channel, got %v", err)
	}
	if err := st.Check("ch", "PUBLISH", nil, []string{"news:x"}); err != nil {
		t.Fatalf("news:x should pass: %v", err)
	}
}
