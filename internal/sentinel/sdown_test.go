package sentinel

import (
	"testing"
	"time"
)

func TestSDOWN_FirstFailure(t *testing.T) {
	r := NewRegistry([]NodeSpec{{Name: "m", MasterAddr: "127.0.0.1:6380", Slaves: []string{"127.0.0.1:6381"}, Quorum: 1}}, time.Second)
	r.RecordProbe("127.0.0.1:6380", false)
	if !r.IsSubjectivelyDown("127.0.0.1:6380") {
		t.Fatal("expect sdown after 1 failure")
	}
	r.RecordProbe("127.0.0.1:6380", true)
	if r.IsSubjectivelyDown("127.0.0.1:6380") {
		t.Fatal("success should clear sdown")
	}
	if r.IsDown("127.0.0.1:6380") {
		t.Fatal("success should clear down flag")
	}
}
