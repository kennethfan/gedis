package acl

import "testing"

func TestKeyExtractStepAndCustom(t *testing.T) {
	RegisterMeta(Meta{Name: "MSET", Category: "string", Keys: KeySpec{First: 0, Last: -1, Step: 2}})
	RegisterMeta(Meta{Name: "SORT", Category: "generic", Keys: KeySpec{Custom: SortStoreKey}})
	keys := ExtractKeys("MSET", []string{"k1", "v1", "k2", "v2"})
	if len(keys) != 2 || keys[0] != "k1" || keys[1] != "k2" {
		t.Fatalf("MSET step extract: %v", keys)
	}
	keys = ExtractKeys("SORT", []string{"src", "STORE", "dst"})
	if len(keys) != 2 || keys[1] != "dst" {
		t.Fatalf("SORT STORE dst: %v", keys)
	}
}
