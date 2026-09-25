package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// oracle 来源：本地 redis-server 7.2.6 单节点 cluster-enabled 实例 CLUSTER KEYSLOT 实测；
// foo→12182 与 testdata/cluster/moved.redis 真机链路互证。
func Test_Slot_when_KnownVectors(t *testing.T) {
	for key, want := range map[string]int{
		"foo":               12182,
		"bar":               5061,
		"hello":             866,
		"{foo}bar":          12182,
		"foo{bar}baz":       5061,
		"{}foo":             9500,
		"foo{}bar":          14292,
		"{a}{b}":            15495,
		"a{b":               13340,
		"a}b{":              6027,
		"{user}.1":          5474,
		"{user}.2":          5474,
		"{user1}.following": 8106,
		"{user1}.followers": 8106,
	} {
		require.Equal(t, want, Slot(key), "key %q", key)
	}
}

// hash-tag 语义：同 tag 同槽（迁移/多 key 同槽判断的基础）。
func Test_Slot_when_SameTagSameSlot(t *testing.T) {
	require.Equal(t, Slot("{user}.1"), Slot("{user}.2"))
	require.Equal(t, Slot("{user1}.following"), Slot("{user1}.followers"))
	require.NotEqual(t, Slot("foo"), Slot("bar"))
}

// 空 key 与边界：不 panic，结果落在槽区间内。
func Test_Slot_when_EdgeKeys(t *testing.T) {
	for _, key := range []string{"", "{", "}", "{}", "{{}}"} {
		got := Slot(key)
		require.GreaterOrEqual(t, got, 0)
		require.Less(t, got, NumSlots)
	}
}
