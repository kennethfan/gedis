package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: list key
// When: EXPIRE / TTL / PERSIST / TYPE / OBJECT ENCODING / KEYS *
// Then: 通用键空间语义对 list 生效
func Test_List_when_Keyspace(t *testing.T) {
	r, _ := openListSetup(t)
	dispatch(r, "RPUSH", "mylist", "a", "b")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "EXPIRE", "mylist", "100"))
	got := dispatch(r, "TTL", "mylist")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Greater(t, got.I, int64(0))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "list"}, dispatch(r, "TYPE", "mylist"))
	require.Equal(t, protocol.BulkOf("ziplist"), dispatch(r, "OBJECT", "ENCODING", "mylist"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "PERSIST", "mylist"))
	require.Equal(t, bulkList("mylist"), dispatch(r, "KEYS", "*"))
}
