package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func openTestSetup(t testing.TB) (*network.Router, *storage.Pebble) {
	t.Helper()
	store := storage.New(t.TempDir())
	require.NoError(t, store.Open())
	t.Cleanup(func() { _ = store.Close() })
	r := network.NewRouter()
	RegisterStrings(r, store)
	return r, store
}

func cmd(args ...string) protocol.Value {
	elems := make([]protocol.Value, len(args))
	for i, a := range args {
		elems[i] = protocol.BulkOf(a)
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: elems}
}

func dispatch(r *network.Router, args ...string) protocol.Value {
	return r.Dispatch(context.Background(), cmd(args...))
}

// Given: 空库
// When: SET k v 后 GET k
// Then: +OK，GET 返回 bulk v
func Test_String_when_SetGet(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, got)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))
}

// Given: 空库
// When: GET 不存在的 key
// Then: null bulk
func Test_String_when_GetMissing(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "GET", "nope")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 已存在的 key
// When: SET NX / SET XX 缺失 key
// Then: 条件不满足返回 null，原值不变
func Test_String_when_NxXx(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v"))

	got := dispatch(r, "SET", "k", "new", "NX")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GET", "k"))

	got = dispatch(r, "SET", "missing", "v", "XX")
	require.Nil(t, got.Bulk)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "missing", "v"))
}

// Given: 已存在的 key
// When: SET ... GET
// Then: 返回旧值并写入新值
func Test_String_when_SetGetOption(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "old")
	got := dispatch(r, "SET", "k", "new", "GET")
	require.Equal(t, protocol.BulkOf("old"), got)
	require.Equal(t, protocol.BulkOf("new"), dispatch(r, "GET", "k"))

	got = dispatch(r, "SET", "fresh", "v", "GET")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 带过期时间的 key
// When: SET ... KEEPTTL
// Then: 过期时间保留；SET 不带选项则清除过期
func Test_String_when_KeepTTL(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v", "EX", "100")
	ttl1 := dispatch(r, "TTL", "k")
	require.Equal(t, int64(100), ttl1.I)

	dispatch(r, "SET", "k", "v2", "KEEPTTL")
	ttl2 := dispatch(r, "TTL", "k")
	require.Greater(t, ttl2.I, int64(0))
	require.LessOrEqual(t, ttl2.I, ttl1.I)

	dispatch(r, "SET", "k", "v3")
	require.Equal(t, int64(-1), dispatch(r, "TTL", "k").I)
}

// Given: 已存在的 key
// When: GETDEL
// Then: 返回值并删除
func Test_String_when_GetDel(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "GETDEL", "k"))
	got := dispatch(r, "GET", "k")
	require.Nil(t, got.Bulk)
}

// Given: 多个 key
// When: DEL / UNLINK
// Then: 返回删除计数，不存在的 key 忽略
func Test_String_when_Del(t *testing.T) {
	r, _ := openTestSetup(t)
	dispatch(r, "SET", "a", "1")
	dispatch(r, "SET", "b", "2")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2}, dispatch(r, "DEL", "a", "b", "missing"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "UNLINK", "missing"))
	dispatch(r, "SET", "c", "3")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "UNLINK", "c"))
}

// Given: 过期时间为过去的 key（EXAT 1）
// When: GET
// Then: 当作不存在（被动过期），且 key 被删除
func Test_String_when_PassiveExpiry(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v", "EXAT", "1"))
	got := dispatch(r, "GET", "k")
	require.Nil(t, got.Bulk)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0}, dispatch(r, "DEL", "k"))
}

// Given: hash 类型 key（直接写存储）
// When: GET
// Then: WRONGTYPE 错误
func Test_String_when_WrongType(t *testing.T) {
	r, store := openTestSetup(t)
	ctx := context.Background()
	require.NoError(t, store.Set(ctx, datastruct.StringKey("h"), datastruct.Encode(datastruct.TypeHash, 0, []byte("f"))))
	got := dispatch(r, "GET", "h")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGTYPE")
}
