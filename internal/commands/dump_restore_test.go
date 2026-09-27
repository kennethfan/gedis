package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// dispatchRaw 透传已组装的命令（承载二进制 DUMP 负载时用）。
func dispatchRaw(r *network.Router, v protocol.Value) protocol.Value {
	return r.Dispatch(context.Background(), v)
}

// Given: key 不存在
// When: DUMP
// Then: 返回 nil
func Test_Dump_MissingKey_ReturnsNil(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "DUMP", "nodumpkey")
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: string key 有值
// When: DUMP → DEL → RESTORE（带 TTL）
// Then: 值与 TTL 恢复
func Test_DumpRestore_String_RoundTrip(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, "OK", dispatch(r, "SET", "dk", "v1").S)
	dumped := dispatch(r, "DUMP", "dk")
	require.Equal(t, protocol.KindBulkString, dumped.Kind)
	require.NotEmpty(t, dumped.Bulk)
	e, err := datastruct.Decode(dumped.Bulk)
	require.NoError(t, err)
	require.Equal(t, datastruct.TypeString, e.Type)
	require.Equal(t, []byte("v1"), e.Payload)

	require.Equal(t, int64(1), dispatch(r, "DEL", "dk").I)
	got := dispatchRaw(r, protocol.ArrayOf(
		protocol.BulkOf("RESTORE"),
		protocol.BulkOf("dk"),
		protocol.BulkOf("5000"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: dumped.Bulk},
	))
	require.Equal(t, "OK", got.S)
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "GET", "dk"))
	ttl := dispatch(r, "PTTL", "dk").I
	require.Greater(t, ttl, int64(0))
	require.LessOrEqual(t, ttl, int64(5000))
}

// Given: 目标 key 已存在
// When: 无 REPLACE RESTORE / 有 REPLACE RESTORE
// Then: BUSYKEY / 覆盖成功
func Test_Restore_ExistingKey_NeedsReplace(t *testing.T) {
	r, _ := openTestSetup(t)
	require.Equal(t, "OK", dispatch(r, "SET", "rk", "old").S)
	require.Equal(t, "OK", dispatch(r, "SET", "src", "new").S)
	dumped := dispatch(r, "DUMP", "src")

	busy := dispatchRaw(r, protocol.ArrayOf(
		protocol.BulkOf("RESTORE"),
		protocol.BulkOf("rk"),
		protocol.BulkOf("0"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: dumped.Bulk},
	))
	require.Equal(t, protocol.KindError, busy.Kind)
	require.Equal(t, "BUSYKEY Target key name already exists.", busy.S)
	require.Equal(t, protocol.BulkOf("old"), dispatch(r, "GET", "rk"))

	ok := dispatchRaw(r, protocol.ArrayOf(
		protocol.BulkOf("RESTORE"),
		protocol.BulkOf("rk"),
		protocol.BulkOf("0"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: dumped.Bulk},
		protocol.BulkOf("REPLACE"),
	))
	require.Equal(t, "OK", ok.S)
	require.Equal(t, protocol.BulkOf("new"), dispatch(r, "GET", "rk"))
}

// Given: 非法 payload
// When: RESTORE
// Then: 报 DUMP 校验错（对齐 7.2.6 措辞）
func Test_Restore_BadPayload_ChecksumError(t *testing.T) {
	r, _ := openTestSetup(t)
	got := dispatch(r, "RESTORE", "bk", "0", "not-a-dump")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR DUMP payload version or checksum are wrong", got.S)
}

// Given: hash key
// When: DUMP → DEL → RESTORE
// Then: 类型与内容恢复
func Test_DumpRestore_Hash_PreservesType(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterHash(r, store)
	require.Equal(t, int64(1), dispatch(r, "HSET", "hk", "f", "v").I)
	dumped := dispatch(r, "DUMP", "hk")
	e, err := datastruct.Decode(dumped.Bulk)
	require.NoError(t, err)
	require.Equal(t, datastruct.TypeHash, e.Type)

	require.Equal(t, int64(1), dispatch(r, "DEL", "hk").I)
	got := dispatchRaw(r, protocol.ArrayOf(
		protocol.BulkOf("RESTORE"),
		protocol.BulkOf("hk"),
		protocol.BulkOf("0"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: dumped.Bulk},
	))
	require.Equal(t, "OK", got.S)
	require.Equal(t, "hash", dispatch(r, "TYPE", "hk").S)
	require.Equal(t, protocol.BulkOf("v"), dispatch(r, "HGET", "hk", "f"))
}
