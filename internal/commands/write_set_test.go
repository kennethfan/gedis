package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: WriteCommandSet 名单
// When: 逐一核对期望写命令与反向只读钉
// Then: 全部命中；只读命令不在集合
func TestWriteCommandSetCoversAllMutatingCommands(t *testing.T) {
	want := []string{
		"HEXPIRE", "HEXPIREAT", "HPEXPIRE", "HPEXPIREAT", "HPERSIST",
		"XREADGROUP", "XACK", "XCLAIM", "XAUTOCLAIM",
		"GETSET", "SETEX", "PSETEX", "SETNX",
		"LMOVE", "BLMOVE", "RPOPLPUSH", "BRPOPLPUSH",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
		"BZPOPMAX", "BZPOPMIN",
		"ZDIFFSTORE", "ZINTERSTORE", "ZUNIONSTORE", "ZRANGESTORE",
		"ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
		"MIGRATE", "RESTORE", "EVAL", "EVALSHA",
	}
	for _, c := range want {
		if !WriteCommandSet()[c] {
			t.Errorf("WriteCommandSet 缺 %s", c)
		}
	}
	for _, c := range []string{"TOUCH", "READONLY", "READWRITE", "GET", "LRANGE", "DUMP", "SORT_RO"} {
		if WriteCommandSet()[c] {
			t.Errorf("只读命令 %s 不得进 WriteCommandSet", c)
		}
	}
}

// Given: 副本只读模式 + WriteCommandSet 已装
// When: dispatch HPERSIST / READONLY
// Then: HPERSIST 报 READONLY 拒写；READONLY 放行 +OK
func TestReadonlyReplicaRejectsHexpires(t *testing.T) {
	r, store := openTestSetup(t)
	st := acl.NewStore()
	authReg := RegisterAuth(r, st)
	RegisterConn(r, st, authReg)
	RegisterHash(r, store)
	r.SetWriteCommands(WriteCommandSet())
	r.SetReadOnly(true)
	t.Cleanup(func() { r.SetReadOnly(false) })

	got := dispatch(r, "HPERSIST", "h", "f")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "READONLY You can't write against a read only replica.")

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "READONLY"))
}

// Given: 本引擎无 RDB/AOF
// When: SAVE / SAVE extra-arg
// Then: 与 BGSAVE 同文案诚实报错；多余参数报 wrong number
func TestSaveFailsHonestly(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)
	got := dispatch(r, "SAVE")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR not supported on this engine: no RDB/AOF persistence", got.S)

	got = dispatch(r, "SAVE", "extra")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")
}
