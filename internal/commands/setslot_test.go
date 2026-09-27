package commands

import (
	"fmt"
	"net"
	"strconv"
	"testing"

	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Setslot_when_MigratingThenStable(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	got := dispatch(r, "CLUSTER", "SETSLOT", "100", "MIGRATING", "nodeB")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	got = dispatch(r, "CLUSTER", "SETSLOT", "100", "STABLE")
	require.Equal(t, "OK", got.S)
}

func Test_Setslot_when_NotOwnerFails(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	got := dispatch(r, "CLUSTER", "SETSLOT", "6000", "MIGRATING", "nodeC")
	require.Equal(t, protocol.KindError, got.Kind)
}

// dispatchSetSlot 发送 CLUSTER SETSLOT 并在非 OK 时返回 error（调用方用 require.NoError 断言）。
func dispatchSetSlot(r *network.Router, slot int, args ...string) error {
	got := dispatch(r, append([]string{"CLUSTER", "SETSLOT", strconv.Itoa(slot)}, args...)...)
	if got.Kind == protocol.KindSimpleString && got.S == "OK" {
		return nil
	}
	return fmt.Errorf("SETSLOT %d %v: got kind=%v s=%q", slot, args, got.Kind, got.S)
}

// Given: 源端槽 MIGRATING，key 仍在本地
// When: GET 该 key
// Then: 放行执行（Task2 Ruling：SET 必须先于 MIGRATING，否则缺 key 按 spec 应回 ASK）
func Test_Intercept_when_MigratingKeyPresentPasses(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	key := "askkey-present-1"
	slot := cluster.Slot(key)
	require.Less(t, slot, 5461)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", key, "v"))
	require.NoError(t, dispatchSetSlot(r, slot, "MIGRATING", "nodeB"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", key, "v2"))
	require.Equal(t, "v2", string(dispatch(r, "GET", key).Bulk))
}

// Given: 源端槽 MIGRATING，key 已搬走（本地无）
// When: GET 该 key
// Then: ASK slot 127.0.0.1:7001（指向目标）
func Test_Intercept_when_MigratingKeyMissingAsks(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	key := "askkey-missing-xyz"
	slot := cluster.Slot(key)
	require.Less(t, slot, 5461)
	require.NoError(t, dispatchSetSlot(r, slot, "MIGRATING", "nodeB"))
	got := dispatch(r, "GET", key)
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "ASK ")
	require.Contains(t, got.S, "127.0.0.1:7001")
}

// Given: 目标端槽 IMPORTING，同连接已 ASKING，本地无 key（RESTORE 前态）
// When: 同连接 GET 该槽 key
// Then: 放行（真机 importing+ASKING 不问 key 有无；缺 key 时 GET 回 nil）
// 互证：真机 R2(IMPORTING) 上 ASKING+SET 可写；缺 key 拒绝 MOVED 会断掉 MIGRATE 的 RESTORE。
func Test_Intercept_when_ImportingAskedAbsentKeyPasses(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7001"))
	key := "impkey-probe"
	slot := cluster.Slot(key)
	require.True(t, slot < 5461 || slot > 10922, "probe key must hash outside nodeB ranges, got slot %d", slot)
	require.NoError(t, dispatchSetSlot(r, slot, "IMPORTING", "nodeA"))
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	require.Equal(t, "OK", dispatchConn(r, c1, "ASKING").S)
	got := dispatchConn(r, c1, "GET", key)
	require.Equal(t, protocol.KindBulkString, got.Kind)
	require.Nil(t, got.Bulk)
}

// Given: 源端槽 MIGRATING，key 不存在
// When: MIGRATE 该 key
// Then: 放行给 handler 回 NOKEY（真机先查 key 存在性，不走 ASK；互证 redis.out MIGRATE=NOKEY）
func Test_Intercept_when_MigratingMigrateAbsentKeyNoKey(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7000"))
	key := "askkey-missing-xyz"
	slot := cluster.Slot(key)
	require.Less(t, slot, 5461)
	require.NoError(t, dispatchSetSlot(r, slot, "MIGRATING", "nodeB"))
	got := dispatch(r, "MIGRATE", "127.0.0.1", "7001", key, "0", "5000")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "NOKEY", got.S)
}

// Given: 目标端槽残留 IMPORTING（源已 STABLE，无 ASKING）
// When: GET 该槽 key
// Then: MOVED（不死循环；Review Focus 第一行）
func Test_Intercept_when_ImportingWithoutAskingMoves(t *testing.T) {
	r, _ := openClusterSetup(t, splitTopo(t, "127.0.0.1:7001"))
	key := "impkey-probe"
	slot := cluster.Slot(key)
	require.True(t, slot < 5461 || slot > 10922, "probe key must hash outside nodeB ranges, got slot %d", slot)
	require.NoError(t, dispatchSetSlot(r, slot, "IMPORTING", "nodeA"))
	got := dispatch(r, "GET", key)
	require.Equal(t, protocol.KindError, got.Kind)
	require.True(t, len(got.S) > 6 && got.S[:6] == "MOVED ", "got %q", got.S)
	require.Contains(t, got.S, "127.0.0.1:7000")
}
