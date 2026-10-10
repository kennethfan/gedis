package commands

import (
	"net"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 已写入合法配置 KEA
// When: 依次写入非法字母、空串
// Then: 非法报错且旧配置原样保留；空串合法
func TestSetNotifyStringParsesFlags(t *testing.T) {
	t.Cleanup(func() { _ = SetNotifyString("") })
	if err := SetNotifyString("KEA"); err != nil {
		t.Fatalf("SetNotifyString(KEA) = %v", err)
	}
	if got := NotifyString(); got != "KEA" {
		t.Fatalf("NotifyString() = %q, want KEA", got)
	}
	if err := SetNotifyString("q"); err == nil {
		t.Fatal("非法字母必须报错")
	}
	if got := NotifyString(); got != "KEA" {
		t.Fatalf("失败的 Set 不得破坏旧配置, got %q", got)
	}
	if err := SetNotifyString(""); err != nil {
		t.Fatalf("空串合法: %v", err)
	}
}

// notifyPMsg 构造 PSUBSCRIBE __key*__:* 收到的 pmessage 推送值。
func notifyPMsg(channel, event string) protocol.Value {
	return protocol.ArrayOf(
		protocol.BulkOf("pmessage"),
		protocol.BulkOf("__key*__:*"),
		protocol.BulkOf(channel),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(event)},
	)
}

// Given: 真实 PubSubRegistry 注入 publisher，A 已 PSUBSCRIBE __key*__:*
// When: 逐档改 mask 并直发 Notify
// Then: K/E 通道门与类门各自生效；nil publisher 零投递不 panic
func TestNotifyRespectsClassAndChannelGates(t *testing.T) {
	r, reg, srvA, cliA, _, _ := openPubSubSetup(t)
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	// K 开 E 关：只落 keyspace；下一条须为 case2 探针（若 E 泄漏会先到 keyevent）
	require.NoError(t, SetNotifyString("Kg"))
	Notify("g", "set", "k")
	require.Equal(t, notifyPMsg("__keyspace@0__:k", "set"), readPushed(t, cliA))

	// 无 $：Notify($,..) 零投递；探针 g 证明通道存活且无 $ 泄漏
	// （mask 必须带类位 g，否则类门把探针也拦掉——brief 字面 "KE" 不含类位，修正为 "KEg"）
	require.NoError(t, SetNotifyString("KEg"))
	Notify("$", "set", "k")
	Notify("g", "del", "probe")
	require.Equal(t, notifyPMsg("__keyspace@0__:probe", "del"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:del", "probe"), readPushed(t, cliA))

	// KE$ 齐备：两频道各一条
	require.NoError(t, SetNotifyString("KE$"))
	Notify("$", "set", "k")
	require.Equal(t, notifyPMsg("__keyspace@0__:k", "set"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:set", "k"), readPushed(t, cliA))

	// nil publisher：零投递不 panic；恢复后 k4 先到（nil 期间零残留）
	SetNotifyPublisher(nil)
	Notify("$", "set", "k3")
	Notify("$", "set", "k3")
	SetNotifyPublisher(reg.Publish)
	Notify("$", "set", "k4")
	require.Equal(t, notifyPMsg("__keyspace@0__:k4", "set"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:set", "k4"), readPushed(t, cliA))
}

// Given: 默认配置
// When: CONFIG SET/GET notify-keyspace-events（合法与非法）
// Then: 合法生效可读回；非法报错且旧值不变
func TestConfigNotifyRoundTrip(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	t.Cleanup(func() { _ = SetNotifyString("") })
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	got := dispatch(r, "CONFIG", "GET", "notify-keyspace-events")
	require.Equal(t, protocol.KindArray, got.Kind)
	require.Equal(t, protocol.BulkOf("notify-keyspace-events"), got.Elems[0])
	require.Equal(t, protocol.BulkOf("KEA"), got.Elems[1])

	require.Equal(t, protocol.KindError,
		dispatch(r, "CONFIG", "SET", "notify-keyspace-events", "ZQ").Kind)
	got = dispatch(r, "CONFIG", "GET", "notify-keyspace-events")
	require.Equal(t, protocol.BulkOf("KEA"), got.Elems[1])
}

// Given: KEA 开启，A 订阅 __key*__:*，B 执行命令
// When: SET foo v / SET NX（已存在）/ DEL foo / DEL nokey
// Then: 成功写删各发 keyspace+keyevent；失败路径零事件
func TestSetDelEmitNotifications(t *testing.T) {
	r, _, _ := openMonitorSetup(t)
	reg := RegisterPubSub(r)
	srvA, cliA := net.Pipe()
	srvB, cliB := net.Pipe()
	t.Cleanup(func() { srvA.Close(); cliA.Close(); srvB.Close(); cliB.Close() })
	SetNotifyPublisher(reg.Publish)
	t.Cleanup(func() {
		SetNotifyPublisher(nil)
		_ = SetNotifyString("")
	})
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "CONFIG", "SET", "notify-keyspace-events", "KEA"))
	require.Equal(t, confirmKind("__key*__:*", 1, "psubscribe"),
		dispatchPub(r, srvA, "PSUBSCRIBE", "__key*__:*"))

	// SET foo v → 双频道 set
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "SET", "foo", "v"))
	require.Equal(t, notifyPMsg("__keyspace@0__:foo", "set"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:set", "foo"), readPushed(t, cliA))

	// SET NX 失败 → nil 且零事件；紧接 DEL 的第一条必须是 del
	require.Equal(t, protocol.Value{Kind: protocol.KindBulkString},
		dispatchPub(r, srvB, "SET", "foo", "v", "NX"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatchPub(r, srvB, "DEL", "foo"))
	require.Equal(t, notifyPMsg("__keyspace@0__:foo", "del"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:del", "foo"), readPushed(t, cliA))

	// DEL nokey → 0 且零事件；探针 SET bar 证明通道间无残留
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatchPub(r, srvB, "DEL", "nokey"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatchPub(r, srvB, "SET", "bar", "v"))
	require.Equal(t, notifyPMsg("__keyspace@0__:bar", "set"), readPushed(t, cliA))
	require.Equal(t, notifyPMsg("__keyevent@0__:set", "bar"), readPushed(t, cliA))
}
