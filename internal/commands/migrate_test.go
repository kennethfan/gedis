package commands

import (
	"bufio"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// fakeRestoreServer 是 MIGRATE 测试替身：扮演目标节点的 TCP 入口，只懂
// AUTH、ASKING 与 RESTORE。existing 命中的 key 在无 REPLACE 时回 BUSYKEY。
// requireAsking 为真时模拟 importing 态目标：同一连接未见 ASKING 即对
// RESTORE 回 MOVED（真机与 gedis 的行为一致）。
type fakeRestoreServer struct {
	ln            net.Listener
	Port          string
	password      string
	mu            sync.Mutex
	gotRestore    bool
	requireAsking bool
	existing      map[string]bool
}

func startFakeRestoreServer(t testing.TB, existing []string) *fakeRestoreServer {
	return startFakeRestoreServerWithAuth(t, existing, "")
}

func startFakeRestoreServerWithAuth(t testing.TB, existing []string, password string) *fakeRestoreServer {	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	f := &fakeRestoreServer{
		ln:       ln,
		Port:     strconv.Itoa(ln.Addr().(*net.TCPAddr).Port),
		password: password,
		existing: make(map[string]bool, len(existing)),
	}
	for _, k := range existing {
		f.existing[k] = true
	}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve()
	return f
}

func startFakeRestoreServerEnforcingAsking(t testing.TB, existing []string) *fakeRestoreServer {
	f := startFakeRestoreServer(t, existing)
	f.mu.Lock()
	f.requireAsking = true
	f.mu.Unlock()
	return f
}

// restored 的 mutex 访问器：handler goroutine 写、测试 goroutine 读，
// 直接读字段会在 -race 下报 data race。
func (f *fakeRestoreServer) restored() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.gotRestore
}

func (f *fakeRestoreServer) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

func (f *fakeRestoreServer) reply(c net.Conn, s string) {
	_, _ = c.Write([]byte(s))
}

func (f *fakeRestoreServer) handle(c net.Conn) {
	defer c.Close()
	authed := f.password == ""
	asking := false
	rd := bufio.NewReader(c)
	for {
		v, err := protocol.Decode(rd)
		if err != nil {
			return
		}
		if v.Kind != protocol.KindArray || len(v.Elems) == 0 || v.Elems[0].Kind != protocol.KindBulkString {
			f.reply(c, "-ERR protocol error\r\n")
			return
		}
		switch strings.ToUpper(string(v.Elems[0].Bulk)) {
		case "ASKING":
			asking = true
			f.reply(c, "+OK\r\n")
		case "AUTH":
			ok := false
			if len(v.Elems) == 2 && string(v.Elems[1].Bulk) == f.password {
				ok = true
			}
			if len(v.Elems) == 3 && string(v.Elems[2].Bulk) == f.password {
				ok = true
			}
			if ok {
				authed = true
				f.reply(c, "+OK\r\n")
			} else {
				f.reply(c, "-WRONGPASS invalid username-password pair or user is disabled.\r\n")
			}
		case "RESTORE":
			if !authed {
				f.reply(c, "-NOAUTH Authentication required.\r\n")
				continue
			}
			f.mu.Lock()
			reqAsking := f.requireAsking
			f.mu.Unlock()
			if reqAsking && !asking {
				f.reply(c, "-MOVED 7574 127.0.0.1:1\r\n")
				continue
			}
			if len(v.Elems) < 4 {
				f.reply(c, "-ERR wrong number of arguments for 'restore' command\r\n")
				continue
			}
			key := string(v.Elems[1].Bulk)
			replace := false
			for _, e := range v.Elems[4:] {
				if e.Kind == protocol.KindBulkString && strings.ToUpper(string(e.Bulk)) == "REPLACE" {
					replace = true
				}
			}
			if f.existing[key] && !replace {
				f.reply(c, "-BUSYKEY Target key name already exists.\r\n")
				continue
			}
			f.mu.Lock()
			f.gotRestore = true
			f.mu.Unlock()
			f.reply(c, "+OK\r\n")
		default:
			f.reply(c, "-ERR unknown command\r\n")
		}
	}
}

func Test_Migrate_when_CopyKeepsSource(t *testing.T) {
	r, store := openTestSetup(t)
	_ = store
	fake := startFakeRestoreServer(t, nil)
	require.Equal(t, "OK", dispatch(r, "SET", "mg1", "v1").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg1", "0", "1000", "COPY")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	require.Equal(t, protocol.BulkOf("v1"), dispatch(r, "GET", "mg1"))
	require.True(t, fake.restored())
}

func Test_Migrate_when_NoKeyReportsNoKey(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServer(t, nil)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg-missing", "0", "1000")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "NOKEY")
}

func Test_Migrate_when_TargetExistsWithoutReplaceFails(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServer(t, []string{"mg2"})
	require.Equal(t, "OK", dispatch(r, "SET", "mg2", "v2").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg2", "0", "1000")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "BUSYKEY")
}

// Given: 目标要求密码而 AUTH 错误
// When: MIGRATE 携带错误密码
// Then: 返回 WRONGPASS，源 key 保留，目标未收到 RESTORE
func Test_Migrate_when_AuthFailsKeepsSource(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServerWithAuth(t, nil, "secret")
	require.Equal(t, "OK", dispatch(r, "SET", "mg3", "v3").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg3", "0", "1000", "AUTH", "wrong")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "WRONGPASS")
	require.Equal(t, protocol.BulkOf("v3"), dispatch(r, "GET", "mg3"))
	require.False(t, fake.restored())
}

// Given: 目标节点处于 importing 态（未见 ASKING 即对 RESTORE 回 MOVED）
// When: MIGRATE 搬运存在的 key
// Then: 返回 OK，源 key 被删除，目标收到 RESTORE
func Test_Migrate_when_TargetRequiresAskingSendsAskingFirst(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServerEnforcingAsking(t, nil)
	require.Equal(t, "OK", dispatch(r, "SET", "mg5", "v5").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg5", "0", "1000")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	require.Nil(t, dispatch(r, "GET", "mg5").Bulk)
	require.True(t, fake.restored())
}

// Given: 已存在的源 key
// When: MIGRATE 不带 COPY 且目标接受
// Then: 返回 OK，源 key 被删除，目标收到 RESTORE
func Test_Migrate_when_MovesByDefault(t *testing.T) {
	r, _ := openTestSetup(t)
	fake := startFakeRestoreServer(t, nil)
	require.Equal(t, "OK", dispatch(r, "SET", "mg4", "v4").S)
	got := dispatch(r, "MIGRATE", "127.0.0.1", fake.Port, "mg4", "0", "1000")
	require.Equal(t, protocol.KindSimpleString, got.Kind)
	require.Equal(t, "OK", got.S)
	require.Nil(t, dispatch(r, "GET", "mg4").Bulk)
	require.True(t, fake.restored())
}
