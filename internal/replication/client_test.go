package replication

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

type memKV struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemKV() *memKV { return &memKV{data: make(map[string][]byte)} }

func (m *memKV) Get(_ context.Context, key []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.data[string(key)]
	if !ok {
		return nil, errNotFound
	}
	return v, nil
}

func (m *memKV) Set(_ context.Context, key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = append([]byte(nil), value...)
	return nil
}

func (m *memKV) Delete(_ context.Context, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, string(key))
	return nil
}

var errNotFound = errNotFoundType{}

type errNotFoundType struct{}

func (errNotFoundType) Error() string { return "not found" }

// fakeMaster 讲 PSYNC 方言：回 PONG，回 FULLRESYNC + RDB，随后推一个 OP。
func fakeMaster(t *testing.T, conn net.Conn, rdb []RawEntry) {
	t.Helper()
	rd := bufio.NewReader(conn)
	_, err := protocol.Decode(rd)
	require.NoError(t, err)
	_, err = conn.Write(protocol.Value{Kind: protocol.KindSimpleString, S: "PONG"}.Append(nil))
	require.NoError(t, err)
	_, err = protocol.Decode(rd)
	require.NoError(t, err)
	reply := protocol.ArrayOf(
		protocol.BulkOf("FULLRESYNC abc123 0"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: MarshalRDB(rdb)},
	)
	_, err = conn.Write(reply.Append(nil))
	require.NoError(t, err)
	op := protocol.ArrayOf(
		protocol.BulkOf("OP"),
		protocol.BulkOf("set"),
		protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte("s:live")},
		protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte("t\x00v")},
		protocol.BulkOf("1"),
	)
	_, err = conn.Write(op.Append(nil))
	require.NoError(t, err)
}

func Test_Client_when_FullSync(t *testing.T) {
	kv := newMemKV()
	master, replica := net.Pipe()
	defer master.Close()
	defer replica.Close()
	go fakeMaster(t, master, []RawEntry{{Key: []byte("s:a"), Value: []byte("v")}})

	c := NewClient(kv, "fake:0")
	c.Dial = func() (net.Conn, error) { return replica, nil }
	c.Start()
	defer c.Stop()

	require.Eventually(t, func() bool {
		v, err := kv.Get(context.Background(), []byte("s:a"))
		return err == nil && string(v) == "v"
	}, 2*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		_, err := kv.Get(context.Background(), []byte("s:live"))
		return err == nil
	}, 2*time.Second, 10*time.Millisecond)
}
