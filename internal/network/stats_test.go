package network

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Stats_when_DispatchCounts(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)
	s := NewStats()
	r.AttachStats(s)
	r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("PING"),
	}})
	r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("NOPE"),
	}})
	snap := s.Snapshot()
	require.Equal(t, int64(2), snap.CommandsProcessed)
}

func Test_SlowLog_when_OverThreshold(t *testing.T) {
	r := NewRouter()
	r.Register("PING", handlePing)
	s := NewStats()
	s.SlowThresholdMicros = 0
	r.AttachStats(s)
	r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
		protocol.BulkOf("PING"), protocol.BulkOf("hello"),
	}})
	entries := s.SlowEntries(10)
	require.Len(t, entries, 1)
	require.Equal(t, "ping", entries[0].Command)
	require.Equal(t, []string{"hello"}, entries[0].Args)

	s.SlowReset()
	require.Empty(t, s.SlowEntries(10))
	require.Equal(t, 0, s.SlowLen())
}
