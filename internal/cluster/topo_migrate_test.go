package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Topology_when_MigratingState(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	slot := 100
	require.NoError(t, topo.SetMigrating(slot, "nodeB"))
	got, ok := topo.MigratingTo(slot)
	require.True(t, ok)
	require.Equal(t, "nodeB", got)
	topo.SetStable(slot)
	_, ok = topo.MigratingTo(slot)
	require.False(t, ok)
}

func Test_Topology_when_SnapshotRoundTrip(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	require.NoError(t, topo.SetMigrating(100, "nodeB"))
	snap := topo.Snapshot()
	require.Equal(t, uint64(1), snap.Epoch)
	topo2, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 16383}}},
	})
	require.NoError(t, err)
	require.NoError(t, topo2.LoadSnapshot(snap))
	got, ok := topo2.MigratingTo(100)
	require.True(t, ok)
	require.Equal(t, "nodeB", got)
	stale := snap
	stale.Epoch = 0
	require.Error(t, topo2.LoadSnapshot(stale))
}
