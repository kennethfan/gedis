package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Snapshot_when_MarshalRoundTrip(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 8191}}},
		{ID: "nodeB", Addr: "127.0.0.1:7001", Ranges: [][2]int{{8192, 16383}}},
	})
	require.NoError(t, err)
	require.NoError(t, topo.SetMigrating(100, "nodeB"))
	require.NoError(t, topo.SetNode(9000, "nodeB"))
	snap := topo.Snapshot()
	data := MarshalSnapshot(snap, topo.Nodes())
	snap2, err := UnmarshalSnapshot(data, topo.Nodes())
	require.NoError(t, err)
	require.Equal(t, snap, snap2)
	_, err = UnmarshalSnapshot([]byte("garbage\n"), topo.Nodes())
	require.Error(t, err)
}
