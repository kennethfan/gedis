package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func twoNodeSpecs() []NodeSpec {
	return []NodeSpec{
		{ID: "nodeA", Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 5460}}},
		{Addr: "127.0.0.1:7001", Ranges: [][2]int{{5461, 10922}, {10923, 16383}}},
	}
}

func Test_Build_when_TwoNodes(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", twoNodeSpecs())
	require.NoError(t, err)
	require.True(t, topo.Owns(0))
	require.True(t, topo.Owns(5460))
	require.False(t, topo.Owns(5461))
	require.Equal(t, "127.0.0.1:7001", topo.OwnerAddr(5461))
	require.Equal(t, "127.0.0.1:7000", topo.OwnerAddr(0))
	require.Equal(t, "nodeA", topo.SelfID())
	require.Equal(t, "127.0.0.1:7000", topo.SelfAddr())
	require.Len(t, topo.SelfID(), 5)
	require.Len(t, topo.Nodes()[1].ID, 40)
	require.Equal(t, 16384, topo.AssignedCount())
}

func Test_Build_when_EmptySpecsFullSlots(t *testing.T) {
	topo, err := Build("127.0.0.1:7777", nil)
	require.NoError(t, err)
	require.True(t, topo.Owns(12182))
	require.Equal(t, "127.0.0.1:7777", topo.OwnerAddr(12182))
	require.Len(t, topo.SelfID(), 40)
	require.Equal(t, 16384, topo.AssignedCount())
}

func Test_Build_when_SelfUnmatched(t *testing.T) {
	_, err := Build("127.0.0.1:9999", twoNodeSpecs())
	require.Error(t, err)
}

func Test_Build_when_SlotClaimedTwice(t *testing.T) {
	_, err := Build("127.0.0.1:7000", []NodeSpec{
		{Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 100}}},
		{Addr: "127.0.0.1:7001", Ranges: [][2]int{{50, 200}}},
	})
	require.Error(t, err)
}

func Test_Build_when_WildcardHostMatchesPort(t *testing.T) {
	topo, err := Build("0.0.0.0:7000", twoNodeSpecs())
	require.NoError(t, err)
	require.Equal(t, "nodeA", topo.SelfID())
}

func Test_Topology_when_GapSlotClusterdown(t *testing.T) {
	topo, err := Build("127.0.0.1:7000", []NodeSpec{
		{Addr: "127.0.0.1:7000", Ranges: [][2]int{{0, 100}}},
	})
	require.NoError(t, err)
	require.False(t, topo.Owns(5000))
	require.Equal(t, "", topo.OwnerAddr(5000))
	require.Equal(t, 101, topo.AssignedCount())
}
