package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Topology_when_Disabled(t *testing.T) {
	topo := NewTopology(false, [][2]int{{0, NumSlots - 1}})
	require.False(t, topo.Owns(0))
	require.False(t, topo.Owns(12182))
}

func Test_Topology_when_Full(t *testing.T) {
	topo := FullTopology()
	require.True(t, topo.Owns(0))
	require.True(t, topo.Owns(12182))
	require.True(t, topo.Owns(NumSlots-1))
}

func Test_Topology_when_Ranges(t *testing.T) {
	topo := NewTopology(true, [][2]int{{0, 5460}, {12345, 12345}})
	require.True(t, topo.Owns(0))
	require.True(t, topo.Owns(5460))
	require.False(t, topo.Owns(5461))
	require.True(t, topo.Owns(12345))
	require.False(t, topo.Owns(-1))
	require.False(t, topo.Owns(NumSlots))
}

func Test_Topology_when_Nil(t *testing.T) {
	var topo *Topology
	require.False(t, topo.Owns(0))
}
