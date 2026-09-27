package commands

import (
	"testing"

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
