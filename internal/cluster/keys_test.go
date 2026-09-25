package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_KeysOf_when_SingleKey(t *testing.T) {
	for _, cmd := range []string{"GET", "SET", "HGET", "XADD", "EXPIRE", "TTL", "TYPE"} {
		keys, ok := KeysOf(cmd, []string{"foo"})
		require.True(t, ok, cmd)
		require.Equal(t, []string{"foo"}, keys)
	}
}

func Test_KeysOf_when_MultiKey(t *testing.T) {
	keys, ok := KeysOf("MSET", []string{"a", "1", "b", "2"})
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, keys)

	keys, ok = KeysOf("DEL", []string{"a", "b"})
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, keys)
}

func Test_KeysOf_when_EvalNumkeys(t *testing.T) {
	keys, ok := KeysOf("EVAL", []string{"2", "a", "b", "x"})
	require.True(t, ok)
	require.Equal(t, []string{"a", "b"}, keys)

	_, ok = KeysOf("EVAL", []string{"0"})
	require.False(t, ok)

	_, ok = KeysOf("EVAL", []string{"nan", "a"})
	require.False(t, ok)
}

func Test_KeysOf_when_XreadStreams(t *testing.T) {
	keys, ok := KeysOf("XREAD", []string{"COUNT", "2", "STREAMS", "s1", "s2", "0", "0"})
	require.True(t, ok)
	require.Equal(t, []string{"s1", "s2"}, keys)

	_, ok = KeysOf("XREAD", []string{"COUNT", "2"})
	require.False(t, ok)
}

func Test_KeysOf_when_SortStore(t *testing.T) {
	keys, ok := KeysOf("SORT", []string{"mylist", "STORE", "out"})
	require.True(t, ok)
	require.Equal(t, []string{"mylist", "out"}, keys)
}

func Test_KeysOf_when_PubSubExempt(t *testing.T) {
	for _, cmd := range []string{"SUBSCRIBE", "PUBLISH", "PUBSUB"} {
		_, ok := KeysOf(cmd, []string{"ch"})
		require.False(t, ok, cmd)
	}
}

func Test_KeysOf_when_KeylessPassthrough(t *testing.T) {
	for _, cmd := range []string{"PING", "CLUSTER", "ASKING", "MULTI", "EXEC", "INFO"} {
		_, ok := KeysOf(cmd, []string{"x"})
		require.False(t, ok, cmd)
	}
}
