package replication

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_Hub_when_PublishSubscribe(t *testing.T) {
	h := NewHub(16)
	id, ch := h.Subscribe()
	defer h.Unsubscribe(id)

	off := h.Publish(Op{Key: []byte("k"), Value: []byte("v")})
	require.Equal(t, int64(1), off)
	got := <-ch
	require.Len(t, got, 1)
	require.Equal(t, []byte("k"), got[0].Key)
	require.Equal(t, int64(1), h.SubCount())
}

func Test_Hub_when_SlowSubDropped(t *testing.T) {
	h := NewHub(16)
	id, ch := h.Subscribe()
	for i := 0; i < 512; i++ {
		h.Publish(Op{Key: []byte("k")})
	}
	require.Equal(t, int64(0), h.SubCount())
	for i := 0; i < 256; i++ {
		<-ch
	}
	_, ok := <-ch
	require.False(t, ok)
	_ = id
}
