package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Metrics_when_Exposition(t *testing.T) {
	s := network.NewStats()
	r := network.NewRouter()
	r.AttachStats(s)
	for i := 0; i < 3; i++ {
		r.Dispatch(context.Background(), protocol.Value{Kind: protocol.KindArray, Elems: []protocol.Value{
			protocol.BulkOf("NOPE"),
		}})
	}

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	Handler(s).ServeHTTP(rec, req)

	resp := rec.Result()
	require.Equal(t, "text/plain; version=0.0.4", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	out := string(body)
	for _, marker := range []string{
		"# HELP redrock_commands_processed_total",
		"# TYPE redrock_commands_processed_total counter",
		"redrock_commands_processed_total 3",
		"redrock_connected_clients",
		"redrock_used_memory_bytes",
	} {
		require.True(t, strings.Contains(out, marker), "missing %q", marker)
	}
}
