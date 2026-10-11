package commands

import (
	"context"
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Given: 静态哨兵拓扑（mymaster）
// When: SENTINEL HELP / INFO-CACHE / PENDING-SCRIPTS / SIMULATE-FAILURE
// Then: HELP 49 行探针原文；INFO-CACHE 已知回 [INFO] 未知回空；
// PENDING 空；SIMULATE 无参 arity 错、HELP 回小写 flag 表、带 flag 诚实错
func Test_Sentinel_when_Introspection(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	ctx := context.Background()

	help := r.Dispatch(ctx, sentinelCmd("SENTINEL", "HELP"))
	require.Equal(t, protocol.KindArray, help.Kind)
	require.Len(t, help.Elems, 49)
	require.Equal(t, "SENTINEL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", help.Elems[0].S)
	require.Equal(t, "    Print this help.", help.Elems[48].S)

	bad := r.Dispatch(ctx, sentinelCmd("SENTINEL", "HELP", "X"))
	require.Equal(t, protocol.KindError, bad.Kind)
	require.Contains(t, bad.S, "sentinel|help")

	hit := r.Dispatch(ctx, sentinelCmd("SENTINEL", "INFO-CACHE", "mymaster"))
	require.Equal(t, protocol.KindArray, hit.Kind)
	require.Len(t, hit.Elems, 1)
	require.Contains(t, string(hit.Elems[0].Bulk), "sentinel_masters")

	miss := r.Dispatch(ctx, sentinelCmd("SENTINEL", "INFO-CACHE", "nope"))
	require.Equal(t, protocol.KindArray, miss.Kind)
	require.Empty(t, miss.Elems)

	noarg := r.Dispatch(ctx, sentinelCmd("SENTINEL", "INFO-CACHE"))
	require.Equal(t, protocol.KindError, noarg.Kind)
	require.Contains(t, noarg.S, "sentinel|info-cache")

	pending := r.Dispatch(ctx, sentinelCmd("SENTINEL", "PENDING-SCRIPTS"))
	require.Equal(t, protocol.KindArray, pending.Kind)
	require.Empty(t, pending.Elems)

	noflag := r.Dispatch(ctx, sentinelCmd("SENTINEL", "SIMULATE-FAILURE"))
	require.Equal(t, protocol.KindError, noflag.Kind)
	require.Contains(t, noflag.S, "sentinel|simulate-failure")

	simHelp := r.Dispatch(ctx, sentinelCmd("SENTINEL", "SIMULATE-FAILURE", "HELP"))
	require.Equal(t, protocol.KindArray, simHelp.Kind)
	require.Len(t, simHelp.Elems, 2)
	require.Equal(t, "crash-after-election", string(simHelp.Elems[0].Bulk))
	require.Equal(t, "crash-after-promotion", string(simHelp.Elems[1].Bulk))

	crash := r.Dispatch(ctx, sentinelCmd("SENTINEL", "SIMULATE-FAILURE", "CRASH-AFTER-ELECTION"))
	require.Equal(t, protocol.KindError, crash.Kind)
	require.Contains(t, crash.S, "refuses to crash")
}

// Given: 同上拓扑
// When: SENTINEL CONFIG GET/SET（含未知参）与 DEBUG 裸/SET（含未知项）
// Then: GET 未知回空 entry；SET 未知回 Invalid argument；裸 CONFIG 报 arity；
// DEBUG 裸回 13 对默认表；SET 已知 OK 且回读生效；未知项回 Unknown option
func Test_Sentinel_when_ConfigDebug(t *testing.T) {
	r, _ := newSentinelRouter("127.0.0.1:6380", "127.0.0.1:6381", nil)
	ctx := context.Background()

	bare := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG"))
	require.Equal(t, protocol.KindError, bare.Kind)
	require.Contains(t, bare.S, "sentinel|config")

	getNew := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG", "GET", "announce-ip"))
	require.Equal(t, protocol.KindArray, getNew.Kind)
	require.Len(t, getNew.Elems, 1)
	require.Len(t, getNew.Elems[0].Elems, 2)

	getUnknown := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG", "GET", "bogus-param"))
	require.Equal(t, protocol.KindArray, getUnknown.Kind)
	require.Empty(t, getUnknown.Elems)

	setOK := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG", "SET", "announce-ip", "10.0.0.1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, setOK)
	getBack := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG", "GET", "announce-ip"))
	require.Equal(t, "10.0.0.1", string(getBack.Elems[0].Elems[1].Bulk))

	setBad := r.Dispatch(ctx, sentinelCmd("SENTINEL", "CONFIG", "SET", "bogus-param", "1"))
	require.Equal(t, protocol.KindError, setBad.Kind)
	require.Contains(t, setBad.S, "Invalid argument 'bogus-param'")

	dbg := r.Dispatch(ctx, sentinelCmd("SENTINEL", "DEBUG"))
	require.Equal(t, protocol.KindArray, dbg.Kind)
	require.Len(t, dbg.Elems, 26)
	require.Equal(t, "INFO-PERIOD", string(dbg.Elems[0].Bulk))
	require.Equal(t, "10000", string(dbg.Elems[1].Bulk))

	setDbg := r.Dispatch(ctx, sentinelCmd("SENTINEL", "DEBUG", "INFO-PERIOD", "5000"))
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, setDbg)
	dbg2 := r.Dispatch(ctx, sentinelCmd("SENTINEL", "DEBUG"))
	require.Equal(t, "5000", string(dbg2.Elems[1].Bulk))

	setDbgBad := r.Dispatch(ctx, sentinelCmd("SENTINEL", "DEBUG", "BOGUSPARAM", "1"))
	require.Equal(t, protocol.KindError, setDbgBad.Kind)
	require.Contains(t, setDbgBad.S, "Unknown option")
}

// Given: 普通 server 实例
// When: CLIENT/MEMORY/LATENCY/COMMAND 四组 HELP 与行为边角
// Then: 行数与首尾探针对齐；DOCTOR 回 Sam/Dave 文；PURGE OK；
// STATS 诚实错；GRAPH 无样本错；HISTOGRAM 空；COMMAND HELP 21 行
func Test_Server_when_HelpIntrospection(t *testing.T) {
	r, _, _, _, _ := openServerSetup(t)

	ch := dispatch(r, "CLIENT", "HELP")
	require.Equal(t, protocol.KindArray, ch.Kind)
	require.Len(t, ch.Elems, 54)
	require.Equal(t, "CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", ch.Elems[0].S)
	require.Equal(t, "    Print this help.", ch.Elems[53].S)
	chBad := dispatch(r, "CLIENT", "HELP", "X")
	require.Equal(t, protocol.KindError, chBad.Kind)
	require.Contains(t, chBad.S, "client|help")

	mh := dispatch(r, "MEMORY", "HELP")
	require.Equal(t, protocol.KindArray, mh.Kind)
	require.Len(t, mh.Elems, 14)
	require.Equal(t, "    Print this help.", mh.Elems[13].S)
	doc := dispatch(r, "MEMORY", "DOCTOR")
	require.Equal(t, protocol.KindBulkString, doc.Kind)
	require.Contains(t, string(doc.Bulk), "Hi Sam")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "MEMORY", "PURGE"))
	stats := dispatch(r, "MEMORY", "STATS")
	require.Equal(t, protocol.KindError, stats.Kind)
	require.Contains(t, stats.S, "not supported")

	lh := dispatch(r, "LATENCY", "HELP")
	require.Equal(t, protocol.KindArray, lh.Kind)
	require.Len(t, lh.Elems, 17)
	ldoc := dispatch(r, "LATENCY", "DOCTOR")
	require.Equal(t, protocol.KindBulkString, ldoc.Kind)
	require.Contains(t, string(ldoc.Bulk), "Dave")
	graph := dispatch(r, "LATENCY", "GRAPH", "command")
	require.Equal(t, protocol.KindError, graph.Kind)
	require.Contains(t, graph.S, "No samples available for event 'command'")
	hist := dispatch(r, "LATENCY", "HISTOGRAM")
	require.Equal(t, protocol.KindArray, hist.Kind)
	require.Empty(t, hist.Elems)

	cmdh := dispatch(r, "COMMAND", "HELP")
	require.Equal(t, protocol.KindArray, cmdh.Kind)
	require.Len(t, cmdh.Elems, 21)
	require.Equal(t, "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", cmdh.Elems[0].S)
	require.Equal(t, "    Print this help.", cmdh.Elems[20].S)
	cmdhBad := dispatch(r, "COMMAND", "HELP", "X")
	require.Equal(t, protocol.KindError, cmdhBad.Kind)
	require.Contains(t, cmdhBad.S, "command|help")
}
