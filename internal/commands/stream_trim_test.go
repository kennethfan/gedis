package commands

import (
	"testing"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/stretchr/testify/require"
)

func Test_Stream_when_TrimMaxlen(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XTRIM", "s", "MAXLEN", "2"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "XLEN", "s"))
	// 无需修剪 → 0。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XTRIM", "s", "MAXLEN", "5"))
	// = 与 ~ 修饰符等价精确修剪。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XTRIM", "s", "MAXLEN", "=", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XTRIM", "s", "MAXLEN", "~", "1"))
	// MAXLEN 0 删光但 key 保留。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XTRIM", "s", "MAXLEN", "0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XLEN", "s"))
	// 缺失 key → 0。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XTRIM", "missing", "MAXLEN", "5"))
	// LIMIT 限制删除数。
	dispatch(r, "XADD", "t", "100-1", "a", "1")
	dispatch(r, "XADD", "t", "100-2", "a", "2")
	dispatch(r, "XADD", "t", "100-3", "a", "3")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XTRIM", "t", "MAXLEN", "~", "1", "LIMIT", "1"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "XLEN", "t"))
}

func Test_Stream_when_TrimMinid(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "XADD", "s", "100-2", "a", "2")
	dispatch(r, "XADD", "s", "100-3", "a", "3")
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 2},
		dispatch(r, "XTRIM", "s", "MINID", "100-3"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XLEN", "s"))
	// 阈值即现存首条 → 0。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XTRIM", "s", "MINID", "100-3"))
	// 超顶阈值删光。
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1},
		dispatch(r, "XTRIM", "s", "MINID", "999-0"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 0},
		dispatch(r, "XLEN", "s"))
}

func Test_Stream_when_TrimErrors(t *testing.T) {
	r, store := openTestSetup(t)
	RegisterStream(r, store, nil)
	RegisterStrings(r, store)
	dispatch(r, "XADD", "s", "100-1", "a", "1")
	dispatch(r, "SET", "str", "v")
	cases := []struct {
		name string
		args []string
		want string
	}{
		{"bare", []string{"XTRIM"}, "wrong number of arguments for 'xtrim'"},
		{"onearg", []string{"XTRIM", "s"}, "wrong number of arguments for 'xtrim'"},
		{"bogus", []string{"XTRIM", "s", "BOGUS", "1"}, "syntax error"},
		{"negmaxlen", []string{"XTRIM", "s", "MAXLEN", "-5"}, "MAXLEN argument must be >= 0"},
		{"badmaxlen", []string{"XTRIM", "s", "MAXLEN", "x"}, "not an integer or out of range"},
		{"badminid", []string{"XTRIM", "s", "MINID", "bad"}, "Invalid stream ID"},
		{"limitnoapprox", []string{"XTRIM", "s", "MAXLEN", "1", "LIMIT", "2"}, "LIMIT cannot be used without the special ~ option"},
		{"limitdangle", []string{"XTRIM", "s", "MAXLEN", "~", "1", "LIMIT"}, "syntax error"},
		{"wrongtype", []string{"XTRIM", "str", "MAXLEN", "1"}, "WRONGTYPE"},
	}
	for _, c := range cases {
		got := dispatch(r, c.args...)
		require.Equal(t, protocol.KindError, got.Kind, c.name)
		require.Contains(t, got.S, c.want, c.name)
	}
}
