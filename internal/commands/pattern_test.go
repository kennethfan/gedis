package commands

import "testing"

// 探针锚定（redis 7.2.6 实测）：h?llo/h*/h[ae]llo/a\*b 对 hello/hllo/hallo/hxllo/a*b/ab 的匹配矩阵。
func TestMatchPatternProbeMatrix(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"h?llo", "hello", true}, {"h?llo", "hallo", true}, {"h?llo", "hxllo", true},
		{"h?llo", "hllo", false}, {"h?llo", "helllo", false},
		{"h*", "hello", true}, {"h*", "hllo", true}, {"h*", "h", true}, {"h*", "x", false},
		{"h[ae]llo", "hello", true}, {"h[ae]llo", "hallo", true}, {"h[ae]llo", "hxllo", false},
		{`a\*b`, "a*b", true}, {`a\*b`, "ab", false}, {`a\*b`, "axb", false},
	}
	for _, c := range cases {
		if got := matchPattern(c.pattern, c.s); got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}

func TestMatchPatternEdges(t *testing.T) {
	cases := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "", true}, {"*", "anything", true}, {"", "", true}, {"", "a", false},
		{"a*b*c", "abc", true}, {"a*b*c", "axbyc", true}, {"a*b*c", "ac", false},
		{"?", "a", true}, {"?", "", false}, {"??", "ab", true}, {"??", "a", false},
		{"[a-z]", "m", true}, {"[a-z]", "M", false}, {"[^ab]", "c", true}, {"[^ab]", "a", false},
		{"[]a]", "]", true}, {"[]a]", "a", true}, {"[]a]", "b", false},
		{`a\?b`, "a?b", true}, {`a\?b`, "axb", false},
		{"a[b", "a[b", true}, {"a[b", "axb", false}, // 未闭合 '[' 字面量
		{"HELLO", "hello", false}, // 大小写敏感
		{"*x", "x", true}, {"x*", "x", true}, {"**", "ab", true},
	}
	for _, c := range cases {
		if got := matchPattern(c.pattern, c.s); got != c.want {
			t.Errorf("matchPattern(%q, %q) = %v, want %v", c.pattern, c.s, got, c.want)
		}
	}
}
