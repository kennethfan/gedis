package commands

// matchPattern 按 Redis stringmatchlen 语义做 glob 匹配（PSUBSCRIBE 通配）：
//   - '*' 匹配任意字节序列（含空）；'?' 精确匹配一个字节
//   - '[...]' 字符类，支持 a-z 范围与 [^...] 取反；'\' 转义（类内外均可）
//
// 大小写敏感、字节级（与 C 实现一致；多字节 UTF-8 下 '?' 匹配一个字节而非一个 rune）。
// 未闭合的 '[' 按字面量处理（真机为边界回退，v1 取确定性简化）。
func matchPattern(pattern, s string) bool {
	return matchBytes([]byte(pattern), []byte(s))
}

func matchBytes(p, s []byte) bool {
	pi, si := 0, 0
	star, mark := -1, 0
	// 失配时回溯到最近 '*' 让它多吞一个字节；无 '*' 可回则整体失败。
	fail := func() bool {
		if star < 0 {
			return false
		}
		mark++
		si = mark
		pi = star + 1
		return true
	}
	for si < len(s) {
		switch {
		case pi < len(p) && p[pi] == '*':
			star, mark = pi, si
			pi++
		case pi < len(p) && p[pi] == '?':
			pi++
			si++
		case pi+1 < len(p) && p[pi] == '\\':
			if p[pi+1] == s[si] {
				pi += 2
				si++
			} else if !fail() {
				return false
			}
		case pi < len(p) && p[pi] == '[':
			end := scanClassEnd(p, pi)
			switch {
			case end < 0 && p[pi] == s[si]: // 未闭合 '[' 按字面量
				pi++
				si++
			case end >= 0 && classMatch(p[pi+1:end], s[si]):
				pi = end + 1
				si++
			default:
				if !fail() {
					return false
				}
			}
		case pi < len(p) && p[pi] == s[si]:
			pi++
			si++
		default:
			if !fail() {
				return false
			}
		}
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

// scanClassEnd 找配对 ']' 下标（跳过 '\' 转义；首字符 ']' 视为字面量，如 []a]）。
// 无配对返回 -1。
func scanClassEnd(p []byte, pi int) int {
	i := pi + 1
	if i < len(p) && p[i] == ']' {
		i++
	}
	for i < len(p) {
		if p[i] == '\\' {
			i += 2
			continue
		}
		if p[i] == ']' {
			return i
		}
		i++
	}
	return -1
}

// classMatch 匹配类体（'[' 与 ']' 之间内容）与单个字节 c：范围 a-z、[^...] 取反、'\' 转义。
func classMatch(body []byte, c byte) bool {
	i := 0
	neg := false
	if i < len(body) && body[i] == '^' {
		neg = true
		i++
	}
	matched := false
	for i < len(body) {
		var lo, hi byte
		switch {
		case body[i] == '\\' && i+1 < len(body):
			lo, hi = body[i+1], body[i+1]
			i += 2
		case i+2 < len(body) && body[i+1] == '-' && body[i+2] != ']':
			lo, hi = body[i], body[i+2]
			i += 3
		default:
			lo, hi = body[i], body[i]
			i++
		}
		if lo <= c && c <= hi {
			matched = true
		}
	}
	if neg {
		return !matched
	}
	return matched
}
