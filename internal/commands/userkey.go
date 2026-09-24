package commands

import "strings"

// rawKeyPrefixes 是存储层 raw key 前缀表（按长度降序），WATCH 把复制事件的 raw key 反查回用户 key。
// 只有 WATCH 用它：cross-db 场景用户 key 与反查 key 不一致是已知近似（见 Issue #23）。
var rawKeyPrefixes = []string{"hll:", "st:", "h:", "l:", "s:", "x:", "z:"}

// RawToUserKey 剥一层存储前缀；无前缀返回原串。
func RawToUserKey(raw string) string {
	for _, p := range rawKeyPrefixes {
		if strings.HasPrefix(raw, p) {
			return raw[len(p):]
		}
	}
	return raw
}
