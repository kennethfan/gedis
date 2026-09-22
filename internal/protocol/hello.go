package protocol

import (
	"fmt"
	"strings"
)

// ParseHello 从 HELLO 命令值提取客户端请求的协议版本。无版本参数默认 3，
// 参数可为 bulk "2"/"3" 或 integer 2/3，其余一律报错。
func ParseHello(v Value) (int, error) {
	if v.Kind != KindArray || len(v.Elems) == 0 {
		return 0, fmt.Errorf("protocol: not a HELLO command")
	}
	if !isBulk(v.Elems[0], "hello") {
		return 0, fmt.Errorf("protocol: not a HELLO command")
	}
	if len(v.Elems) == 1 {
		return 3, nil
	}
	switch arg := v.Elems[1]; arg.Kind {
	case KindBulkString:
		switch string(arg.Bulk) {
		case "2":
			return 2, nil
		case "3":
			return 3, nil
		}
	case KindInteger:
		if arg.I == 2 || arg.I == 3 {
			return int(arg.I), nil
		}
	}
	return 0, fmt.Errorf("protocol: unsupported HELLO version")
}

func isBulk(v Value, want string) bool {
	return v.Kind == KindBulkString && strings.EqualFold(string(v.Bulk), want)
}
