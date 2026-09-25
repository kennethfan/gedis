package cluster

import (
	"strconv"
	"strings"
)

// key→slot 抽取表：intercept / EXEC 预扫 / EVAL 入口共用（#39 决议）。
// 叶子包：只收 []string，不碰 protocol，保持零内部依赖。
//
// KeysOf 返回命令触及的 key 名；ok=false 表示无 key 路由（直通）：
//   - 非 key 命令（PING/INFO/CLUSTER/ASKING/MULTI/EXEC…）
//   - Pub/Sub（#39：豁免）
//   - 参数形状非法（个数不够、numkeys 非法）——handler 侧照常报错
//   - 未收录的新命令（默认放行，见 default 分支注释）
//
// 多 key 命令返回全部 key，由调用方做同槽校验（CROSSSLOT）。
func KeysOf(cmd string, args []string) ([]string, bool) {
	switch cmd {
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO":
		return evalKeys(args)
	case "XREAD", "XREADGROUP":
		return xreadKeys(args)
	case "SORT":
		return sortKeys(args)
	case "MIGRATE":
		if len(args) >= 3 && args[2] != "" {
			return []string{args[2]}, true
		}
		return nil, false
	case "OBJECT", "MEMORY":
		// OBJECT <sub> key / MEMORY USAGE key
		if len(args) >= 2 && args[1] != "" {
			return []string{args[1]}, true
		}
		return nil, false
	case "XGROUP":
		// XGROUP <sub> key …
		if len(args) >= 2 && args[1] != "" {
			return []string{args[1]}, true
		}
		return nil, false
	case "XINFO":
		// XINFO <sub> key …（HELP 无 key，len<2 直通）
		if len(args) >= 2 && args[1] != "" {
			return []string{args[1]}, true
		}
		return nil, false
	case "BITOP":
		// BITOP op dest srckey…：dest 与源必须同槽
		return allNonEmpty(args)
	case "COPY", "RENAME", "RENAMENX":
		return allNonEmpty(args[:min(2, len(args))])
	case "SMOVE":
		if len(args) >= 2 {
			return allNonEmpty(args[:2])
		}
		return nil, false
	case "DEL", "UNLINK", "EXISTS", "TOUCH", "MGET":
		return allNonEmpty(args)
	case "MSET", "MSETNX":
		// k v 对：只取偶数位
		var out []string
		for i := 0; i+1 < len(args); i += 2 {
			if args[i] == "" {
				return nil, false
			}
			out = append(out, args[i])
		}
		if len(out) == 0 {
			return nil, false
		}
		return out, true
	case "PFCOUNT":
		// 单 key 是基数查询，多 key 需同槽
		return allNonEmpty(args)
	case "SINTER", "SUNION", "SDIFF",
		"SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
		"SINTERCARD":
		return allNonEmpty(args)
	case "ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE":
		// dest numkeys key…：dest 也须同槽
		if len(args) < 3 {
			return nil, false
		}
		n, err := strconv.Atoi(args[1])
		if err != nil || n < 1 || len(args) < 2+n {
			return nil, false
		}
		return allNonEmpty(append([]string{args[0]}, args[2:2+n]...))
	case "ZUNION", "ZINTER", "ZDIFF":
		if len(args) < 2 {
			return nil, false
		}
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || len(args) < 1+n {
			return nil, false
		}
		return allNonEmpty(args[1 : 1+n])
	case "LMPOP", "BLMPOP":
		// LMPOP numkeys key…：与 ZUNION 同形（无 dest）
		if len(args) < 2 {
			return nil, false
		}
		n, err := strconv.Atoi(args[0])
		if err != nil || n < 1 || len(args) < 1+n {
			return nil, false
		}
		return allNonEmpty(args[1 : 1+n])
	case "SUBSCRIBE", "UNSUBSCRIBE", "PUBLISH", "PUBSUB",
		"SSUBSCRIBE", "SUNSUBSCRIBE", "SPUBLISH",
		"PSUBSCRIBE", "PUNSUBSCRIBE":
		// #39：Pub/Sub 豁免
		return nil, false
	case "CLUSTER", "ASKING", "MULTI", "EXEC", "DISCARD",
		"WATCH", "UNWATCH", "RESET", "COMMAND", "CONFIG":
		return nil, false
	default:
		if singleFirstCmd(cmd) {
			if len(args) >= 1 && args[0] != "" {
				return []string{args[0]}, true
			}
			return nil, false
		}
		// 未收录命令默认放行：新命令在登记 key-spec 前不被误杀；
		// 误放行的代价是错槽不报错，误拦截的代价是正常命令被拒绝——
		// 前者可接受（minimal 阶段），后者不可接受。
		return nil, false
	}
}

// evalKeys 按 EVAL numkeys 语义取 key；非法形状直通（handler 报参数错）。
func evalKeys(args []string) ([]string, bool) {
	if len(args) < 1 {
		return nil, false
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n < 0 || len(args) < 1+n {
		return nil, false
	}
	if n == 0 {
		return nil, false
	}
	return allNonEmpty(args[1 : 1+n])
}

// xreadKeys 取 STREAMS 之后前半段 key；无 STREAMS 直通。
func xreadKeys(args []string) ([]string, bool) {
	at := -1
	for i, a := range args {
		if strings.ToUpper(a) == "STREAMS" {
			at = i
			break
		}
	}
	if at < 0 {
		return nil, false
	}
	rest := args[at+1:]
	if len(rest) == 0 || len(rest)%2 != 0 {
		return nil, false
	}
	return allNonEmpty(rest[:len(rest)/2])
}

// sortKeys 取被排序 key；STORE dest 一并约束。
func sortKeys(args []string) ([]string, bool) {
	if len(args) < 1 || args[0] == "" {
		return nil, false
	}
	out := []string{args[0]}
	for i := 1; i < len(args); i++ {
		if strings.ToUpper(args[i]) == "STORE" {
			if i+1 >= len(args) || args[i+1] == "" {
				return nil, false
			}
			out = append(out, args[i+1])
			break
		}
	}
	return out, true
}

// allNonEmpty 全为 key；空表或空串直通（handler 报错）。
func allNonEmpty(args []string) ([]string, bool) {
	if len(args) == 0 {
		return nil, false
	}
	for _, a := range args {
		if a == "" {
			return nil, false
		}
	}
	return args, true
}

// singleFirstCmd 是 key 在 args[0] 的命令集合（GET/SET/H*/*SCAN 族…）。
func singleFirstCmd(cmd string) bool {
	switch cmd {
	case "GET", "SET", "SETNX", "GETSET", "GETDEL", "GETEX",
		"SETEX", "PSETEX", "SETRANGE", "GETRANGE", "STRLEN", "APPEND",
		"INCR", "DECR", "INCRBY", "DECRBY", "INCRBYFLOAT",
		"HSET", "HGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HDEL",
		"HEXISTS", "HMGET", "HSETNX", "HINCRBY", "HINCRBYFLOAT", "HSTRLEN", "HRANDFIELD",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LLEN",
		"LRANGE", "LINDEX", "LSET", "LTRIM", "LREM", "LINSERT", "LMOVE",
		"RPOPLPUSH", "BRPOPLPUSH", "BLPOP", "BRPOP", "BLMOVE",
		"SADD", "SREM", "SCARD", "SISMEMBER", "SMEMBERS", "SMISMEMBER",
		"SPOP", "SRANDMEMBER",
		"ZADD", "ZREM", "ZSCORE", "ZRANK", "ZREVRANK", "ZCARD", "ZCOUNT",
		"ZINCRBY", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE",
		"ZREMRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYLEX", "ZRANGEBYLEX",
		"ZREVRANGEBYLEX", "ZLEXCOUNT", "ZPOPMIN", "ZPOPMAX", "ZRANDMEMBER",
		"ZRANGESTORE", "ZMSCORE",
		"PFADD", "PFMERGE",
		"GETBIT", "SETBIT", "BITCOUNT", "BITPOS", "BITFIELD", "BITFIELD_RO",
		"EXPIRE", "EXPIREAT", "PEXPIRE", "PEXPIREAT", "PERSIST",
		"TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME",
		"TYPE", "DUMP", "RESTORE", "MOVE", "KEYS",
		"XADD", "XLEN", "XRANGE", "XREVRANGE", "XACK", "XCLAIM",
		"XAUTOCLAIM", "XPENDING", "XTRIM", "XDEL", "XSETID",
		"GEOADD", "GEODIST", "GEOHASH", "GEOPOS", "GEORADIUS",
		"GEORADIUSBYMEMBER", "GEOSEARCH", "GEOSEARCHSTORE",
		"SORT_RO",
		"DELEX", "GETDELEX":
		return true
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
