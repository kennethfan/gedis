package commands

import (
	"github.com/kennethfan/gedis/internal/network"
)

// WriteCommandSet 是全部变更命令集合（只读副本模式拒绝这些）。
// REPLICAOF/PSYNC/SLOWLOG 等管理命令不在其中。
func WriteCommandSet() map[string]bool {
	names := []string{
		"SET", "GETDEL", "GETEX", "GETSET", "SETEX", "PSETEX", "SETNX", "DEL", "UNLINK",
		"MSET", "MSETNX",
		"INCR", "INCRBY", "DECR", "DECRBY", "INCRBYFLOAT",
		"APPEND", "SETRANGE",
		"HSET", "HMSET", "HDEL", "HSETNX", "HINCRBY", "HINCRBYFLOAT",
		"SWAPDB",
		"HEXPIRE", "HEXPIREAT", "HPEXPIRE", "HPEXPIREAT", "HPERSIST",
		"EVAL", "EVALSHA",
		"MIGRATE", "RESTORE",
		"LPUSH", "RPUSH", "LPOP", "RPOP",
		"LMOVE", "BLMOVE", "RPOPLPUSH", "BRPOPLPUSH",
		"LSET", "LINSERT", "LREM", "LTRIM",
		"BLPOP", "BRPOP", "BLMPOP", "BRMPOP", "LMPOP",
		"BZMPOP", "ZMPOP",
		"SADD", "SREM", "SPOP", "SMOVE",
		"SINTERSTORE", "SUNIONSTORE", "SDIFFSTORE",
		"GEOADD", "GEOSEARCHSTORE", "GEORADIUS", "GEORADIUSBYMEMBER",
		"COPY", "RENAME", "RENAMENX", "SORT", "MOVE",
		"FLUSHDB", "FLUSHALL",
		"FCALL", "FUNCTION",
		"SETBIT", "BITOP", "BITFIELD",
		"PFADD", "PFMERGE",
		"XADD", "XTRIM", "XDEL", "XGROUP", "XSETID",
		"XREADGROUP", "XACK", "XCLAIM", "XAUTOCLAIM",
		"ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
		"BZPOPMAX", "BZPOPMIN",
		"ZDIFFSTORE", "ZINTERSTORE", "ZUNIONSTORE", "ZRANGESTORE",
		"ZREMRANGEBYLEX", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
		"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST",
	}
	out := make(map[string]bool, len(names))
	for _, n := range names {
		out[n] = true
	}
	return out
}

// RegisterWriteCommands 把写命令集合装进 Router（main 启动时调一次）。
func RegisterWriteCommands(r *network.Router) {
	r.SetWriteCommands(WriteCommandSet())
}
