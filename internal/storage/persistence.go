package storage

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/kennethfan/gedis/internal/replication"
)

// FsyncPolicy 是写入落盘策略：always 每次写同步，everysec 每秒刷盘，
// no 完全交给 OS。映射到 Pebble 的 WriteOptions 与后台 Flush。
type FsyncPolicy string

const (
	FsyncAlways   FsyncPolicy = "always"
	FsyncEverysec FsyncPolicy = "everysec"
	FsyncNo       FsyncPolicy = "no"
)

// ParseFsyncPolicy 解析配置中的 fsync 字符串，大小写不敏感。
func ParseFsyncPolicy(s string) (FsyncPolicy, error) {
	switch FsyncPolicy(strings.ToLower(s)) {
	case FsyncAlways:
		return FsyncAlways, nil
	case FsyncEverysec:
		return FsyncEverysec, nil
	case FsyncNo:
		return FsyncNo, nil
	default:
		return "", fmt.Errorf("storage: unknown fsync policy %q", s)
	}
}

// Options 是持久化选项：AppendOnly=false 关闭 WAL（仅 memtable，
// 崩溃丢数据）；Fsync 控制每次写入是否同步；Hub 非空时每次成功写入
// 向其发布 raw KV 操作（复制扇出）。
type Options struct {
	AppendOnly bool
	Fsync      FsyncPolicy
	Hub        *replication.Hub
}

// DefaultOptions 是默认持久化选项：WAL 开，落盘交给 OS。
func DefaultOptions() Options {
	return Options{AppendOnly: true, Fsync: FsyncNo}
}

// NewWithOptions 返回带持久化选项的 Pebble 存储。Open 之前不可读写。
func NewWithOptions(path string, opts Options) *Pebble {
	return &Pebble{path: path, opts: opts}
}

// writeOptions 按 Fsync 策略返回写入选项；everysec 的定时刷盘由后台
// flusher 负责（见 slice2），写入本身不同步。
func (p *Pebble) writeOptions() *pebble.WriteOptions {
	if p.opts.Fsync == FsyncAlways {
		return pebble.Sync
	}
	return pebble.NoSync
}
