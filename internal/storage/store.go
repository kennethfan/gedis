// Package storage 定义 Redrock 的存储 seam：上层只依赖 Store 接口。
package storage

import (
	"context"
	"errors"
)

// ErrNotFound 表示 key 不存在，调用方用 errors.Is 判定。
var ErrNotFound = errors.New("storage: key not found")

// Store 是存储引擎的公共边界。ctx 必须是第一个参数。
type Store interface {
	Open() error
	Close() error
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}
