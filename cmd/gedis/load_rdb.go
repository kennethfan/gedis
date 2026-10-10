package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/kennethfan/gedis/internal/rdb"
	"github.com/kennethfan/gedis/internal/storage"
)

// loadRDBFile 在 store Open 之后、对外服务之前一次性导入真机 RDB。
// 仅允许空库：库里已有任何 key（含内部键）直接拒绝，避免与在线数据交织。
func loadRDBFile(ctx context.Context, store *storage.Pebble, path string) error {
	has, err := store.HasAny(ctx)
	if err != nil {
		return fmt.Errorf("check empty db: %w", err)
	}
	if has {
		return fmt.Errorf("refusing --load-rdb: database is not empty")
	}
	res, err := rdb.LoadFile(path)
	if err != nil {
		return fmt.Errorf("parse rdb: %w", err)
	}
	stats, err := rdb.Import(ctx, res, store)
	if err != nil {
		return fmt.Errorf("import rdb: %w", err)
	}
	slog.Info("rdb loaded",
		"path", path,
		"loaded", stats.Loaded,
		"expired_skipped", stats.ExpiredSkipped,
		"skipped", len(stats.Skipped),
	)
	return nil
}
