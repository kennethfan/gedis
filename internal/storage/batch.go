package storage

import (
	"context"
	"fmt"

	"github.com/kennethfan/gedis/internal/replication"
)

// BatchOp 是一次批量写入中的单个操作：Delete=true 表删除，否则表写入。
type BatchOp struct {
	Key    []byte
	Value  []byte
	Delete bool
}

// WriteBatch 原子提交一批写入（pebble.Batch，一次 Commit）。空批次直接返回。
// 落盘策略遵循 Fsync 配置：always 用 Sync 提交。
func (p *Pebble) WriteBatch(ctx context.Context, ops []BatchOp) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(ops) == 0 {
		return nil
	}
	b := p.db.NewBatch()
	defer func() { _ = b.Close() }()
	for _, op := range ops {
		if op.Delete {
			if err := b.Delete(op.Key, nil); err != nil {
				return fmt.Errorf("storage: batch delete %q: %w", op.Key, err)
			}
			continue
		}
		if err := b.Set(op.Key, op.Value, nil); err != nil {
			return fmt.Errorf("storage: batch set %q: %w", op.Key, err)
		}
	}
	var delta int64
	seen := make(map[string]struct{}, len(ops))
	for i := len(ops) - 1; i >= 0; i-- {
		op := ops[i]
		k := string(op.Key)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		if op.Delete {
			delta += p.deltaFor(k, 0)
		} else {
			delta += p.deltaFor(k, int64(len(op.Key)+len(op.Value)))
		}
	}
	if err := p.evictForDelta(ctx, delta); err != nil {
		return err
	}
	if err := b.Commit(p.writeOptions()); err != nil {
		return fmt.Errorf("storage: batch commit %d ops: %w", len(ops), err)
	}
	for _, op := range ops {
		if op.Delete {
			p.trackDelete(op.Key)
		} else {
			p.trackSet(op.Key, op.Value)
		}
	}
	if err := p.evictIfNeeded(ctx); err != nil {
		return err
	}
	if p.opts.Hub != nil {
		for _, op := range ops {
			p.opts.Hub.Publish(replication.Op{Del: op.Delete, Key: op.Key, Value: op.Value})
		}
	}
	return nil
}
