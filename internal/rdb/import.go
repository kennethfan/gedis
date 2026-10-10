package rdb

import (
	"context"
	"fmt"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/storage"
)

// BatchWriter 是导入器的批量写入槽（*storage.Pebble 的 WriteBatch 满足）。
type BatchWriter interface {
	WriteBatch(ctx context.Context, ops []storage.BatchOp) error
}

// importBatchSize 是单次 WriteBatch 的条目上限（大 RDB 分批提交）。
const importBatchSize = 1000

// ImportStats 是 Import 的计数。Skipped 复用解析层的 Skipped 类型
// （如 sparse HLL，见下）。
type ImportStats struct {
	Loaded         int
	ExpiredSkipped int
	Skipped        []Skipped
}

// Import 把解析结果转入存储：RDB 条目 → datastruct 编码 → codec 存储单元。
// 过期时间 ms→ns（unixnano，codec 头 8 字节大端）；导入时刻已过期的跳过
// （真机加载同语义），不报错。调用方负责 LoadFile（多 DB/版本/checksum 已
// 在解析层拒载）。
func Import(ctx context.Context, res *Result, w BatchWriter) (*ImportStats, error) {
	st := &ImportStats{}
	now := time.Now().UnixNano()
	ops := make([]storage.BatchOp, 0, importBatchSize)
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := w.WriteBatch(ctx, ops); err != nil {
			return err
		}
		ops = ops[:0]
		return nil
	}
	for i := range res.Entries {
		e := &res.Entries[i]
		var expNs int64
		if e.ExpireMs > 0 {
			expNs = e.ExpireMs * 1e6
			if expNs <= now {
				st.ExpiredSkipped++
				continue
			}
		}
		key, val, err := encodeEntry(e, expNs)
		if err != nil {
			return nil, err
		}
		ops = append(ops, storage.BatchOp{Key: key, Value: val})
		if len(ops) >= importBatchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
		st.Loaded++
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return st, nil
}

// encodeEntry 把单条目映射为（存储键，存储单元）。
func encodeEntry(e *Entry, expNs int64) ([]byte, []byte, error) {
	switch e.Type {
	case TypeString:
		return datastruct.StringKey(e.Key), datastruct.EncodeString(e.Str, expNs), nil
	case TypeHash:
		return datastruct.HashKey(e.Key),
			datastruct.Encode(datastruct.TypeHash, expNs, datastruct.EncodeHash(e.Hash)), nil
	case TypeList:
		return datastruct.ListKey(e.Key),
			datastruct.Encode(datastruct.TypeList, expNs, datastruct.EncodeList(e.List)), nil
	case TypeSet:
		return datastruct.SetKey(e.Key),
			datastruct.Encode(datastruct.TypeSet, expNs, datastruct.EncodeSet(e.Set)), nil
	case TypeZSet:
		m := make(map[string]float64, len(e.ZSet))
		for _, ms := range e.ZSet {
			m[ms.Member] = ms.Score
		}
		return datastruct.ZSetKey(e.Key),
			datastruct.Encode(datastruct.TypeZSet, expNs, datastruct.EncodeZSet(m)), nil
	case TypeHLL:
		// RDB 存 HYLL 全 blob：dense 用 DecodeHLL 校验后重组（cached card
		// 归零，真机 RDB 重载同样重算）；sparse（小集合常见形态）先 densify。
		reg, err := datastruct.DecodeHLL(e.HLL)
		if err != nil {
			if reg, err = DensifySparseHLL(e.HLL); err != nil {
				return nil, nil, fmt.Errorf("rdb: key %q: %w", e.Key, err)
			}
		}
		return datastruct.HLLKey(e.Key),
			datastruct.Encode(datastruct.TypeHLL, expNs, datastruct.EncodeHLL(reg)), nil
	case TypeStream:
		return datastruct.StreamKey(e.Key),
			datastruct.Encode(datastruct.TypeStream, expNs, datastruct.EncodeStream(importStream(e.Stream))), nil
	default:
		return nil, nil, fmt.Errorf("rdb: key %q has unhandled type %q", e.Key, e.Type)
	}
}

// importStream 把解码后的 StreamData 装进 datastruct.Stream：条目 ID/字段直拷，
// 组 last/已读计数/PEL owner 关系照搬；consumer active 用 -1 哨兵判有无投递。
func importStream(sd *StreamData) *datastruct.Stream {
	s := &datastruct.Stream{
		Entries:    make([]datastruct.StreamEntry, 0, len(sd.Entries)),
		Last:       datastruct.StreamID{Ms: sd.LastMS, Seq: sd.LastSeq},
		HasLast:    true,
		Added:      sd.Added,
		MaxDeleted: datastruct.StreamID{Ms: sd.MaxDeletedMS, Seq: sd.MaxDeletedSeq},
	}
	for _, en := range sd.Entries {
		fields := make([]string, 0, len(en.Fields)*2)
		for _, fv := range en.Fields {
			fields = append(fields, fv[0], fv[1])
		}
		s.Entries = append(s.Entries, datastruct.StreamEntry{
			ID:     datastruct.StreamID{Ms: en.MS, Seq: en.Seq},
			Fields: fields,
		})
	}
	for _, gd := range sd.Groups {
		g := &datastruct.StreamGroup{
			Name:        gd.Name,
			LastID:      datastruct.StreamID{Ms: gd.LastMS, Seq: gd.LastSeq},
			EntriesRead: gd.EntriesRead,
			HasRead:     gd.HasEntriesRead,
		}
		for _, cd := range gd.Consumers {
			g.Consumers = append(g.Consumers, datastruct.StreamConsumer{
				Name:      cd.Name,
				SeenMs:    cd.SeenTime,
				ActiveMs:  cd.ActiveTime,
				HasActive: cd.ActiveTime != ^uint64(0),
			})
		}
		for _, pd := range gd.Pending {
			g.PEL = append(g.PEL, &datastruct.StreamPEL{
				ID:         datastruct.StreamID{Ms: pd.MS, Seq: pd.Seq},
				Consumer:   pd.Consumer,
				DeliveryMs: pd.DeliveryTime,
				Count:      pd.DeliveryCount,
			})
		}
		s.Groups = append(s.Groups, g)
	}
	return s
}
