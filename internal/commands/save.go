package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/rdb"
	"github.com/kennethfan/gedis/internal/storage"
)

// SaveTarget 是落盘读取面：优先快照一致读，不支持快照的 Store 回退 live 迭代。
type SaveTarget interface {
	KV
	NewSnapshot() (*storage.Snapshot, error)
}

// snapshotter 断言快照能力：不断言成败，只决定一致读还是 live 回退。
type snapshotter interface {
	NewSnapshot() (*storage.Snapshot, error)
}

const DefaultSaveFileName = "dump.rdb"

// SaveTracker 持有落盘状态：目录/文件名、上次成功 save 的 unix 秒、
// 后台 save 在飞标记与最近一次后台结果。零值不可用，用 NewSaveTracker 构造。
type SaveTracker struct {
	mu            sync.Mutex
	dir           string
	file          string
	target        SaveTarget
	lastSaveUnix  int64
	bgsaveRunning bool
	// lastStatus 是最近一次后台 save 结果：ok 或 err。
	lastStatus string
}

// NewSaveTracker 返回绑定目录与读取面的 tracker；lastSaveUnix 初值为 startUnix
//（LASTSAVE 在从未 save 时与旧行为一致）。
func NewSaveTracker(dir string, target SaveTarget, startUnix int64) *SaveTracker {
	return &SaveTracker{dir: dir, file: DefaultSaveFileName, target: target, lastSaveUnix: startUnix, lastStatus: "ok"}
}

// SetFileName 切换落盘文件名（CONFIG SET dbfilename 接线用）。
func (t *SaveTracker) SetFileName(name string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.file = name
}

// FileName 返回当前落盘文件名（CONFIG GET dbfilename 接线用）。
func (t *SaveTracker) FileName() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.file
}

// LastSave 返回上次成功 save 的 unix 秒（LASTSAVE 接线用）。
func (t *SaveTracker) LastSave() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.lastSaveUnix
}

// BGSAVEState 返回后台标记与最近结果（INFO 接线用）。
func (t *SaveTracker) BGSAVEState() (running bool, lastStatus string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.bgsaveRunning, t.lastStatus
}

// savePath 拼落盘完整路径。
func (t *SaveTracker) savePath() string {
	return filepath.Join(t.dir, t.file)
}

// collectEntry 把一条原始记录转为 rdb.Entry：Decode 定类型后按类型组装；
// 解码失败返回 false。x: 前缀的 hash field 过期 sidecar（TypeHash）在此
// 被丢弃：v11 无 hash-field-TT 编码（见导出偏差）。
func collectEntry(raw []byte) (rdb.Entry, bool) {
	ent, err := datastruct.Decode(raw)
	if err != nil {
		return rdb.Entry{}, false
	}
	out := rdb.Entry{}
	if ent.Expiry != 0 {
		out.ExpireMs = ent.Expiry / int64(time.Millisecond)
	}
	switch ent.Type {
	case datastruct.TypeString:
		out.Type = rdb.TypeString
		out.Str = ent.Payload
	case datastruct.TypeHash:
		out.Type = rdb.TypeHash
		m, _, err := datastruct.DecodeHash(ent.Payload)
		if err != nil {
			return rdb.Entry{}, false
		}
		out.Hash = m
	case datastruct.TypeList:
		out.Type = rdb.TypeList
		l, err := datastruct.DecodeList(ent.Payload)
		if err != nil {
			return rdb.Entry{}, false
		}
		out.List = l
	case datastruct.TypeSet:
		out.Type = rdb.TypeSet
		m, _, err := datastruct.DecodeSet(ent.Payload)
		if err != nil {
			return rdb.Entry{}, false
		}
		out.Set = make([]string, 0, len(m))
		for member := range m {
			out.Set = append(out.Set, member)
		}
		sort.Strings(out.Set)
	case datastruct.TypeZSet:
		out.Type = rdb.TypeZSet
		m, _, err := datastruct.DecodeZSet(ent.Payload)
		if err != nil {
			return rdb.Entry{}, false
		}
		for member, score := range m {
			out.ZSet = append(out.ZSet, rdb.MemberScore{Member: member, Score: score})
		}
		sort.Slice(out.ZSet, func(i, j int) bool { return out.ZSet[i].Member < out.ZSet[j].Member })
	case datastruct.TypeHLL:
		out.Type = rdb.TypeHLL
		out.HLL = ent.Payload
	case datastruct.TypeStream:
		out.Type = rdb.TypeStream
		st, err := datastruct.DecodeStream(ent.Payload)
		if err != nil {
			return rdb.Entry{}, false
		}
		out.Stream = streamDataOf(st)
	default:
		return rdb.Entry{}, false
	}
	return out, true
}

// streamDataOf 把内存 stream 转为 RDB 解码形（导出走统一 v3 写端）。
func streamDataOf(st *datastruct.Stream) *rdb.StreamData {
	out := &rdb.StreamData{
		LastMS: st.Last.Ms, LastSeq: st.Last.Seq,
		MaxDeletedMS: st.MaxDeleted.Ms, MaxDeletedSeq: st.MaxDeleted.Seq,
		Added: st.Added,
	}
	if len(st.Entries) > 0 {
		out.FirstMS, out.FirstSeq = st.Entries[0].ID.Ms, st.Entries[0].ID.Seq
	}
	for _, e := range st.Entries {
		fields := make([][2]string, 0, len(e.Fields)/2)
		for i := 0; i+1 < len(e.Fields); i += 2 {
			fields = append(fields, [2]string{e.Fields[i], e.Fields[i+1]})
		}
		out.Entries = append(out.Entries, rdb.StreamEntry{MS: e.ID.Ms, Seq: e.ID.Seq, Fields: fields})
	}
	out.Length = uint64(len(st.Entries))
	for _, g := range st.Groups {
		gd := rdb.StreamGroupData{
			Name: g.Name, LastMS: g.LastID.Ms, LastSeq: g.LastID.Seq,
			EntriesRead: g.EntriesRead, HasEntriesRead: g.HasRead,
		}
		for _, p := range g.PEL {
			gd.Pending = append(gd.Pending, rdb.StreamPending{
				MS: p.ID.Ms, Seq: p.ID.Seq,
				DeliveryTime: p.DeliveryMs, DeliveryCount: p.Count,
				Consumer: p.Consumer,
			})
		}
		for _, c := range g.Consumers {
			gd.Consumers = append(gd.Consumers, rdb.StreamConsumerData{
				Name: c.Name, SeenTime: c.SeenMs, ActiveTime: c.ActiveMs,
			})
		}
		out.Groups = append(out.Groups, gd)
	}
	return out
}

// collectAll 全量收集用户条目：优先快照一致读，不支持快照则 live 迭代；
// 按用户名去重（跨类型同名 first-win，与 allUserKeys 一致：s: 优先），
// 跳过已过期条目，fn: 等非前缀键天然不在扫描范围内。
func (t *SaveTracker) collectAll(ctx context.Context) ([]rdb.Entry, error) {
	t.mu.Lock()
	target := t.target
	t.mu.Unlock()
	if target == nil {
		return nil, fmt.Errorf("commands: save target not set")
	}
	now := time.Now().UnixNano()
	seen := map[string]rdb.Entry{}
	emit := func(rawKey, raw []byte) {
		name, ok := stripTypePrefix(string(rawKey))
		if !ok {
			return
		}
		if _, dup := seen[name]; dup {
			return
		}
		e, ok := collectEntry(raw)
		if !ok {
			return
		}
		if e.ExpireMs != 0 && e.ExpireMs*int64(time.Millisecond) <= now {
			return
		}
		e.Key = name
		seen[name] = e
	}
	if s, ok := interface{}(target).(snapshotter); ok {
		if snap, err := s.NewSnapshot(); err == nil && snap != nil {
			defer snap.Close()
			for _, p := range typePrefixes {
				keys, err := snap.Scan([]byte(p))
				if err != nil {
					return nil, err
				}
				for _, k := range keys {
					v, err := snap.Get(k)
					if err != nil {
						continue
					}
					emit(k, v)
				}
			}
			return mapToSlice(seen), nil
		}
	}
	for _, p := range typePrefixes {
		keys, err := target.Scan(ctx, []byte(p))
		if err != nil {
			return nil, err
		}
		for _, k := range keys {
			v, err := target.Get(ctx, k)
			if err != nil {
				continue
			}
			emit(k, v)
		}
	}
	return mapToSlice(seen), nil
}

// mapToSlice 把去重表转为导出条目（顺序不定，Export 内不依赖顺序）。
func mapToSlice(seen map[string]rdb.Entry) []rdb.Entry {
	out := make([]rdb.Entry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}
	return out
}

// saveFile 全量收集并原子落盘（写临时文件后 rename），成功更新 lastSaveUnix。
// saveTracker 是进程级单例：server.go/monitor.go 的命令入口与 main.go 的
// SetSaveTarget 共用（沿用包级 publisher 先例）。测试 setup 每次重设。
var saveTracker = NewSaveTracker("", nil, 0)

// SetSaveTarget 绑定存储目标与落盘位置：main 启动时调一次，测试 setup 每次调。
// 重置在飞标记与后台结果为初态；lastSaveUnix 不动（由 RegisterServer 经 StartUnix 初始化）。
func SetSaveTarget(target SaveTarget, dir, file string) {
	saveTracker.mu.Lock()
	defer saveTracker.mu.Unlock()
	saveTracker.target = target
	saveTracker.dir = dir
	saveTracker.file = file
	saveTracker.bgsaveRunning = false
	saveTracker.lastStatus = "ok"
}

// resetStartUnix 把 lastSaveUnix 回到服务启动秒：RegisterServer 调一次。
// SAVE/BGSAVE 成功后更新的值不被覆盖（启动与测试 setup 之间无 save）。
func (t *SaveTracker) resetStartUnix(startUnix int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastSaveUnix = startUnix
}

func (t *SaveTracker) saveFile(ctx context.Context) error {
	if t.target == nil {
		return fmt.Errorf("save target not set")
	}
	entries, err := t.collectAll(ctx)
	if err != nil {
		return err
	}
	t.mu.Lock()
	path := t.savePath()
	t.mu.Unlock()
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := rdb.Export(f, entries); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	t.mu.Lock()
	t.lastSaveUnix = time.Now().Unix()
	t.mu.Unlock()
	return nil
}

// saveCmd SAVE：同步落盘，成功回 OK（真机语义）。
func (t *SaveTracker) saveCmd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR wrong number of arguments for 'save' command")
	}
	if err := t.saveFile(ctx); err != nil {
		return errValueStr("ERR " + err.Error())
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// bgsaveCmd BGSAVE：后台落盘，立即回 Background saving started；
// 在飞时回真机原文错（注意 save 非 saving）。
func (t *SaveTracker) bgsaveCmd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 0 {
		return errValueStr("ERR syntax error")
	}
	t.mu.Lock()
	if t.bgsaveRunning {
		t.mu.Unlock()
		return errValueStr("ERR Background save already in progress")
	}
	t.bgsaveRunning = true
	t.mu.Unlock()
	go func() {
		err := t.saveFile(ctx)
		t.mu.Lock()
		defer t.mu.Unlock()
		t.bgsaveRunning = false
		if err != nil {
			t.lastStatus = "err"
		} else {
			t.lastStatus = "ok"
		}
	}()
	return protocol.Value{Kind: protocol.KindSimpleString, S: "Background saving started"}
}
