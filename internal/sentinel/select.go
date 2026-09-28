package sentinel

import (
	"net"
	"sort"
	"strconv"
	"strings"
)

// SlaveInfo 描述一个候选从节点：Addr 为 ip:port；Priority 缺席默认 100；
// Offset 为 INFO replication 中该从的复制偏移；RunID 缺席为空；State 为
// INFO 中 state= 的值（online/offline），缺席（无 INFO 条目）为空。
type SlaveInfo struct {
	Addr     string
	Priority int
	Offset   int64
	RunID    string
	State    string
}

// SortSlaves 按 priority 升序→offset 降序→runid 升序→addr 升序排序，
// 不改动入参。
func SortSlaves(infos []SlaveInfo) []SlaveInfo {
	out := append([]SlaveInfo(nil), infos...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		if out[i].Offset != out[j].Offset {
			return out[i].Offset > out[j].Offset
		}
		if out[i].RunID != out[j].RunID {
			return out[i].RunID < out[j].RunID
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}

// ParseSlaveInfo 从 INFO replication 文本中按 ip:port 匹配
// slaveN:ip=..,port=..,state=..,offset=.. 行：对上才填 Offset/State；
// Priority 缺席默认 100（另认 priority= 键），RunID 缺席为空（另认 runid= 键）。
func ParseSlaveInfo(addr, infoOut string) SlaveInfo {
	info := SlaveInfo{Addr: addr, Priority: 100}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return info
	}
	for _, line := range strings.Split(infoOut, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if !strings.HasPrefix(line, "slave") {
			continue
		}
		kv := line[strings.Index(line, ":")+1:]
		fields := map[string]string{}
		for _, part := range strings.Split(kv, ",") {
			k, v, ok := strings.Cut(part, "=")
			if ok {
				fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
		if fields["ip"] != host || fields["port"] != port {
			continue
		}
		if off, err := strconv.ParseInt(fields["offset"], 10, 64); err == nil {
			info.Offset = off
		}
		info.State = fields["state"]
		if p, err := strconv.Atoi(fields["priority"]); err == nil {
			info.Priority = p
		}
		if rid, ok := fields["runid"]; ok {
			info.RunID = rid
		}
		return info
	}
	return info
}

// SelectSlave 选出 name 下最优可用从节点：读 Slaves(name) 顺序为 infos 建
// 索引，无 info 条目者补 Priority:100（缺席保留）；SortSlaves 后跳过 IsDown
// 与 State 非 online（缺席 State 为空者保留），首个返回；无可用返回
// ErrNoHealthySlave，未知 name 返回 ErrUnknownMaster。
func (r *Registry) SelectSlave(name string, infos []SlaveInfo) (string, error) {
	r.mu.RLock()
	st, found := r.masters[name]
	if !found {
		r.mu.RUnlock()
		return "", ErrUnknownMaster
	}
	slaves := append([]string(nil), st.spec.Slaves...)
	r.mu.RUnlock()

	byAddr := make(map[string]SlaveInfo, len(infos))
	for _, in := range infos {
		byAddr[in.Addr] = in
	}
	cands := make([]SlaveInfo, 0, len(slaves))
	for _, addr := range slaves {
		if in, ok := byAddr[addr]; ok {
			cands = append(cands, in)
		} else {
			cands = append(cands, SlaveInfo{Addr: addr, Priority: 100})
		}
	}
	for _, c := range SortSlaves(cands) {
		if r.IsDown(c.Addr) {
			continue
		}
		if c.State != "" && c.State != "online" {
			continue
		}
		return c.Addr, nil
	}
	return "", ErrNoHealthySlave
}
