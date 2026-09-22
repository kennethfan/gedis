package network

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"
)

// SlowEntry 是一条慢查询记录：自增 ID、unix 秒时间戳、耗时微秒、
// 小写命令名与参数。
type SlowEntry struct {
	ID             int64
	Timestamp      int64
	DurationMicros int64
	Command        string
	Args           []string
}

// Stats 是服务统计与慢日志的并发安全容器。Router 与 Server 共享一例，
// commands 层的 INFO/SLOWLOG 只读它。nil *Stats 永远安全（方法判空）。
type Stats struct {
	mu                sync.Mutex
	startTime         time.Time
	connsReceived     atomic.Int64
	commandsProcessed atomic.Int64
	connectedClients  atomic.Int64
	blockedClients    atomic.Int64
	expiredKeys       atomic.Int64

	SlowThresholdMicros int64
	slowMaxLen          int
	slow                []SlowEntry
	slowNextID          int64

	Role        string
	ReplID      string
	MasterHost  string
	MasterPort  int
	slaveOffset atomic.Int64
}

// NewStats 返回计数从零、慢日志阈值 10ms、上限 128 的 Stats，
// 附带随机生成的 40 位 master_replid（#13 接管前有效）。
func NewStats() *Stats {
	return &Stats{
		startTime:           time.Now(),
		SlowThresholdMicros: 10000,
		slowMaxLen:          128,
		Role:                "master",
		ReplID:              randomReplID(),
	}
}

func randomReplID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000000000000000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// Snapshot 拷贝当前计数与运行秒数。
func (s *Stats) Snapshot() StatsView {
	if s == nil {
		return StatsView{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return StatsView{
		UptimeSeconds:     int64(time.Since(s.startTime) / time.Second),
		ConnsReceived:     s.connsReceived.Load(),
		CommandsProcessed: s.commandsProcessed.Load(),
		ConnectedClients:  s.connectedClients.Load(),
		BlockedClients:    s.blockedClients.Load(),
		ExpiredKeys:       s.expiredKeys.Load(),
		Role:              s.Role,
		ReplID:            s.ReplID,
		MasterHost:        s.MasterHost,
		MasterPort:        s.MasterPort,
		SlaveOffset:       s.slaveOffset.Load(),
	}
}

// StatsView 是 Stats 的一次只读拷贝。
type StatsView struct {
	UptimeSeconds     int64
	ConnsReceived     int64
	CommandsProcessed int64
	ConnectedClients  int64
	BlockedClients    int64
	ExpiredKeys       int64
	Role              string
	ReplID            string
	MasterHost        string
	MasterPort        int
	SlaveOffset       int64
}

// SetRole 原子切换主从角色（REPLICAOF 用）。
func (s *Stats) SetRole(role string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Role = role
}

// SetMaster 记录主库地址并切为 slave；ClearMaster 切回 master。
func (s *Stats) SetMaster(host string, port int) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MasterHost = host
	s.MasterPort = port
	s.Role = "slave"
}

func (s *Stats) ClearMaster() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.MasterHost = ""
	s.MasterPort = 0
	s.Role = "master"
}

// SetSlaveOffset 记录复本已应用的最大 offset（INFO slave_repl_offset 用）。
func (s *Stats) SetSlaveOffset(off int64) {
	if s == nil {
		return
	}
	s.slaveOffset.Store(off)
}

func (s *Stats) incConn() {
	if s == nil {
		return
	}
	s.connsReceived.Add(1)
	s.connectedClients.Add(1)
}

func (s *Stats) decConn() {
	if s == nil {
		return
	}
	s.connectedClients.Add(-1)
}

// IncExpired 记录一次过期删除（被动或主动）；nil 安全。
func (s *Stats) IncExpired() {
	if s == nil {
		return
	}
	s.expiredKeys.Add(1)
}

// IncBlocked 在进入阻塞等待时计数，DecBlocked 在退出时调用；nil 安全。
func (s *Stats) IncBlocked() {
	if s == nil {
		return
	}
	s.blockedClients.Add(1)
}

func (s *Stats) DecBlocked() {
	if s == nil {
		return
	}
	s.blockedClients.Add(-1)
}

func (s *Stats) incCommands() {
	if s == nil {
		return
	}
	s.commandsProcessed.Add(1)
}

// AddSlow 记录一条慢查询，超出上限丢弃最老记录。
func (s *Stats) AddSlow(cmd string, args []string, durationMicros int64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slowNextID++
	s.slow = append(s.slow, SlowEntry{
		ID:             s.slowNextID,
		Timestamp:      time.Now().Unix(),
		DurationMicros: durationMicros,
		Command:        cmd,
		Args:           args,
	})
	if len(s.slow) > s.slowMaxLen {
		s.slow = append([]SlowEntry(nil), s.slow[len(s.slow)-s.slowMaxLen:]...)
	}
}

// SlowEntries 返回最多 n 条最新记录（时间正序）。
func (s *Stats) SlowEntries(n int) []SlowEntry {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > len(s.slow) {
		n = len(s.slow)
	}
	out := make([]SlowEntry, n)
	copy(out, s.slow[len(s.slow)-n:])
	return out
}

// SlowLen 返回慢日志当前长度。
func (s *Stats) SlowLen() int {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.slow)
}

// SlowReset 清空慢日志。
func (s *Stats) SlowReset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slow = nil
}
