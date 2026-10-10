package sentinel

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/tlsdial"
)

// peerTimeout 是哨兵间点对点 RESP 请求的单次超时。
const peerTimeout = 2 * time.Second

// sendCmd 直连 addr 发一条命令并解码首个 RESP 回包。
func sendCmd(addr string, args ...string) (protocol.Value, error) {
	conn, err := tlsdial.DialTimeout("tcp", addr, peerTimeout)
	if err != nil {
		return protocol.Value{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(peerTimeout))
	elems := make([]protocol.Value, 0, len(args))
	for _, a := range args {
		elems = append(elems, protocol.BulkOf(a))
	}
	if _, err := conn.Write(protocol.ArrayOf(elems...).Append(nil)); err != nil {
		return protocol.Value{}, err
	}
	return protocol.Decode(bufio.NewReader(conn))
}

// QueryPeer 问询对端哨兵：master 是否 SDOWN + 本 epoch 投给了谁。
// 返回（对端视角 down 与否，leader runid（无为 "*"），leader epoch）。
func QueryPeer(peerAddr, masterHost string, masterPort int, epoch uint64, runID, master string) (bool, string, uint64, error) {
	v, err := sendCmd(peerAddr, "SENTINEL", "IS-MASTER-DOWN-BY-ADDR",
		masterHost, strconv.Itoa(masterPort), strconv.FormatUint(epoch, 10), runID)
	if err != nil {
		return false, "", 0, err
	}
	if v.Kind == protocol.KindError {
		return false, "", 0, fmt.Errorf("sentinel: peer %s master %s: %s", peerAddr, master, v.S)
	}
	if v.Kind != protocol.KindArray || len(v.Elems) != 3 {
		return false, "", 0, fmt.Errorf("sentinel: peer %s: bad is-master-down reply", peerAddr)
	}
	le, err := strconv.ParseUint(string(v.Elems[2].Bulk), 10, 64)
	if err != nil {
		return false, "", 0, err
	}
	return string(v.Elems[0].Bulk) == "1", string(v.Elems[1].Bulk), le, nil
}

// FetchRole 拉取节点的 INFO replication 并解析 role 字段。
func FetchRole(addr string) (string, error) {
	v, err := sendCmd(addr, "INFO", "replication")
	if err != nil {
		return "", err
	}
	if v.Kind != protocol.KindBulkString {
		return "", fmt.Errorf("sentinel: INFO replication: bad reply kind %d", v.Kind)
	}
	for _, line := range splitLines(string(v.Bulk)) {
		if role, ok := cutPrefix(line, "role:"); ok {
			return role, nil
		}
	}
	return "", fmt.Errorf("sentinel: INFO replication: no role")
}

// FetchHello 向对端订阅 __sentinel__:hello，取首条本频道消息解析。
// timeout 内无消息返回超时错。
func FetchHello(peerAddr string, timeout time.Duration) (Hello, error) {
	conn, err := tlsdial.DialTimeout("tcp", peerAddr, timeout)
	if err != nil {
		return Hello{}, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if _, err := conn.Write(protocol.ArrayOf(
		protocol.BulkOf("SUBSCRIBE"), protocol.BulkOf("__sentinel__:hello")).Append(nil)); err != nil {
		return Hello{}, err
	}
	rd := bufio.NewReader(conn)
	for i := 0; i < 16; i++ {
		v, err := protocol.Decode(rd)
		if err != nil {
			return Hello{}, err
		}
		if v.Kind != protocol.KindArray || len(v.Elems) != 3 {
			continue
		}
		if string(v.Elems[0].Bulk) != "message" || string(v.Elems[1].Bulk) != "__sentinel__:hello" {
			continue
		}
		return ParseHello(string(v.Elems[2].Bulk))
	}
	return Hello{}, fmt.Errorf("sentinel: no hello from %s", peerAddr)
}

// parseSlaveSideInfo 解析从节点自身 INFO replication：role 非 slave
// 即非候选（多半是已被提升的新主，返回 false 交上层决策）；offset 取
// slave_repl_offset；gedis INFO 无 slave_priority 字段，Priority 恒 100；
// 可达即记 State online（gedis 的 master_link_status 硬编码 up，不可信）。
func parseSlaveSideInfo(addr, infoOut string) (SlaveInfo, bool) {
	si := SlaveInfo{Addr: addr, Priority: 100}
	fields := map[string]string{}
	for _, line := range splitLines(infoOut) {
		if k, v, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	if fields["role"] != "slave" {
		return si, false
	}
	if off, err := strconv.ParseInt(fields["slave_repl_offset"], 10, 64); err == nil {
		si.Offset = off
	}
	si.State = "online"
	return si, true
}

func splitLines(s string) []string {	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, trimCR(s[start:i]))
			start = i + 1
		}
	}
	out = append(out, trimCR(s[start:]))
	return out
}

func trimCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}

func cutPrefix(s, pre string) (string, bool) {
	if len(s) < len(pre) || s[:len(pre)] != pre {
		return "", false
	}
	return s[len(pre):], true
}

// FetchSlaveInfos 汇总每个 slave 的 SlaveInfo：先尽力拉 master 的 INFO
// replication（slaveN 行匹配，自动转移时主已死通常拉不到），再逐个直连
// 活着的 slave 取其自身 INFO（slave_repl_offset 为准），合并后按 Slaves
// 顺序返回；均拉不到者保留 Priority:100 缺席默认。master 与各从
// 任一不可达都不再整体报错（调用方照常用 SelectSlave 决策）。
func FetchSlaveInfos(masterAddr string, slaves []string) ([]SlaveInfo, error) {
	byAddr := make(map[string]SlaveInfo, len(slaves))
	if v, err := sendCmd(masterAddr, "INFO", "replication"); err == nil && v.Kind == protocol.KindBulkString {
		for _, addr := range slaves {
			byAddr[addr] = ParseSlaveInfo(addr, string(v.Bulk))
		}
	}
	out := make([]SlaveInfo, 0, len(slaves))
	for _, addr := range slaves {
		if v, err := sendCmd(addr, "INFO", "replication"); err == nil && v.Kind == protocol.KindBulkString {
			if si, ok := parseSlaveSideInfo(addr, string(v.Bulk)); ok {
				if base, has := byAddr[addr]; has {
					if si.RunID == "" {
						si.RunID = base.RunID
					}
				}
				byAddr[addr] = si
			}
		}
		if si, ok := byAddr[addr]; ok {
			out = append(out, si)
		} else {
			out = append(out, SlaveInfo{Addr: addr, Priority: 100})
		}
	}
	return out, nil
}
