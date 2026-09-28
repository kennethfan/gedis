package sentinel

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

// peerTimeout 是哨兵间点对点 RESP 请求的单次超时。
const peerTimeout = 2 * time.Second

// sendCmd 直连 addr 发一条命令并解码首个 RESP 回包。
func sendCmd(addr string, args ...string) (protocol.Value, error) {
	conn, err := net.DialTimeout("tcp", addr, peerTimeout)
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
	conn, err := net.DialTimeout("tcp", peerAddr, timeout)
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

func splitLines(s string) []string {
	var out []string
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

// FetchSlaveInfos 拉取 master 的 INFO replication 并为每个 slave
// 产出 SlaveInfo（无条目者保留 Priority:100 缺席默认）。
func FetchSlaveInfos(masterAddr string, slaves []string) ([]SlaveInfo, error) {
	v, err := sendCmd(masterAddr, "INFO", "replication")
	if err != nil {
		return nil, err
	}
	if v.Kind != protocol.KindBulkString {
		return nil, fmt.Errorf("sentinel: INFO replication: bad reply kind %d", v.Kind)
	}
	out := make([]SlaveInfo, 0, len(slaves))
	for _, addr := range slaves {
		out = append(out, ParseSlaveInfo(addr, string(v.Bulk)))
	}
	return out, nil
}
