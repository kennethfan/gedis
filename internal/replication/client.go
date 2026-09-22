package replication

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kennethfan/gedis/internal/protocol"
)

// KV 是复本写路径的最小接口（raw KV 直写，不走命令层）。
type KV interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key, value []byte) error
	Delete(ctx context.Context, key []byte) error
}

// Client 是复本侧的同步客户端：拨号 → PING → PSYNC → 全量/部分应用 →
// 增量流。断线按 replid+offset 重连续传。Dial 可注入（单测用 pipe）。
type Client struct {
	kv   KV
	addr string
	Dial func() (net.Conn, error)

	// OnOffset 在 offset 推进时回调（REPLICAOF 接入后同步 Stats）。
	OnOffset func(int64)

	mu      sync.Mutex
	stop    chan struct{}
	running bool
	conn    net.Conn

	replID string
	offset int64
	keys   map[string]struct{}
}

// NewClient 返回指向 addr 的复本客户端，Start 之前不工作。
func NewClient(kv KV, addr string) *Client {
	return &Client{kv: kv, addr: addr, offset: -1, keys: make(map[string]struct{})}
}

// Start 启动后台同步；重复调用先停旧循环。
func (c *Client) Start() {
	c.Stop()
	c.mu.Lock()
	c.stop = make(chan struct{})
	c.running = true
	stop := c.stop
	c.mu.Unlock()
	go c.loop(stop)
}

// Stop 停止后台同步并打断阻塞读；重复调用安全。
func (c *Client) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return
	}
	c.running = false
	close(c.stop)
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
}

func (c *Client) loop(stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		if err := c.syncOnce(stop); err != nil {
			select {
			case <-stop:
				return
			case <-time.After(time.Second):
			}
		}
	}
}

func (c *Client) dial() (net.Conn, error) {
	if c.Dial != nil {
		return c.Dial()
	}
	return net.Dial("tcp", c.addr)
}

func (c *Client) state() (string, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.replID == "" {
		return "?", -1
	}
	return c.replID, c.offset
}

func (c *Client) setState(replID string, offset int64) {
	c.mu.Lock()
	c.replID = replID
	c.offset = offset
	cb := c.OnOffset
	c.mu.Unlock()
	if cb != nil {
		cb(offset)
	}
}

func (c *Client) trackOffset(off int64) {
	c.mu.Lock()
	if off > c.offset {
		c.offset = off
	}
	cb := c.OnOffset
	cur := c.offset
	c.mu.Unlock()
	if cb != nil {
		cb(cur)
	}
}

func writeValue(conn net.Conn, v protocol.Value) error {
	_, err := conn.Write(v.Append(nil))
	return err
}

// syncOnce 做一次同步会话：握手后按 FULLRESYNC/CONTINUE 应用，随后消费增量流。
// 任何错误都关闭连接返回，由 loop 重试。
func (c *Client) syncOnce(stop <-chan struct{}) error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	defer conn.Close()
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		c.conn = nil
		c.mu.Unlock()
	}()

	rd := bufio.NewReader(conn)
	if err := writeValue(conn, protocol.ArrayOf(protocol.BulkOf("PING"))); err != nil {
		return err
	}
	if _, err := protocol.Decode(rd); err != nil {
		return err
	}
	replID, offset := c.state()
	if err := writeValue(conn, protocol.ArrayOf(
		protocol.BulkOf("PSYNC"),
		protocol.BulkOf(replID),
		protocol.BulkOf(strconv.FormatInt(offset, 10)),
	)); err != nil {
		return err
	}
	reply, err := protocol.Decode(rd)
	if err != nil {
		return err
	}
	if reply.Kind != protocol.KindArray || len(reply.Elems) != 2 {
		return fmt.Errorf("replication: bad psync reply")
	}
	marker := string(reply.Elems[0].Bulk)
	fields := strings.Split(marker, " ")
	if len(fields) < 2 {
		return fmt.Errorf("replication: bad psync marker %q", marker)
	}
	switch fields[0] {
	case "FULLRESYNC":
		if len(fields) != 3 {
			return fmt.Errorf("replication: bad fullresync marker %q", marker)
		}
		off, err := strconv.ParseInt(fields[2], 10, 64)
		if err != nil {
			return fmt.Errorf("replication: bad fullresync offset %q", marker)
		}
		c.setState(fields[1], off)
		if reply.Elems[1].Kind != protocol.KindBulkString {
			return fmt.Errorf("replication: rdb is not bulk")
		}
		if err := c.applyRDB(reply.Elems[1].Bulk); err != nil {
			return err
		}
	case "CONTINUE":
		c.setState(fields[1], c.currentOffset())
		if reply.Elems[1].Kind != protocol.KindArray {
			return fmt.Errorf("replication: missed ops are not array")
		}
		for _, op := range reply.Elems[1].Elems {
			if err := c.applyOp(op); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("replication: unknown psync marker %q", marker)
	}

	for {
		select {
		case <-stop:
			return nil
		default:
		}
		v, err := protocol.Decode(rd)
		if err != nil {
			return err
		}
		if err := c.applyOp(v); err != nil {
			return err
		}
	}
}

func (c *Client) currentOffset() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.offset
}

// applyRDB 清掉上轮全量跟踪的 key 后写入 RDB（复本重定向新主库不留脏数据）。
func (c *Client) applyRDB(raw []byte) error {
	entries, err := UnmarshalRDB(raw)
	if err != nil {
		return err
	}
	ctx := context.Background()
	c.mu.Lock()
	old := c.keys
	c.keys = make(map[string]struct{}, len(entries))
	c.mu.Unlock()
	for k := range old {
		_ = c.kv.Delete(ctx, []byte(k))
	}
	for _, e := range entries {
		if err := c.kv.Set(ctx, e.Key, e.Value); err != nil {
			return err
		}
		c.mu.Lock()
		c.keys[string(e.Key)] = struct{}{}
		c.mu.Unlock()
	}
	return nil
}

// applyOp 应用一条 ["OP","set"|"del",key,value?,offset] 帧并推进 offset。
func (c *Client) applyOp(v protocol.Value) error {
	if v.Kind != protocol.KindArray || len(v.Elems) < 4 || string(v.Elems[0].Bulk) != "OP" {
		return fmt.Errorf("replication: bad op frame")
	}
	ctx := context.Background()
	switch string(v.Elems[1].Bulk) {
	case "set":
		if len(v.Elems) != 5 {
			return fmt.Errorf("replication: bad set op frame")
		}
		if err := c.kv.Set(ctx, v.Elems[2].Bulk, v.Elems[3].Bulk); err != nil {
			return err
		}
		off, err := strconv.ParseInt(string(v.Elems[4].Bulk), 10, 64)
		if err != nil {
			return fmt.Errorf("replication: bad op offset")
		}
		c.trackOffset(off)
		c.mu.Lock()
		c.keys[string(v.Elems[2].Bulk)] = struct{}{}
		c.mu.Unlock()
	case "del":
		if len(v.Elems) != 4 {
			return fmt.Errorf("replication: bad del op frame")
		}
		if err := c.kv.Delete(ctx, v.Elems[2].Bulk); err != nil {
			return err
		}
		off, err := strconv.ParseInt(string(v.Elems[3].Bulk), 10, 64)
		if err != nil {
			return fmt.Errorf("replication: bad op offset")
		}
		c.trackOffset(off)
	default:
		return fmt.Errorf("replication: unknown op %q", string(v.Elems[1].Bulk))
	}
	return nil
}
