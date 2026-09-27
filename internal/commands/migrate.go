package commands

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// migrateMeta 登记 MIGRATE：key 在命令名后的 args[2]（host port key db timeout…）。
var migrateMeta = []acl.Meta{
	{Name: "MIGRATE", Category: "keyspace", Keys: acl.KeySpec{First: 2, Last: 2}},
}

// RegisterMigrate 注册 MIGRATE 单 key 迁移命令（DUMP→TCP RESTORE→DEL）。
func RegisterMigrate(r *network.Router, kv KV) {
	for _, m := range migrateMeta {
		acl.RegisterMeta(m)
	}
	h := &migrateHandler{kv: kv}
	r.Register("MIGRATE", h.migrate)
}

type migrateHandler struct {
	kv KV
}

type migrateOptions struct {
	host    string
	port    string
	key     string
	timeout time.Duration
	copy    bool
	replace bool
	auth    []string // 空表无认证；[pass] 或 [user pass]
}

func parseMigrateArgs(args []protocol.Value) (migrateOptions, *protocol.Value) {
	var o migrateOptions
	if len(args) < 5 {
		return o, errPtr("ERR wrong number of arguments for 'migrate' command")
	}
	strs := make([]string, len(args))
	for i, a := range args {
		s, ok := argString(a)
		if !ok {
			return o, errPtr("ERR invalid argument")
		}
		strs[i] = s
	}
	o.host, o.port, o.key = strs[0], strs[1], strs[2]
	if _, err := strconv.Atoi(o.port); err != nil {
		return o, errPtr("ERR value is not an integer or out of range")
	}
	db, err := strconv.Atoi(strs[3])
	if err != nil {
		return o, errPtr("ERR value is not an integer or out of range")
	}
	if db != 0 {
		return o, errPtr("ERR SELECT is not allowed in cluster mode")
	}
	ms, err := strconv.Atoi(strs[4])
	if err != nil || ms <= 0 {
		return o, errPtr("ERR timeout is not an integer or out of range")
	}
	o.timeout = time.Duration(ms) * time.Millisecond
	for i := 5; i < len(strs); i++ {
		switch strings.ToUpper(strs[i]) {
		case "COPY":
			o.copy = true
		case "REPLACE":
			o.replace = true
		case "AUTH":
			if o.auth != nil || i+1 >= len(strs) {
				return o, errPtr("ERR syntax error")
			}
			o.auth = []string{strs[i+1]}
			i++
		case "AUTH2":
			if o.auth != nil || i+2 >= len(strs) {
				return o, errPtr("ERR syntax error")
			}
			o.auth = []string{strs[i+1], strs[i+2]}
			i += 2
		default:
			return o, errPtr("ERR syntax error")
		}
	}
	return o, nil
}

// migrate 实现 MIGRATE host port key db timeout [COPY] [REPLACE]
// [AUTH password | AUTH2 user pass]：读本地 Entry，经 TCP 发 RESTORE 到目标，
// 成功后非 COPY 删除源 key。目标 BUSYKEY / IO 超时均不删源 key。
func (h *migrateHandler) migrate(ctx context.Context, args []protocol.Value) protocol.Value {
	o, errReply := parseMigrateArgs(args)
	if errReply != nil {
		return *errReply
	}
	raw, e, err := lookupRaw(ctx, h.kv, o.key)
	if err != nil {
		if isNotFound(err) {
			return errValueStr("NOKEY No such key")
		}
		return errValue(err)
	}
	var ttl int64
	if e.Expiry != 0 {
		ttl = (e.Expiry - time.Now().UnixNano()) / int64(time.Millisecond)
		if ttl < 0 {
			ttl = 0
		}
	}
	if err := o.sendRestore(datastruct.Encode(e.Type, e.Expiry, e.Payload), ttl); err != nil {
		return errValue(err)
	}
	if !o.copy {
		if err := h.kv.Delete(ctx, raw); err != nil {
			return errValue(err)
		}
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

// sendRestore 建连目标并按序发 AUTH（若有）、ASKING 与 RESTORE key ttl
// payload [REPLACE]；读目标回复，非 +OK 即错（BUSYKEY 原样回透）。
// ASKING 必发：importing 态目标在同连接未见 ASKING 时拒收 RESTORE（回
// MOVED），与原生 MIGRATE 先 ASKING 后 RESTORE 的线序一致。
func (o migrateOptions) sendRestore(payload []byte, ttl int64) error {
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(o.host, o.port), o.timeout)
	if err != nil {
		return fmt.Errorf("IOERR error or timeout connecting to target instance")
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(o.timeout))
	rd := bufio.NewReader(conn)
	wr := bufio.NewWriter(conn)
	send := func(v protocol.Value) error {
		if _, err := wr.Write(v.Append(nil)); err != nil {
			return fmt.Errorf("IOERR error or timeout writing to target instance")
		}
		if err := wr.Flush(); err != nil {
			return fmt.Errorf("IOERR error or timeout writing to target instance")
		}
		return nil
	}
	if len(o.auth) > 0 {
		elems := []protocol.Value{protocol.BulkOf("AUTH")}
		for _, a := range o.auth {
			elems = append(elems, protocol.BulkOf(a))
		}
		if err := send(protocol.ArrayOf(elems...)); err != nil {
			return err
		}
		reply, err := protocol.Decode(rd)
		if err != nil {
			return fmt.Errorf("IOERR error or timeout reading from target instance")
		}
		if reply.Kind == protocol.KindError {
			return fmt.Errorf("%s", reply.S)
		}
	}
	if err := send(protocol.ArrayOf(protocol.BulkOf("ASKING"))); err != nil {
		return err
	}
	reply, err := protocol.Decode(rd)
	if err != nil {
		return fmt.Errorf("IOERR error or timeout reading from target instance")
	}
	if reply.Kind == protocol.KindError {
		return fmt.Errorf("%s", reply.S)
	}
	if reply.Kind != protocol.KindSimpleString || reply.S != "OK" {
		return fmt.Errorf("IOERR unexpected reply from target instance")
	}
	restore := []protocol.Value{
		protocol.BulkOf("RESTORE"),
		protocol.BulkOf(o.key),
		protocol.BulkOf(strconv.FormatInt(ttl, 10)),
		{Kind: protocol.KindBulkString, Bulk: payload},
	}
	if o.replace {
		restore = append(restore, protocol.BulkOf("REPLACE"))
	}
	if err := send(protocol.ArrayOf(restore...)); err != nil {
		return err
	}
	reply, err = protocol.Decode(rd)
	if err != nil {
		return fmt.Errorf("IOERR error or timeout reading from target instance")
	}
	if reply.Kind == protocol.KindError {
		return fmt.Errorf("%s", reply.S)
	}
	if reply.Kind != protocol.KindSimpleString || reply.S != "OK" {
		return fmt.Errorf("IOERR unexpected reply from target instance")
	}
	return nil
}
