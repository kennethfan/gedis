package commands

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

var dumpRestoreMeta = []acl.Meta{
	{Name: "DUMP", Category: "keyspace", ReadOnly: true, Keys: acl.KeySpec{First: 0, Last: 0}},
	{Name: "RESTORE", Category: "keyspace", Keys: acl.KeySpec{First: 0, Last: 0}},
}

func RegisterDumpRestore(r *network.Router, kv KV) {
	for _, m := range dumpRestoreMeta {
		acl.RegisterMeta(m)
	}
	h := &dumpRestoreHandler{kv: kv}
	r.Register("DUMP", h.dump)
	r.Register("RESTORE", h.restore)
}

type dumpRestoreHandler struct {
	kv KV
}

func (h *dumpRestoreHandler) dump(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'dump' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	_, e, err := lookupRaw(ctx, h.kv, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindBulkString, Bulk: datastruct.Encode(e.Type, e.Expiry, e.Payload)}
}

func (h *dumpRestoreHandler) restore(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 3 {
		return errValueStr("ERR wrong number of arguments for 'restore' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	ttlStr, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid expire time in 'restore' command")
	}
	ttl, err := strconv.ParseInt(ttlStr, 10, 64)
	if err != nil || ttl < 0 {
		return errValueStr("ERR invalid expire time in 'restore' command")
	}
	if args[2].Kind != protocol.KindBulkString || args[2].Bulk == nil {
		return errValueStr("ERR DUMP payload version or checksum are wrong")
	}
	replace := false
	absttl := false
	for _, a := range args[3:] {
		name, ok := argString(a)
		if !ok {
			return errValueStr("ERR syntax error")
		}
		switch strings.ToUpper(name) {
		case "REPLACE":
			replace = true
		case "ABSTTL":
			absttl = true
		default:
			return errValueStr("ERR syntax error")
		}
	}
	e, err := datastruct.Decode(args[2].Bulk)
	if err != nil {
		return errValueStr("ERR DUMP payload version or checksum are wrong")
	}
	prefix := restorePrefix(e.Type)
	if prefix == "" {
		return errValueStr("ERR DUMP payload version or checksum are wrong")
	}
	if _, _, err := lookupRaw(ctx, h.kv, key); err == nil {
		if !replace {
			return errValueStr("BUSYKEY Target key name already exists.")
		}
		if de, derr := lookupKey(ctx, h.kv, key); derr == nil {
			h.deleteByRaw(ctx, []byte(prefixForType(de.Type)+key), de, key)
		}
	} else if !isNotFound(err) {
		return errValue(err)
	}
	var expiry int64
	if absttl {
		if ttl != 0 {
			expiry = ttl * int64(time.Millisecond)
		}
	} else if ttl != 0 {
		expiry = time.Now().UnixNano() + ttl*int64(time.Millisecond)
	}
	if err := h.kv.Set(ctx, []byte(prefix+key), datastruct.Encode(e.Type, expiry, e.Payload)); err != nil {
		return errValue(err)
	}
	Notify("g", "restore", key)
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}

func (h *dumpRestoreHandler) deleteByRaw(ctx context.Context, raw []byte, e datastruct.Entry, userKey string) {
	_ = h.kv.Delete(ctx, raw)
	if e.Type == datastruct.TypeHash {
		_ = h.kv.Delete(ctx, datastruct.HashExpKey(userKey))
	}
}

func restorePrefix(t byte) string {
	if p := prefixForType(t); p != "" {
		return p
	}
	switch t {
	case datastruct.TypeHLL:
		return "hll:"
	case datastruct.TypeStream:
		return "x:"
	}
	return ""
}
