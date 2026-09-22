package commands

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"

	"github.com/kennethfan/gedis/internal/datastruct"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
)

// RegisterSet 注册基础 set 命令；集合运算与 SSCAN 由 slice3 的 registerOps 追加。
func RegisterSet(r *network.Router, kv KV) {
	h := &setHandler{kv: kv}
	r.Register("SADD", h.sadd)
	r.Register("SREM", h.srem)
	r.Register("SMEMBERS", h.smembers)
	r.Register("SISMEMBER", h.sismember)
	r.Register("SMISMEMBER", h.smismember)
	r.Register("SCARD", h.scard)
	r.Register("SPOP", h.spop)
	r.Register("SRANDMEMBER", h.srandmember)
	r.Register("SMOVE", h.smove)
	h.registerOps(r)
}

type setHandler struct {
	kv KV
}

// readSet 读 set key：不存在返回 ErrNotFound；已过期删除后返回
// ErrNotFound；类型非 set 返回 wrongType 错误。
func (h *setHandler) readSet(ctx context.Context, key string) (map[string]struct{}, int64, error) {
	e, err := lookupKey(ctx, h.kv, key)
	if err != nil {
		return nil, 0, err
	}
	if e.Type != datastruct.TypeSet {
		return nil, 0, fmt.Errorf("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	set, _, err := datastruct.DecodeSet(e.Payload)
	if err != nil {
		return nil, 0, err
	}
	return set, e.Expiry, nil
}

func (h *setHandler) writeSet(ctx context.Context, key string, set map[string]struct{}, expiry int64) error {
	members := make([]string, 0, len(set))
	for m := range set {
		members = append(members, m)
	}
	return h.kv.Set(ctx, datastruct.SetKey(key), datastruct.Encode(datastruct.TypeSet, expiry, datastruct.EncodeSet(members)))
}

func setKey(args []protocol.Value) (string, bool) {
	if len(args) < 1 {
		return "", false
	}
	return argString(args[0])
}

func sortedMembers(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

func (h *setHandler) sadd(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'sadd' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	set, expiry, err := h.readSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		set = make(map[string]struct{})
	}
	var added int64
	for _, a := range args[1:] {
		m, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid member")
		}
		if _, exists := set[m]; !exists {
			set[m] = struct{}{}
			added++
		}
	}
	if err := h.writeSet(ctx, key, set, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: added}
}

func (h *setHandler) srem(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'srem' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	set, expiry, err := h.readSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	var deleted int64
	for _, a := range args[1:] {
		m, ok := argString(a)
		if !ok {
			continue
		}
		if _, exists := set[m]; exists {
			delete(set, m)
			deleted++
		}
	}
	if len(set) == 0 {
		if derr := h.kv.Delete(ctx, datastruct.SetKey(key)); derr != nil {
			return errValue(derr)
		}
	} else if err := h.writeSet(ctx, key, set, expiry); err != nil {
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: deleted}
}

func (h *setHandler) smembers(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'smembers' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	set, _, err := h.readSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return toBulkArray(nil)
		}
		return errValue(err)
	}
	return toBulkArray(sortedMembers(set))
}

func (h *setHandler) sismember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 2 {
		return errValueStr("ERR wrong number of arguments for 'sismember' command")
	}
	key, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid key")
	}
	member, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid member")
	}
	set, _, err := h.readSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	if _, exists := set[member]; exists {
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 0}
}

func (h *setHandler) smismember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 2 {
		return errValueStr("ERR wrong number of arguments for 'smismember' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	set, _, err := h.readSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		set = make(map[string]struct{})
	}
	out := make([]protocol.Value, 0, len(args)-1)
	for _, a := range args[1:] {
		m, ok := argString(a)
		if !ok {
			return errValueStr("ERR invalid member")
		}
		if _, exists := set[m]; exists {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 1})
		} else {
			out = append(out, protocol.Value{Kind: protocol.KindInteger, I: 0})
		}
	}
	return protocol.Value{Kind: protocol.KindArray, Elems: out}
}

func (h *setHandler) scard(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 1 {
		return errValueStr("ERR wrong number of arguments for 'scard' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	set, _, err := h.readSet(ctx, key)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: int64(len(set))}
}

// popCount 解析可选的 count 参数；allowNegative 为 true 时允许负数（SRANDMEMBER 语义）。
func popCount(args []protocol.Value, allowNegative bool) (int64, bool, *protocol.Value) {
	if len(args) == 1 {
		return 1, false, nil
	}
	c, ok := argString(args[1])
	if !ok {
		return 0, false, errPtr("ERR value is not an integer or out of range")
	}
	n, err := strconv.ParseInt(c, 10, 64)
	if err != nil || (!allowNegative && n < 0) {
		return 0, false, errPtr("ERR value is not an integer or out of range")
	}
	return n, true, nil
}

func (h *setHandler) spop(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return errValueStr("ERR wrong number of arguments for 'spop' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	count, withCount, errReply := popCount(args, false)
	if errReply != nil {
		return *errReply
	}
	set, expiry, err := h.readSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		if withCount {
			return toBulkArray(nil)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	members := sortedMembers(set)
	if count > int64(len(members)) {
		count = int64(len(members))
	}
	perm := rand.Perm(len(members))
	popped := make([]string, 0, count)
	for i := int64(0); i < count; i++ {
		m := members[perm[i]]
		popped = append(popped, m)
		delete(set, m)
	}
	if len(set) == 0 {
		if derr := h.kv.Delete(ctx, datastruct.SetKey(key)); derr != nil {
			return errValue(derr)
		}
	} else if werr := h.writeSet(ctx, key, set, expiry); werr != nil {
		return errValue(werr)
	}
	if !withCount {
		if len(popped) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(popped[0])
	}
	return toBulkArray(popped)
}

func (h *setHandler) srandmember(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) < 1 || len(args) > 2 {
		return errValueStr("ERR wrong number of arguments for 'srandmember' command")
	}
	key, ok := setKey(args)
	if !ok {
		return errValueStr("ERR invalid key")
	}
	count, withCount, errReply := popCount(args, true)
	if errReply != nil {
		return *errReply
	}
	set, _, err := h.readSet(ctx, key)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		if withCount {
			return toBulkArray(nil)
		}
		return protocol.Value{Kind: protocol.KindBulkString}
	}
	members := sortedMembers(set)
	if !withCount {
		if len(members) == 0 {
			return protocol.Value{Kind: protocol.KindBulkString}
		}
		return protocol.BulkOf(members[rand.Intn(len(members))])
	}
	if count == 0 {
		return toBulkArray(nil)
	}
	out := make([]string, 0)
	if count > 0 {
		if count > int64(len(members)) {
			count = int64(len(members))
		}
		perm := rand.Perm(len(members))
		for i := int64(0); i < count; i++ {
			out = append(out, members[perm[i]])
		}
	} else {
		for i := int64(0); i < -count; i++ {
			out = append(out, members[rand.Intn(len(members))])
		}
	}
	return toBulkArray(out)
}

func (h *setHandler) smove(ctx context.Context, args []protocol.Value) protocol.Value {
	if len(args) != 3 {
		return errValueStr("ERR wrong number of arguments for 'smove' command")
	}
	src, ok := argString(args[0])
	if !ok {
		return errValueStr("ERR invalid source")
	}
	dst, ok := argString(args[1])
	if !ok {
		return errValueStr("ERR invalid destination")
	}
	member, ok := argString(args[2])
	if !ok {
		return errValueStr("ERR invalid member")
	}
	srcSet, srcExpiry, err := h.readSet(ctx, src)
	if err != nil {
		if isNotFound(err) {
			return protocol.Value{Kind: protocol.KindInteger, I: 0}
		}
		return errValue(err)
	}
	if _, exists := srcSet[member]; !exists {
		return protocol.Value{Kind: protocol.KindInteger, I: 0}
	}
	if src == dst {
		return protocol.Value{Kind: protocol.KindInteger, I: 1}
	}
	dstSet, dstExpiry, err := h.readSet(ctx, dst)
	if err != nil {
		if !isNotFound(err) {
			return errValue(err)
		}
		dstSet = make(map[string]struct{})
	}
	delete(srcSet, member)
	dstSet[member] = struct{}{}
	if len(srcSet) == 0 {
		if derr := h.kv.Delete(ctx, datastruct.SetKey(src)); derr != nil {
			return errValue(derr)
		}
	} else if werr := h.writeSet(ctx, src, srcSet, srcExpiry); werr != nil {
		return errValue(werr)
	}
	if werr := h.writeSet(ctx, dst, dstSet, dstExpiry); werr != nil {
		return errValue(werr)
	}
	return protocol.Value{Kind: protocol.KindInteger, I: 1}
}
