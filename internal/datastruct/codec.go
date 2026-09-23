package datastruct

import (
	"encoding/binary"
	"fmt"
)

const (
	TypeString byte = 's'
	TypeHash   byte = 'h'
	TypeList   byte = 'l'
	TypeSet    byte = 't'
	TypeZSet   byte = 'z'
	TypeHLL    byte = 'H'
)

const headerLen = 9

// Entry 是解码后的存储单元：类型 + 过期 unixnano（0 表无过期）+ 负载。
type Entry struct {
	Type    byte
	Expiry  int64
	Payload []byte
}

// Encode 组装 [type][8字节大端expiry][payload]。
func Encode(typ byte, expiry int64, payload []byte) []byte {
	out := make([]byte, headerLen+len(payload))
	out[0] = typ
	binary.BigEndian.PutUint64(out[1:9], uint64(expiry))
	copy(out[9:], payload)
	return out
}

// EncodeString 是 Encode 的 string 特化。
func EncodeString(payload []byte, expiry int64) []byte {
	return Encode(TypeString, expiry, payload)
}

// Decode 解析存储单元，短于 9 字节报错；类型标签原样返回由调用方校验。
func Decode(raw []byte) (Entry, error) {
	if len(raw) < headerLen {
		return Entry{}, fmt.Errorf("datastruct: truncated entry %d bytes", len(raw))
	}
	return Entry{
		Type:    raw[0],
		Expiry:  int64(binary.BigEndian.Uint64(raw[1:9])),
		Payload: raw[9:],
	}, nil
}

// StringKey 给用户 key 加 s: 前缀。
func StringKey(key string) []byte {
	return append([]byte("s:"), key...)
}
