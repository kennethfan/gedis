package datastruct

import (
	"encoding/binary"
	"fmt"
)

const (
	ListEncodingZiplist   byte = 0
	ListEncodingQuicklist byte = 1
)

const (
	listMaxZiplistEntries = 128
	listMaxZiplistValue   = 64
)

// ListKey 给 key 加 l: 前缀。
func ListKey(key string) []byte { return []byte("l:" + key) }

// EncodeList 把有序元素编码为 [encoding][uvarint count][元素长度前缀序列]。
// 自适应规则照抄 hash：>128 个元素或任一元素 >64B 用 quicklist。
func EncodeList(elems []string) []byte {
	enc := ListEncodingZiplist
	if len(elems) > listMaxZiplistEntries {
		enc = ListEncodingQuicklist
	} else {
		for _, e := range elems {
			if len(e) > listMaxZiplistValue {
				enc = ListEncodingQuicklist
				break
			}
		}
	}
	size := 1 + binary.MaxVarintLen64
	for _, e := range elems {
		size += binary.MaxVarintLen64 + len(e)
	}
	out := make([]byte, 0, size)
	out = append(out, enc)
	out = binary.AppendUvarint(out, uint64(len(elems)))
	for _, e := range elems {
		out = binary.AppendUvarint(out, uint64(len(e)))
		out = append(out, e...)
	}
	return out
}

// DecodeList 解析 EncodeList 的输出，按头到尾顺序返回元素。
func DecodeList(raw []byte) ([]string, error) {
	if len(raw) < 1 {
		return nil, fmt.Errorf("datastruct: truncated list payload")
	}
	rest := raw[1:]
	n, adv := binary.Uvarint(rest)
	if adv <= 0 {
		return nil, fmt.Errorf("datastruct: bad list count")
	}
	rest = rest[adv:]
	elems := make([]string, 0, n)
	for i := uint64(0); i < n; i++ {
		e, rest2, ok := readListStr(rest)
		if !ok {
			return nil, fmt.Errorf("datastruct: truncated list element %d", i)
		}
		elems = append(elems, e)
		rest = rest2
	}
	return elems, nil
}

func readListStr(b []byte) (string, []byte, bool) {
	n, adv := binary.Uvarint(b)
	if adv <= 0 {
		return "", nil, false
	}
	b = b[adv:]
	if uint64(len(b)) < n {
		return "", nil, false
	}
	return string(b[:n]), b[n:], true
}

// ListPushHead 在表头插入一个元素，返回重新编码的 payload。
func ListPushHead(raw []byte, elem string) []byte {
	elems, err := DecodeList(raw)
	if err != nil {
		elems = nil
	}
	return EncodeList(append([]string{elem}, elems...))
}

// ListPushTail 在表尾追加一个元素，返回重新编码的 payload。
func ListPushTail(raw []byte, elem string) []byte {
	elems, err := DecodeList(raw)
	if err != nil {
		elems = nil
	}
	return EncodeList(append(elems, elem))
}
