package rdb

import (
	"bytes"
	"io"
	"os"
)

func bytesReader(b []byte) io.ReadSeeker { return bytes.NewReader(b) }

func osRead(path string) ([]byte, error) { return os.ReadFile(path) }

// lzfTestReader 构造 [clen][ulen][comp] 的 reader（小整数单字节长度编码）。
func lzfTestReader(comp []byte, clen, ulen int) *reader {
	buf := []byte{byte(clen), byte(ulen)}
	buf = append(buf, comp...)
	return newReader(bytes.NewReader(buf))
}

// minimalStreamRDB 构造仅含一条 stream 条目的合法 RDB（checksum 正确）。
func minimalStreamRDB() []byte {
	body := []byte{'R', 'E', 'D', 'I', 'S', '0', '0', '1', '1'}
	body = append(body, opSelectDB, 0x00) // SELECTDB 0
	body = append(body, vtStreamLP2)      // value type 19
	body = append(body, 0x01, 's')        // key "s"（解析至此即报 pending）
	body = append(body, opEOF)
	sum := crc64(0, body)
	for i := 0; i < 8; i++ {
		body = append(body, byte(sum>>(8*i)))
	}
	return body
}
