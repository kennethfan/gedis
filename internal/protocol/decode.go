package protocol

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// Decode 从 r 解码一个 RESP 值。无类型前缀的行按 inline 命令处理，
// 切分为 bulk string 数组。调用方负责循环调用以处理 pipeline。
func Decode(r *bufio.Reader) (Value, error) {
	b, err := r.ReadByte()
	if err != nil {
		return Value{}, fmt.Errorf("protocol: read type: %w", err)
	}
	if err := r.UnreadByte(); err != nil {
		return Value{}, fmt.Errorf("protocol: unread type: %w", err)
	}
	if !isTypeByte(b) {
		return decodeInline(r)
	}
	return decodeTyped(r, b)
}

func isTypeByte(b byte) bool {
	switch b {
	case '+', '-', ':', '$', '*', '_', '#', ',', '(', '!', '=', '%', '~', '|', '>':
		return true
	}
	return false
}

// decodeInline 处理 "CMD arg arg\r\n" 形式，切分为 bulk 数组。
func decodeInline(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	fields := strings.Fields(line)
	elems := make([]Value, 0, len(fields))
	for _, f := range fields {
		elems = append(elems, BulkOf(f))
	}
	return Value{Kind: KindArray, Elems: elems}, nil
}

func decodeTyped(r *bufio.Reader, typ byte) (Value, error) {
	if _, err := r.ReadByte(); err != nil {
		return Value{}, fmt.Errorf("protocol: read type: %w", err)
	}
	switch typ {
	case '+':
		s, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindSimpleString, S: s}, nil
	case '-':
		s, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindError, S: s}, nil
	case ':':
		n, err := readInt(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindInteger, I: n}, nil
	case '$':
		return decodeBulk(r)
	case '*':
		return decodeArray(r)
	case '_':
		if err := expectCRLF(r); err != nil {
			return Value{}, err
		}
		return Value{Kind: KindNull}, nil
	case '#':
		b, err := r.ReadByte()
		if err != nil {
			return Value{}, fmt.Errorf("protocol: read boolean: %w", err)
		}
		if err := expectCRLF(r); err != nil {
			return Value{}, err
		}
		switch b {
		case 't':
			return Value{Kind: KindBoolean, B: true}, nil
		case 'f':
			return Value{Kind: KindBoolean, B: false}, nil
		default:
			return Value{}, fmt.Errorf("protocol: bad boolean %q", b)
		}
	case ',':
		s, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return Value{}, fmt.Errorf("protocol: bad double %q: %w", s, err)
		}
		return Value{Kind: KindDouble, F: f}, nil
	case '(':
		s, err := readLine(r)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBigNumber, S: s}, nil
	case '!':
		return decodeSized(r, KindBulkError)
	case '=':
		return decodeVerbatim(r)
	case '%':
		return decodeMap(r)
	case '~':
		v, err := decodeN(r, "set")
		if err != nil {
			return Value{}, err
		}
		v.Kind = KindSet
		return v, nil
	case '|':
		return decodeMapAs(r, KindAttribute)
	case '>':
		v, err := decodeN(r, "push")
		if err != nil {
			return Value{}, err
		}
		v.Kind = KindPush
		return v, nil
	default:
		return Value{}, fmt.Errorf("protocol: unsupported type %q", typ)
	}
}

func decodeBulk(r *bufio.Reader) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return Value{Kind: KindBulkString}, nil
	}
	if n < -1 {
		return Value{}, fmt.Errorf("protocol: invalid bulk length %d", n)
	}
	buf := make([]byte, n+2)
	if _, err := readFull(r, buf); err != nil {
		return Value{}, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return Value{}, fmt.Errorf("protocol: bulk missing CRLF")
	}
	return Value{Kind: KindBulkString, Bulk: buf[:n]}, nil
}

func decodeArray(r *bufio.Reader) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n == -1 {
		return Value{Kind: KindArray}, nil
	}
	if n < -1 {
		return Value{}, fmt.Errorf("protocol: invalid array length %d", n)
	}
	elems := make([]Value, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		elems = append(elems, v)
	}
	return Value{Kind: KindArray, Elems: elems}, nil
}

func decodeSized(r *bufio.Reader, kind Kind) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{}, fmt.Errorf("protocol: invalid sized length %d", n)
	}
	buf := make([]byte, n+2)
	if _, err := readFull(r, buf); err != nil {
		return Value{}, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return Value{}, fmt.Errorf("protocol: sized missing CRLF")
	}
	return Value{Kind: kind, Bulk: buf[:n]}, nil
}

// decodeVerbatim 解码 "=15\r\ntxt:Some string\r\n"，格式与内容分离。
func decodeVerbatim(r *bufio.Reader) (Value, error) {
	v, err := decodeSized(r, KindVerbatim)
	if err != nil {
		return Value{}, err
	}
	parts := strings.SplitN(string(v.Bulk), ":", 2)
	if len(parts) != 2 {
		return Value{}, fmt.Errorf("protocol: verbatim missing format prefix")
	}
	v.VerbatimFmt = parts[0]
	v.Bulk = []byte(parts[1])
	return v, nil
}

func decodeN(r *bufio.Reader, what string) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{}, fmt.Errorf("protocol: invalid %s length %d", what, n)
	}
	elems := make([]Value, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		elems = append(elems, v)
	}
	return Value{Kind: KindArray, Elems: elems}, nil
}

func decodeMap(r *bufio.Reader) (Value, error) {
	return decodeMapAs(r, KindMap)
}

func decodeMapAs(r *bufio.Reader, kind Kind) (Value, error) {
	n, err := readInt(r)
	if err != nil {
		return Value{}, err
	}
	if n < 0 {
		return Value{}, fmt.Errorf("protocol: invalid map length %d", n)
	}
	pairs := make([]Pair, 0, n)
	for i := int64(0); i < n; i++ {
		k, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		v, err := Decode(r)
		if err != nil {
			return Value{}, err
		}
		pairs = append(pairs, Pair{K: k, V: v})
	}
	return Value{Kind: kind, Pairs: pairs}, nil
}

func expectCRLF(r *bufio.Reader) error {
	b1, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("protocol: read CRLF: %w", err)
	}
	b2, err := r.ReadByte()
	if err != nil {
		return fmt.Errorf("protocol: read CRLF: %w", err)
	}
	if b1 != '\r' || b2 != '\n' {
		return fmt.Errorf("protocol: expected CRLF")
	}
	return nil
}
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("protocol: read line: %w", err)
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("protocol: line missing CRLF")
	}
	return line[:len(line)-2], nil
}

// readInt 读一个十进制整数行。
func readInt(r *bufio.Reader) (int64, error) {
	s, err := readLine(r)
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("protocol: bad integer %q: %w", s, err)
	}
	return n, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, fmt.Errorf("protocol: read payload: %w", err)
		}
	}
	return total, nil
}
