// Package protocol 实现 RESP2/RESP3 的解码与编码。公共 seam 只有
// Value、Decode、Value.Append、ParseHello。
package protocol

// Kind 标识 RESP 值的类型。
type Kind int

const (
	KindSimpleString Kind = iota + 1
	KindError
	KindInteger
	KindBulkString
	KindArray
	KindNull
	KindBoolean
	KindDouble
	KindBigNumber
	KindBulkError
	KindVerbatim
	KindMap
	KindSet
	KindAttribute
	KindPush
	KindHello
)

// Pair 是 Map/Attribute 的一个键值对（保序，不用 Go map）。
type Pair struct {
	K Value
	V Value
}

// Value 是一个 RESP 值。按 Kind 只读对应字段：
// SimpleString/Error/BigNumber 读 S；Integer 读 I；Double 读 F；
// Boolean 读 B；BulkString/BulkError/Verbatim 内容读 Bulk（nil 表 null）；
// Verbatim 格式读 S；Array/Set/Push 读 Elems（nil 表 null array）；
// Map/Attribute 读 Pairs。
type Value struct {
	Kind Kind
	S    string
	I    int64
	F    float64
	B    bool
	Bulk []byte
	// VerbatimFmt 仅 KindVerbatim 使用，如 "txt"。
	VerbatimFmt string
	Elems       []Value
	Pairs       []Pair
}

// BulkOf 构造 bulk string 值。
func BulkOf(s string) Value {
	return Value{Kind: KindBulkString, Bulk: []byte(s)}
}

// ArrayOf 构造 array 值。
func ArrayOf(elems ...Value) Value {
	return Value{Kind: KindArray, Elems: elems}
}
