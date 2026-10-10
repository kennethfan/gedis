package commands

import (
	"fmt"
	"sync"

	"github.com/kennethfan/gedis/internal/protocol"
)

// 位定义（与 Global Constraints §5 字母表一致）
const (
	notifyK = uint32(1 << iota) // K
	notifyE                     // E
	notifyG                     // g generic
	notifyStr                   // $ string
	notifyL                     // l list
	notifyS                     // s set
	notifyH                     // h hash
	notifyZ                     // z zset
	notifyT                     // t stream
	notifyX                     // x expired
	notifyEv                    // e evicted
)

var notifyMu sync.RWMutex
var notifyMask uint32
var notifyRaw string
var notifyPub func(string, protocol.Value) int64

func classBit(class string) uint32 {
	switch class {
	case "g":
		return notifyG
	case "$":
		return notifyStr
	case "l":
		return notifyL
	case "s":
		return notifyS
	case "h":
		return notifyH
	case "z":
		return notifyZ
	case "t":
		return notifyT
	case "x":
		return notifyX
	case "e":
		return notifyEv
	}
	return 0
}

// SetNotifyString 解析 notify-keyspace-events 并原子生效；报错时旧值不变。
// 接受字母：KEg$lshztxe 及别名 A(=g$lshztxe)、a(=lshz)；d/m/n/o/c 接受但不产生事件。
func SetNotifyString(s string) error {
	var mask uint32
	for _, r := range s {
		switch r {
		case 'K':
			mask |= notifyK
		case 'E':
			mask |= notifyE
		case 'g':
			mask |= notifyG
		case '$':
			mask |= notifyStr
		case 'l':
			mask |= notifyL
		case 's':
			mask |= notifyS
		case 'h':
			mask |= notifyH
		case 'z':
			mask |= notifyZ
		case 't':
			mask |= notifyT
		case 'x':
			mask |= notifyX
		case 'e':
			mask |= notifyEv
		case 'A':
			mask |= notifyG | notifyStr | notifyL | notifyS | notifyH | notifyZ | notifyT | notifyX | notifyEv
		case 'a':
			mask |= notifyL | notifyS | notifyH | notifyZ | notifyT
		case 'd', 'm', 'n', 'o', 'c': // 接受但本批不产生对应事件
		default:
			return fmt.Errorf("ERR invalid notify-keyspace-events parameter")
		}
	}
	notifyMu.Lock()
	notifyMask, notifyRaw = mask, s
	notifyMu.Unlock()
	return nil
}

func NotifyString() string {
	notifyMu.RLock()
	defer notifyMu.RUnlock()
	return notifyRaw
}

func SetNotifyPublisher(pub func(string, protocol.Value) int64) {
	notifyMu.Lock()
	notifyPub = pub
	notifyMu.Unlock()
}

// Notify 发布一条键空间/键事件通知（db 固定 0）。
func Notify(class, event, key string) {
	notifyMu.RLock()
	mask, pub := notifyMask, notifyPub
	notifyMu.RUnlock()
	if pub == nil || mask == 0 {
		return
	}
	cb := classBit(class)
	if cb == 0 || mask&cb == 0 {
		return
	}
	if mask&notifyK != 0 {
		pub("__keyspace@0__:"+key, protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(event)})
	}
	if mask&notifyE != 0 {
		pub("__keyevent@0__:"+event, protocol.Value{Kind: protocol.KindBulkString, Bulk: []byte(key)})
	}
}
