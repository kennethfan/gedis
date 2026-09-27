package acl

import (
	"strconv"
	"strings"
	"sync"
)

// KeySpec 描述一条命令的参数中哪些位置是 key。
//
// 无键命令用 First: -1 约定（ExtractKeys 直接返回 nil）；未知命令（未登记
// Meta）同样返回 nil，key 检查阶段放行、命令门按类别判定。
type KeySpec struct {
	First  int
	Last   int // -1 表末尾
	Step   int // <=1 表逐个；2 表隔位（如 MSET）
	Custom func(args []string) []int
}

// Meta 是一条命令的静态元数据：类别、读写性、key 位置。
type Meta struct {
	Name     string
	Category string
	ReadOnly bool
	Keys     KeySpec
}

var (
	metaMu sync.RWMutex
	metas  = make(map[string]Meta)
)

// RegisterMeta 登记一条命令的元数据（命令名大小写不敏感）。
func RegisterMeta(m Meta) {
	metaMu.Lock()
	defer metaMu.Unlock()
	metas[strings.ToUpper(m.Name)] = m
}

// LookupMeta 查询命令元数据。
func LookupMeta(cmd string) (Meta, bool) {
	metaMu.RLock()
	defer metaMu.RUnlock()
	m, ok := metas[strings.ToUpper(cmd)]
	return m, ok
}

// ExtractKeys 按元数据从参数中提取 key（args 为命令名之后的参数）。
func ExtractKeys(cmd string, args []string) []string {
	m, ok := LookupMeta(cmd)
	if !ok || m.Keys.First < 0 {
		return nil
	}
	if m.Keys.Custom != nil {
		var out []string
		for _, i := range m.Keys.Custom(args) {
			if i >= 0 && i < len(args) {
				out = append(out, args[i])
			}
		}
		return out
	}
	step := m.Keys.Step
	if step <= 0 {
		step = 1
	}
	last := m.Keys.Last
	if last < 0 || last >= len(args) {
		last = len(args) - 1
	}
	if m.Keys.First >= len(args) {
		return nil
	}
	var out []string
	for i := m.Keys.First; i <= last; i += step {
		out = append(out, args[i])
	}
	return out
}

// SortStoreKey: SORT src [STORE dst] —— src 恒为 key，STORE 后紧跟目的 key。
func SortStoreKey(args []string) []int {
	idx := []int{0}
	for i := 1; i < len(args); i++ {
		if strings.EqualFold(args[i], "STORE") && i+1 < len(args) {
			idx = append(idx, i+1)
			break
		}
	}
	return idx
}

// EvalKeys: EVAL script numkeys k1..kn a1.. —— args[0]=numkeys 取 1..n。
// n 非法/超界返回空（handler 侧 arity 错优先，鉴权门只处理合法提取）。
func EvalKeys(args []string) []int {
	if len(args) == 0 {
		return nil
	}
	n, err := strconv.Atoi(args[0])
	if err != nil || n <= 0 || n > len(args)-1 {
		return nil
	}
	idx := make([]int, 0, n)
	for i := 1; i <= n; i++ {
		idx = append(idx, i)
	}
	return idx
}

// NumkeysKeys 提取 numkeys 形命令的 key：numPos 处为个数，keyPos 起连续 n 个。
// 覆盖 ZUNION/ZINTER/ZDIFF（0,1）与 BLMPOP/BRMPOP/BZMPOP 系列（1,2）。
func NumkeysKeys(numPos, keyPos int) func([]string) []int {
	return func(args []string) []int {
		if numPos >= len(args) {
			return nil
		}
		n, err := strconv.Atoi(args[numPos])
		if err != nil || n <= 0 || keyPos+n > len(args) {
			return nil
		}
		idx := make([]int, 0, n)
		for i := keyPos; i < keyPos+n; i++ {
			idx = append(idx, i)
		}
		return idx
	}
}

// ZStoreKeys 提取 Z*STORE 目的 key：dst(0) + numkeys(1) 起 n 个源 key。
func ZStoreKeys(args []string) []int {
	return append([]int{0}, NumkeysKeys(1, 2)(args)...)
}

// DstSrc 提取“目的 key + 源 key”形命令前两位（ZRANGESTORE/GEOSEARCHSTORE）。
func DstSrc(args []string) []int {
	if len(args) < 2 {
		return nil
	}
	return []int{0, 1}
}

// AllButLast 提取除末位外的全部参数为 key（BLPOP/BRPOP 的 key...timeout 形）。
func AllButLast(args []string) []int {
	if len(args) < 2 {
		return nil
	}
	idx := make([]int, 0, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		idx = append(idx, i)
	}
	return idx
}

// GeoRadiusKeys: GEORADIUS key ... [STORE dst | STOREDIST dst]。
func GeoRadiusKeys(args []string) []int {
	if len(args) == 0 {
		return nil
	}
	idx := []int{0}
	for i := 1; i < len(args); i++ {
		if (strings.EqualFold(args[i], "STORE") || strings.EqualFold(args[i], "STOREDIST")) && i+1 < len(args) {
			idx = append(idx, i+1)
			break
		}
	}
	return idx
}

// XreadKeys 提取 XREAD/XREADGROUP 的流 key：STREAMS 后前半为 key。
func XreadKeys(args []string) []int {
	si := -1
	for i, a := range args {
		if strings.EqualFold(a, "STREAMS") {
			si = i
			break
		}
	}
	if si < 0 || si+1 >= len(args) {
		return nil
	}
	rest := len(args) - si - 1
	if rest%2 != 0 {
		return nil
	}
	idx := make([]int, 0, rest/2)
	for i := si + 1; i < si+1+rest/2; i++ {
		idx = append(idx, i)
	}
	return idx
}

// SubKeyAt1 提取“子命令 key”形命令：OBJECT FREQ key / XGROUP CREATE key ...。
// HELP 或参数不足返回空。
func SubKeyAt1(args []string) []int {
	if len(args) < 2 || strings.EqualFold(args[0], "HELP") {
		return nil
	}
	return []int{1}
}
