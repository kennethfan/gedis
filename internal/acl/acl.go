package acl

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"time"
)

const DefaultUser = "default"

type ruleOp struct {
	allow bool
	token string // "@all" / "@string" / "GET" ...
}

type User struct {
	On       bool
	NoPass   bool
	passHash [32]byte
	hasPass  bool
	rules    []ruleOp
	KeyPatterns []string
	Channels    []string
	Selectors   []Selector
}

// Selector 是括号规则组（命令+key+channel 三元组，Check 时与主规则 OR）。
type Selector struct {
	Cmds []ruleOp
	Keys []string
	Chans []string
}

type Store struct {
	mu    sync.RWMutex
	users map[string]*User
	log   []LogEntry
}

func NewStore() *Store {
	st := &Store{users: make(map[string]*User)}
	st.users[DefaultUser] = &User{On: true, NoPass: true, rules: []ruleOp{{allow: true, token: "@all"}}}
	return st
}

func (s *Store) SetUser(name string, rules ...string) error {
	u := &User{}
	inSel := false
	var cur Selector
	flushSel := func() {
		u.Selectors = append(u.Selectors, cur)
		cur = Selector{}
	}
	for _, r := range rules {
		if inSel {
			switch {
			case r == ")":
				flushSel()
				inSel = false
			case strings.HasPrefix(r, "+"), strings.HasPrefix(r, "-"):
				cur.Cmds = append(cur.Cmds, ruleOp{allow: r[0] == '+', token: r[1:]})
			case strings.HasPrefix(r, "~"):
				cur.Keys = append(cur.Keys, r[1:])
			case strings.HasPrefix(r, "&"):
				cur.Chans = append(cur.Chans, r[1:])
			default:
				return fmt.Errorf("acl: invalid selector rule %q", r)
			}
			continue
		}
		switch {
		case r == "(":
			inSel = true
		case r == ")":
			return fmt.Errorf("acl: unexpected %q without selector", r)
		case r == "on":
			u.On = true
		case r == "off":
			u.On = false
		case r == "nopass":
			u.NoPass = true
		case r == "reset":
		case strings.HasPrefix(r, ">"):
			u.passHash = sha256.Sum256([]byte(r[1:]))
			u.hasPass = true
		case strings.HasPrefix(r, "#"):
			h, err := hex.DecodeString(r[1:])
			if err != nil || len(h) != 32 {
				return fmt.Errorf("acl: invalid password hash %q", r)
			}
			copy(u.passHash[:], h)
			u.hasPass = true
		case strings.HasPrefix(r, "+"), strings.HasPrefix(r, "-"):
			u.rules = append(u.rules, ruleOp{allow: r[0] == '+', token: r[1:]})
		case strings.HasPrefix(r, "~"):
			u.KeyPatterns = append(u.KeyPatterns, r[1:])
		case strings.HasPrefix(r, "&"):
			u.Channels = append(u.Channels, r[1:])
		default:
			return fmt.Errorf("acl: unknown rule %q", r)
		}
	}
	if inSel {
		return fmt.Errorf("acl: unclosed selector, missing %q", ")")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[name] = u
	return nil
}

func (s *Store) Authenticate(name, pass string) bool {
	s.mu.RLock()
	u, ok := s.users[name]
	s.mu.RUnlock()
	if !ok || !u.On {
		return false
	}
	if u.NoPass {
		return true
	}
	if !u.hasPass {
		return false
	}
	sum := sha256.Sum256([]byte(pass))
	return subtle.ConstantTimeCompare(sum[:], u.passHash[:]) == 1
}

func inCategory(cmd, cat string) bool {
	if cat == "all" {
		return true
	}
	m, ok := LookupMeta(cmd)
	return ok && m.Category == cat
}

// CanRun 按规则顺序叠加求值：命中最后一条匹配规则；无命中默认拒绝（default 用户 +@all 兜底）。
func (s *Store) CanRun(name, cmd string) bool {
	s.mu.RLock()
	u, ok := s.users[name]
	rules := append([]ruleOp(nil), u.rules...)
	s.mu.RUnlock()
	if !ok || !u.On {
		return false
	}
	return evalRules(rules, cmd)
}

func evalRules(rules []ruleOp, cmd string) bool {
	allowed := false
	for _, r := range rules {
		tok := strings.ToUpper(r.token)
		match := false
		if strings.HasPrefix(tok, "@") {
			match = inCategory(strings.ToUpper(cmd), strings.TrimPrefix(strings.ToLower(tok), "@"))
		} else {
			match = strings.EqualFold(tok, cmd)
		}
		if match {
			allowed = r.allow
		}
	}
	return allowed
}

func commandNOPERM(user, cmd string) error {
	cmd = strings.ToLower(cmd)
	if user == DefaultUser {
		return &Denial{Kind: DenyCommand, User: user, Cmd: cmd, msg: fmt.Sprintf("NOPERM this user has no permissions to run the '%s' command", cmd)}
	}
	return &Denial{Kind: DenyCommand, User: user, Cmd: cmd, msg: fmt.Sprintf("NOPERM User %s has no permissions to run the '%s' command", user, cmd)}
}

// DenyKind 标识拒绝发生的阶段（DRYRUN 与脚本内检查据此改写文案）。
type DenyKind int

const (
	DenyCommand DenyKind = iota
	DenyKey
	DenyChannel
)

// Denial 是 Check 的拒绝形态：Error() 为直接下发客户端的 NOPERM 原文；
// Kind/Name 供 DRYRUN（逐 key 文案）与脚本内检查（ACL failure in script 包装）改写。
type Denial struct {
	Kind       DenyKind
	User, Cmd  string
	Name       string
	msg        string
}

func (d *Denial) Error() string { return d.msg }

// AsDenial 从 Check 返回的 error 还原 Denial。
func AsDenial(err error) (*Denial, bool) {
	d, ok := err.(*Denial)
	return d, ok
}

func matchAny(patterns []string, s string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, s); ok {
			return true
		}
	}
	return false
}

// Check 三阶鉴权：用户存在+on → 主规则三元组（命令/keys/channels）→
// 任一 selector 三元组 OR。返回 nil 表放行；error 的 Error() 即 RESP 措辞原文。
func (s *Store) Check(user, cmd string, keys, channels []string) error {
	s.mu.RLock()
	u, ok := s.users[user]
	rules := append([]ruleOp(nil), u.rules...)
	patterns := append([]string(nil), u.KeyPatterns...)
	chans := append([]string(nil), u.Channels...)
	sels := append([]Selector(nil), u.Selectors...)
	on := u.On
	s.mu.RUnlock()
	if !ok || !on {
		return commandNOPERM(user, cmd)
	}
	mainErr := checkTriple(user, cmd, rules, patterns, chans, keys, channels)
	if mainErr == nil {
		return nil
	}
	for _, sel := range sels {
		if checkTriple(user, cmd, sel.Cmds, sel.Keys, sel.Chans, keys, channels) == nil {
			return nil
		}
	}
	return mainErr
}

func checkTriple(user, cmd string, rules []ruleOp, patterns, chans, keys, channels []string) error {
	if !evalRules(rules, cmd) {
		return commandNOPERM(user, cmd)
	}
	for _, k := range keys {
		if !matchAny(patterns, k) {
			return &Denial{Kind: DenyKey, User: user, Cmd: strings.ToLower(cmd), Name: k, msg: "NOPERM No permissions to access a key"}
		}
	}
	for _, c := range channels {
		if !matchAny(chans, c) {
			return &Denial{Kind: DenyChannel, User: user, Cmd: strings.ToLower(cmd), Name: c, msg: "NOPERM No permissions to access a channel"}
		}
	}
	return nil
}

// LogEntry 是 ACL LOG 的单条拒绝记录。
type LogEntry struct {
	Time   int64
	Client string
	Cmd    string
	Reason string
}

func (s *Store) LogDenied(client, cmd, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, LogEntry{Time: time.Now().Unix(), Client: client, Cmd: cmd, Reason: reason})
	if len(s.log) > 128 {
		s.log = s.log[len(s.log)-128:]
	}
}

// AclLog 返回拒绝记录（ newest first 拷贝）。
func (s *Store) AclLog() []LogEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]LogEntry, len(s.log))
	for i, e := range s.log {
		out[len(s.log)-1-i] = e
	}
	return out
}

// ResetLog 清空拒绝记录。
func (s *Store) ResetLog() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = nil
}

func (s *Store) UserExists(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.users[name]
	return ok
}

// DelUser 删除用户；default 不可删。
func (s *Store) DelUser(name string) error {
	if name == DefaultUser {
		return fmt.Errorf("acl: the 'default' user cannot be removed")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[name]; !ok {
		return nil
	}
	delete(s.users, name)
	return nil
}

// Names 返回排序后的用户名单（ACL USERS 用）。
func (s *Store) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.users))
	for n := range s.users {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Snapshot 是用户只读快照（ACL LIST/GETUSER 渲染用，口令只给哈希）。
type Snapshot struct {
	On       bool
	NoPass   bool
	PassHash string
	Rules    []string
	Keys     []string
	Chans    []string
	// Selectors 是括号组的结构化视图（tokens 保持 +/-/~/& 前缀原样）。
	Selectors []SelectorView
}

// SelectorView 是单个括号组的可渲染视图。
type SelectorView struct {
	Commands []string
	Keys     []string
	Chans    []string
}

// Inline 把括号组渲染为 LIST/aclfile 行内形状，如 "(+get ~cache:*)"。
func (v SelectorView) Inline() string {
	toks := append([]string{}, v.Commands...)
	toks = append(toks, v.Keys...)
	toks = append(toks, v.Chans...)
	return "(" + strings.Join(toks, " ") + ")"
}

// GetUser 取用户快照；规则按 "+token"/"-token" 规范串返回。
func (s *Store) GetUser(name string) (Snapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[name]
	if !ok {
		return Snapshot{}, false
	}
	snap := Snapshot{On: u.On, NoPass: u.NoPass}
	if u.hasPass {
		snap.PassHash = hex.EncodeToString(u.passHash[:])
	}
	snap.Keys = append([]string(nil), u.KeyPatterns...)
	snap.Chans = append([]string(nil), u.Channels...)
	for _, sel := range u.Selectors {
		var vw SelectorView
		for _, r := range sel.Cmds {
			sign := "+"
			if !r.allow {
				sign = "-"
			}
			vw.Commands = append(vw.Commands, sign+r.token)
		}
		for _, k := range sel.Keys {
			vw.Keys = append(vw.Keys, "~"+k)
		}
		for _, c := range sel.Chans {
			vw.Chans = append(vw.Chans, "&"+c)
		}
		snap.Selectors = append(snap.Selectors, vw)
	}
	for _, r := range u.rules {
		sign := "+"
		if !r.allow {
			sign = "-"
		}
		snap.Rules = append(snap.Rules, sign+r.token)
	}
	return snap, true
}

// HasPassword 用户是否配置了口令（#哈希同样算）。
func (s *Store) HasPassword(name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[name]
	return ok && u.hasPass
}

// DefaultRequiresAuth 默认用户是否要求认证（缺失/off/设口令即要求）。
func (s *Store) DefaultRequiresAuth() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[DefaultUser]
	return !ok || !u.On || (!u.NoPass && u.hasPass)
}
// 纯默认（on nopass +@all）时为 false，鉴权门零开销通过。
// HasRestrictedUsers 存在除默认全开放 default 外的用户配置即 true；
// 纯默认（on nopass +@all）时为 false，鉴权门零开销通过。
func (s *Store) HasRestrictedUsers() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.users) != 1 {
		return true
	}
	d, ok := s.users[DefaultUser]
	return !ok || !d.NoPass || len(d.rules) != 1 || d.rules[0].token != "@all" || !d.rules[0].allow
}
