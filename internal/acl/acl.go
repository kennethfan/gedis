package acl

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	// KeyPatterns/Channels 是二期（key/channel/selectors）的扩展点，本期恒空。
	KeyPatterns []string
	Channels    []string
}

type Store struct {
	mu    sync.RWMutex
	users map[string]*User
}

func NewStore() *Store {
	st := &Store{users: make(map[string]*User)}
	st.users[DefaultUser] = &User{On: true, NoPass: true, rules: []ruleOp{{allow: true, token: "@all"}}}
	return st
}

func (s *Store) SetUser(name string, rules ...string) error {
	u := &User{}
	for _, r := range rules {
		switch {
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
		default:
			return fmt.Errorf("acl: unknown rule %q", r)
		}
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

var cmdCategory = map[string]string{
	"GET": "string", "SET": "string", "DEL": "generic", "PING": "connection",
	"AUTH": "connection", "ACL": "connection",
}

func inCategory(cmd, cat string) bool {
	if cat == "all" {
		return true
	}
	c, ok := cmdCategory[cmd]
	return ok && c == cat
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
