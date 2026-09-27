package acl

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ParseLine 解析单行 aclfile（`user <name> <rules...>`，与 ACL LIST 同形）。
func ParseLine(line string) (string, []string, error) {
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "user" {
		return "", nil, fmt.Errorf("must start with 'user <name>'")
	}
	return fields[1], fields[2:], nil
}

// Load 逐行读入用户表；行错返回行号。
func Load(path string, st *Store) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, rules, err := ParseLine(line)
		if err != nil {
			return fmt.Errorf("aclfile line %d: %w", i+1, err)
		}
		if err := st.SetUser(name, rules...); err != nil {
			return fmt.Errorf("aclfile line %d: %w", i+1, err)
		}
	}
	return nil
}

// Save 原子写用户表（tmp+rename）；口令落盘为 #哈希（内存不存明文）。
func Save(path string, st *Store) error {
	var sb strings.Builder
	for _, n := range st.Names() {
		snap, _ := st.GetUser(n)
		sb.WriteString(describeLine(n, snap))
		sb.WriteByte('\n')
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(sb.String()), 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Clean(path)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func describeLine(name string, snap Snapshot) string {
	parts := []string{"user", name}
	if snap.On {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}
	if snap.NoPass {
		parts = append(parts, "nopass")
	} else if snap.PassHash != "" {
		parts = append(parts, "#"+snap.PassHash)
	}
	parts = append(parts, snap.Rules...)
	for _, k := range snap.Keys {
		parts = append(parts, "~"+k)
	}
	for _, c := range snap.Chans {
		parts = append(parts, "&"+c)
	}
	for _, sel := range snap.Selectors {
		parts = append(parts, sel.Inline())
	}
	return strings.Join(parts, " ")
}
