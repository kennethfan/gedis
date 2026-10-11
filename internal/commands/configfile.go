package commands

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/BurntSushi/toml"
	"github.com/kennethfan/gedis/internal/protocol"
)

// configFileMu 守卫 configFilePath：main 启动时 SetConfigFile 写一次，
// CONFIG REWRITE 读；读写锁防 REWRITE 与未来重载路径竞态。
var (
	configFileMu   sync.RWMutex
	configFilePath string
)

// SetConfigFile 登记服务端配置文件路径（main 启动时调一次）。
// 空串表示无配置文件（默认行为），REWRITE 照真机探针原文报错。
func SetConfigFile(path string) {
	configFileMu.Lock()
	defer configFileMu.Unlock()
	configFilePath = path
}

// getConfigFile 返回登记的配置文件路径（空=无文件）。
func getConfigFile() string {
	configFileMu.RLock()
	defer configFileMu.RUnlock()
	return configFilePath
}

// rewriteConfig CONFIG REWRITE：把 live 可变参数回写 TOML 配置文件。
// 只回写白名单四参（memory.maxmemory/policy、persistence.dbfilename、
// notify.keyspace_events）；注释格式不保留（文档已声明）。
// 无配置文件时回真机 7.2.6 探针原文。
func (h *monitorHandler) rewriteConfig() protocol.Value {
	path := getConfigFile()
	if path == "" {
		return errValueStr("ERR The server is running without a config file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return errValueStr(fmt.Sprintf("ERR Rewriting config file: %s", err))
	}
	var doc map[string]any
	if _, err := toml.Decode(string(data), &doc); err != nil {
		return errValueStr(fmt.Sprintf("ERR Rewriting config file: %s", err))
	}

	// setSection 确保小节 map 存在后由 put 回填。
	setSection := func(name string, put func(sec map[string]any)) {
		sec, ok := doc[name].(map[string]any)
		if !ok {
			sec = map[string]any{}
		}
		put(sec)
		doc[name] = sec
	}
	if mi, ok := h.kv.(MemoryInfo); ok && mi != nil {
		maxBytes, policy := mi.MaxBytes(), mi.Policy()
		setSection("memory", func(sec map[string]any) {
			sec["maxmemory"] = maxBytes
			sec["policy"] = policy
		})
	}
	setSection("persistence", func(sec map[string]any) {
		sec["dbfilename"] = saveTracker.FileName()
	})
	setSection("notify", func(sec map[string]any) {
		sec["keyspace_events"] = NotifyString()
	})

	out, err := toml.Marshal(doc)
	if err != nil {
		return errValueStr(fmt.Sprintf("ERR Rewriting config file: %s", err))
	}
	// tmp+rename 原子落盘（与 SAVE 同范式）：崩溃不留半截文件。
	tmp := path + ".tmp." + strconv.Itoa(os.Getpid())
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return errValueStr(fmt.Sprintf("ERR Rewriting config file: %s", err))
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return errValueStr(fmt.Sprintf("ERR Rewriting config file: %s", err))
	}
	return protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}
}
