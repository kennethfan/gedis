package commands

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/protocol"
	"github.com/kennethfan/gedis/internal/rdb"
	"github.com/kennethfan/gedis/internal/storage"
	"github.com/stretchr/testify/require"
)

func saveSetup(t *testing.T) (*network.Router, *storage.Pebble, string) {
	t.Helper()
	r, store, _, _, _ := openServerSetup(t)
	RegisterHash(r, store)
	dir := t.TempDir()
	SetSaveTarget(store, dir, "dump.rdb")
	return r, store, dir
}

func waitBGSAVEIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		running, _ := saveTracker.BGSAVEState()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("BGSAVE goroutine did not finish within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Given: 库中有 string 与 hash
// When: SAVE
// Then: 回 OK；dump.rdb 可被解析器读回且键一致
func Test_Save_when_Roundtrip(t *testing.T) {
	r, _, dir := saveSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SET", "k", "v"))
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1}, dispatch(r, "HSET", "h", "f", "1"))

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SAVE"))

	raw, err := os.ReadFile(filepath.Join(dir, "dump.rdb"))
	require.NoError(t, err)
	res, err := rdb.Load(bytes.NewReader(raw))
	require.NoError(t, err)
	names := map[string]bool{}
	for _, e := range res.Entries {
		names[e.Key] = true
	}
	require.True(t, names["k"] && names["h"])
}

// Given: SAVE 接线完成
// When: SAVE 带多余参数
// Then: 报 wrong number（真机对齐）
func Test_Save_when_ExtraArg(t *testing.T) {
	r, _, _ := saveSetup(t)
	got := dispatch(r, "SAVE", "extra")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Contains(t, got.S, "wrong number of arguments")
}

// Given: 库中有数据
// When: BGSAVE 后等收尾
// Then: 立即回 Background saving started；落盘成功；LASTSAVE 推进
func Test_Bgsave_when_Async(t *testing.T) {
	r, _, dir := saveSetup(t)
	dispatch(r, "SET", "k", "v")

	got := dispatch(r, "BGSAVE")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "Background saving started"}, got)

	waitBGSAVEIdle(t)
	_, err := os.Stat(filepath.Join(dir, "dump.rdb"))
	require.NoError(t, err)

	running, status := saveTracker.BGSAVEState()
	require.False(t, running)
	require.Equal(t, "ok", status)

	got = dispatch(r, "LASTSAVE")
	require.Equal(t, protocol.KindInteger, got.Kind)
	require.Greater(t, got.I, int64(1700000000))
}

// Given: 后台 save 在飞（直接置标记，避免 flake）
// When: BGSAVE
// Then: 回真机原文错（注意 save 非 saving）
func Test_Bgsave_when_InProgress(t *testing.T) {
	r, _, _ := saveSetup(t)
	saveTracker.mu.Lock()
	saveTracker.bgsaveRunning = true
	saveTracker.mu.Unlock()
	defer func() {
		saveTracker.mu.Lock()
		saveTracker.bgsaveRunning = false
		saveTracker.mu.Unlock()
	}()

	got := dispatch(r, "BGSAVE")
	require.Equal(t, protocol.KindError, got.Kind)
	require.Equal(t, "ERR Background save already in progress", got.S)
}

// Given: 新 setup 未做过 save
// When: LASTSAVE
// Then: 回启动秒（转正前行为保持）
func Test_Lastsave_when_NoSave(t *testing.T) {
	r, _, _ := saveSetup(t)
	require.Equal(t, protocol.Value{Kind: protocol.KindInteger, I: 1700000000}, dispatch(r, "LASTSAVE"))
}

func configVals(v protocol.Value) string {
	s := ""
	for _, e := range v.Elems {
		s += string(e.Bulk) + "\n"
	}
	return s
}

// Given: 默认文件名 dump.rdb
// When: CONFIG GET/SET dbfilename 后 SAVE
// Then: 取值跟随；落盘写到新文件名
func Test_Config_dbfilename(t *testing.T) {
	r, _, dir := saveSetup(t)
	got := dispatch(r, "CONFIG", "GET", "dbfilename")
	require.Contains(t, configVals(got), "dump.rdb")

	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"},
		dispatch(r, "CONFIG", "SET", "dbfilename", "custom.rdb"))
	got = dispatch(r, "CONFIG", "GET", "dbfilename")
	require.Contains(t, configVals(got), "custom.rdb")

	dispatch(r, "SET", "k", "v")
	require.Equal(t, protocol.Value{Kind: protocol.KindSimpleString, S: "OK"}, dispatch(r, "SAVE"))
	_, err := os.Stat(filepath.Join(dir, "custom.rdb"))
	require.NoError(t, err)
}

// Given: INFO persistence 段
// When: SAVE 前后各取一次
// Then: rdb_last_save_time 从启动秒推进；in_progress 恒 0（同步无在飞）
func Test_Info_persistence_reflects_save(t *testing.T) {
	r, _, _ := saveSetup(t)
	before := dispatch(r, "INFO", "persistence")
	require.Contains(t, string(before.Bulk), "rdb_bgsave_in_progress:0")
	require.Contains(t, string(before.Bulk), "rdb_last_save_time:1700000000")

	dispatch(r, "SET", "k", "v")
	dispatch(r, "SAVE")
	after := dispatch(r, "INFO", "persistence")
	require.Contains(t, string(after.Bulk), "rdb_bgsave_in_progress:0")
	require.NotContains(t, string(after.Bulk), "rdb_last_save_time:1700000000")
}
