// Command gedis 是 Gedis 的入口：解析 flag、加载 TOML 配置、打开存储引擎。
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/kennethfan/gedis/internal/commands"
	"github.com/kennethfan/gedis/internal/config"
	"github.com/kennethfan/gedis/internal/metrics"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/storage"
)

func main() {
	if err := run(); err != nil {
		slog.Error("gedis exited", "err", err)
		os.Exit(1)
	}
}

func mustParseFsync(s string) storage.FsyncPolicy {
	policy, err := storage.ParseFsyncPolicy(s)
	if err != nil {
		slog.Error("invalid fsync policy", "fsync", s)
		os.Exit(1)
	}
	return policy
}

func run() error {
	configPath := flag.String("config", "gedis.toml", "TOML 配置文件路径")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	slog.Info("gedis starting",
		"host", cfg.Server.Host,
		"port", cfg.Server.Port,
		"dataDir", cfg.Storage.DataDir,
	)

	hub := replication.NewHub(4096)
	store := storage.NewWithOptions(cfg.Storage.DataDir, storage.Options{
		AppendOnly: cfg.Persistence.AppendOnly,
		Fsync:      mustParseFsync(cfg.Persistence.Fsync),
		Hub:        hub,
	})
	store.SetMaxBytes(cfg.Memory.MaxMemory)
	if err := store.SetPolicy(cfg.Memory.Policy); err != nil {
		return fmt.Errorf("invalid maxmemory-policy: %w", err)
	}
	if err := store.Open(); err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	defer func() { _ = store.Close() }()

	router := network.DefaultRouter()
	srv := network.NewServer(router)
	stats := srv.Stats()
	if cfg.Metrics.Enabled && cfg.Metrics.Port > 0 {
		addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Metrics.Port)
		go func() {
			slog.Info("metrics listening", "addr", addr)
			if err := http.ListenAndServe(addr, metrics.Handler(stats)); err != nil {
				slog.Error("metrics server exited", "err", err)
			}
		}()
	}
	commands.RegisterStrings(router, store)
	commands.RegisterHash(router, store)
	commands.RegisterList(router, store, stats)
	commands.RegisterSet(router, store)
	commands.RegisterZSet(router, store)
	commands.RegisterGeo(router, store)
	commands.RegisterScan(router, store)
	commands.RegisterGeneric(router, store)
	commands.RegisterBitmap(router, store)
	commands.RegisterHLL(router, store)
	commands.RegisterStream(router, store, stats)
	commands.RegisterMonitor(router, store, stats, hub)
	commands.RegisterReplication(router, store, stats, hub)
	commands.RegisterWriteCommands(router)
	exp := commands.NewExpirer(store, stats)
	exp.Start()

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	slog.Info("gedis listening", "addr", ln.Addr())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutting down")
		exp.Stop()
		_ = srv.Close()
	}()

	if err := srv.Serve(ln); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
