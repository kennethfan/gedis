// Command gedis 是 Gedis 的入口：解析 flag、加载 TOML 配置、打开存储引擎。
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/kennethfan/gedis/internal/acl"
	"github.com/kennethfan/gedis/internal/cluster"
	"github.com/kennethfan/gedis/internal/commands"
	"github.com/kennethfan/gedis/internal/config"
	"github.com/kennethfan/gedis/internal/metrics"
	"github.com/kennethfan/gedis/internal/network"
	"github.com/kennethfan/gedis/internal/replication"
	"github.com/kennethfan/gedis/internal/sentinel"
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

// listenMain 建主服务监听：TLS 关闭时返回明文 listener；启用时加载证书
// 并用 tls.NewListener 包装（同端口切换，该口只讲 TLS）。证书缺失 fail-fast。
func listenMain(host string, port int, tlsCfg config.TLS) (net.Listener, error) {
	addr := net.JoinHostPort(host, fmt.Sprintf("%d", port))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	if !tlsCfg.Enabled {
		return ln, nil
	}
	cert, err := tls.LoadX509KeyPair(tlsCfg.CertFile, tlsCfg.KeyFile)
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("load tls cert: %w", err)
	}
	return tls.NewListener(ln, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}), nil
}

func loadClusterSnapshot(path string, topo *cluster.Topology) error {	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read cluster snapshot: %w", err)
	}
	snap, err := cluster.UnmarshalSnapshot(data, topo.Nodes())
	if err != nil {
		return fmt.Errorf("parse cluster snapshot: %w", err)
	}
	if err := topo.LoadSnapshot(snap); err != nil {
		return fmt.Errorf("load cluster snapshot: %w", err)
	}
	return nil
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
	commands.RegisterDumpRestore(router, store)
	commands.RegisterBitmap(router, store)
	commands.RegisterHLL(router, store)
	commands.RegisterStream(router, store, stats)
	commands.RegisterMonitor(router, store, stats, hub)
	commands.RegisterReplication(router, store, stats, hub)
	commands.RegisterWriteCommands(router)
	txnReg := commands.RegisterTxn(router, hub)
	pubsubReg := commands.RegisterPubSub(router)
	luaTimeout, err := cfg.Lua.EffectiveTimeLimit()
	if err != nil {
		return fmt.Errorf("invalid lua.time_limit: %w", err)
	}
	commands.RegisterLua(router, luaTimeout)
	var clusterTopo *cluster.Topology
	if cfg.Cluster.Enabled {
		specs, err := cfg.Cluster.Specs()
		if err != nil {
			return fmt.Errorf("invalid cluster.nodes: %w", err)
		}
		selfAddr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
		clusterTopo, err = cluster.Build(selfAddr, specs)
		if err != nil {
			return fmt.Errorf("invalid cluster topology: %w", err)
		}
		slog.Info("cluster mode enabled", "self", clusterTopo.SelfAddr(), "nodes", len(clusterTopo.Nodes()))
	}
	askingReg := commands.NewAskRegistry()
	commands.RegisterMigrate(router, store)
	clusterH := commands.RegisterCluster(router, store, clusterTopo, askingReg)
	txnReg.PreExec = clusterH.CheckExec
	if clusterTopo != nil && cfg.Storage.DataDir != "" {
		snapPath := filepath.Join(cfg.Storage.DataDir, "nodes.conf")
		if err := loadClusterSnapshot(snapPath, clusterTopo); err != nil {
			return err
		}
		nodes := clusterTopo.Nodes()
		clusterH.SetPersist(func(snap cluster.Snapshot) error {
			return os.WriteFile(snapPath, cluster.MarshalSnapshot(snap, nodes), 0o644)
		})
	}
	aclStore := acl.NewStore()
	if cfg.ACLFile != "" {
		if err := acl.Load(cfg.ACLFile, aclStore); err != nil {
			return fmt.Errorf("load aclfile: %w", err)
		}
	}
	if cfg.RequirePass != "" {
		if err := aclStore.SetUser("default", "on", ">"+cfg.RequirePass, "+@all"); err != nil {
			return fmt.Errorf("invalid requirepass: %w", err)
		}
	}
	authReg := commands.RegisterAuth(router, aclStore)
	var aclSaver commands.ACLSaver
	if cfg.ACLFile != "" {
		aclSaver = func() error { return acl.Save(cfg.ACLFile, aclStore) }
	}
	commands.RegisterACL(router, aclStore, authReg, aclSaver)
	router.SetAuthorizer(aclStore)
	srv.UserProvider = authReg
	var sentinelSrv *network.Server
	var sentinelStop chan struct{}
	if cfg.Sentinel.Enabled {
		specs, err := cfg.Sentinel.Specs()
		if err != nil {
			return fmt.Errorf("invalid sentinel config: %w", err)
		}
		sentinelPort := cfg.Sentinel.Port
		if sentinelPort == 0 {
			sentinelPort = config.DefaultSentinelPort
		}
		sentinelSelf := fmt.Sprintf("%s:%d", cfg.Server.Host, sentinelPort)
		sentinelReg := sentinel.NewRegistry(specs, time.Duration(cfg.Sentinel.DownAfterMs)*time.Millisecond)
		sentinelStop = make(chan struct{})
		go sentinelReg.StartProbeLoop(sentinelStop)
		sRouter := network.DefaultRouter()
		sentinelPub := commands.RegisterPubSub(sRouter)
		commands.RegisterSentinel(sRouter, sentinelReg, sentinelSelf, sentinelPub)
		sentinelReg.Peers.SeedPeers(cfg.Sentinel.Sentinels)
		runSentinelLoops(sentinelReg, sentinelSelf, sentinel.NewRunID(),
			time.Duration(cfg.Sentinel.DownAfterMs)*time.Millisecond,
			time.Duration(cfg.Sentinel.FailoverTimeoutMs)*time.Millisecond,
			sentinelPub, sentinelStop)
		sentinelSrv = network.NewServer(sRouter)
		sentinelSrv.OnConnClose(func(c net.Conn) {
			sentinelPub.ConnClosed(c)
		})
		sln, err := net.Listen("tcp", sentinelSelf)
		if err != nil {
			close(sentinelStop)
			return fmt.Errorf("listen sentinel %s: %w", sentinelSelf, err)
		}
		slog.Info("sentinel listening", "addr", sln.Addr(), "masters", len(specs))
		go func() {
			if err := sentinelSrv.Serve(sln); err != nil {
				slog.Error("sentinel server exited", "err", err)
			}
		}()
	}
	txnReg.PreExec = clusterH.CheckExec
	srv.OnConnClose(func(c net.Conn) {
		txnReg.ConnClosed(c)
		pubsubReg.ConnClosed(c)
		askingReg.ConnClosed(c)
		authReg.ConnClosed(c)
	})
	exp := commands.NewExpirer(store, stats)
	exp.Start()
	defer exp.Stop() // 早退路径（监听失败等）先停清扫再关存储，防 SweepOnce 扫已关 DB panic

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	ln, err := listenMain(cfg.Server.Host, cfg.Server.Port, cfg.TLS)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	slog.Info("gedis listening", "addr", ln.Addr(), "tls", cfg.TLS.Enabled)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		slog.Info("shutting down")
		exp.Stop()
		if sentinelStop != nil {
			close(sentinelStop)
		}
		if sentinelSrv != nil {
			_ = sentinelSrv.Close()
		}
		_ = srv.Close()
	}()

	if err := srv.Serve(ln); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
