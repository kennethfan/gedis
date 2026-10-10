# TLS v2 dial 端闭环设计（2026-10-08）

## 背景与范围

- 基线：TLS v1 已交付——`config.TLS{enabled, cert_file, key_file}`，缺席默认关闭；`cmd/gedis/main.go:listenMain()` 在 `enabled` 时用 `tls.NewListener` 包主服务口（同端口切换，单向 TLS，MinVersion TLS1.2，缺证书 fail-fast）。
- 问题：v1 只做了服务端。出向 dial 全是明文（`net.Dial/DialTimeout`），`enabled=true` 的集群内部（复制、sentinel gossip/vote、MIGRATE）打不通自己的 TLS 口。
- 目标：5 个逻辑 dial 点（6 处代码）跟随全局 `[tls].enabled` 切 TLS，一次闭环。
- 非目标：Sentinel 监听口、metrics 口、mTLS、独立 `tls_port`（留给 v3）。

## 已确认约束（Q1–Q4 全选 1）

1. 范围：dial 端全闭环——复制、MIGRATE、sentinel peer/vote/hello、存活 probe、failover REPLICAOF，5 点全做。
2. 启用粒度：跟随全局 `[tls].enabled`，同一进程内服务端开则出向全切，不搞每类独立开关。
3. 校验：默认系统根 CA，`ServerName` 取目标 host；新增 `insecure_skip_verify` 显式放行自签/内网。
4. 路线：集中式 dial 包（做法 A），6 处调用点各一行替换。

## 一、配置形状

`internal/config/config.go` 的 `TLS` 加一个字段：

```go
InsecureSkipVerify bool `toml:"insecure_skip_verify"`
```

- 缺席默认 false；`enabled=false` 时 dial 包直通明文，零行为变化。
- `cert_file/key_file` 仍只供服务端监听用，dial 端不读（系统根 CA 做服务端证书校验）。

## 二、dial 包语义（新增 `internal/tlsdial/tlsdial.go`）

- `Configure(cfg config.TLS)`：main 启动时调一次（`listenMain` 旁边），原子存快照；传零值即明文模式。测试用例可重复调以切换模式。
- `DialTimeout(network, addr string, timeout time.Duration) (net.Conn, error)`：
  - 明文模式 → `net.DialTimeout` 原样返回；
  - TLS 模式 → `tls.DialWithDialer(&net.Dialer{Timeout: timeout}, network, addr, tlsCfg)`，其中 `tlsCfg{ServerName: host(addr), MinVersion: TLS1.2, InsecureSkipVerify: 快照值}`，nil RootCAs 即系统根 CA。
- 超时语义与原来完全一致：dial 超时走传入值，建连后的 `SetDeadline` 由各调用方原逻辑保留。

## 三、六点接线（各一行替换，行为除传输层外不变）

1. `internal/replication/client.go:95` `dial()` → `tlsdial.DialTimeout("tcp", c.addr, dialTimeout)`（新增命名常量 `dialTimeout = 5 * time.Second`）。语义增量：原来 `net.Dial` 无超时，TLS 握手 hang 会卡死 loop，加上限兜底。`client_test.go` 走 `Dial` 注入的 pipe，不受影响。
2. `internal/sentinel/peer.go:19` `sendCmd` → `tlsdial.DialTimeout("tcp", addr, peerTimeout)`。一次覆盖 vote（`QueryPeer`）、`FetchRole`、`FetchSlaveInfos` 三条线。
3. `internal/sentinel/peer.go:76` `FetchHello` → `tlsdial.DialTimeout("tcp", peerAddr, timeout)`，timeout 参数原样透传。
4. `internal/sentinel/probe.go:62` `pingOK` → `tlsdial.DialTimeout("tcp", addr, timeout)`，timeout 原样透传。
5. `internal/sentinel/registry.go:187` `dialAndSend` → `tlsdial.DialTimeout("tcp", addr, replicaofTimeout)`。一次覆盖自动 failover 与手动 `FAILOVER` 的 REPLICAOF。
6. `internal/commands/migrate.go:138` `sendRestore` → `tlsdial.DialTimeout("tcp", host:port, o.timeout)`，`o.timeout` 原样。
7. `cmd/gedis/main.go` 启动链加一行 `tlsdial.Configure(cfg.TLS)`。

测试专用 dial（`main_test.go`、`server_test.go`、`cluster_accept_test.go`）不在范围内，保持明文。

## 四、测试矩阵（TDD，先红后绿）

- `tlsdial` 单测（`tlsdial_test.go`，自签证书走 `t.TempDir()`，沿用 v1 测试模式）：
  - 明文模式直通：dial 本地明文口成功；
  - TLS + `insecure=true`：对自签口成功；
  - TLS + `insecure=false`：对自签口握手失败；
  - `ServerName` 取 host：CN=127.0.0.1 的自签证书连 127.0.0.1 能过（`insecure=false` 下验证 ServerName 生效）。
- 回归：`internal/sentinel`、`internal/commands`、`internal/replication`、`cmd/gedis` 全包绿；`go build ./...` + `go vet` 干净。
- 不做（已和用户确认）：TLS 模式下复制 PSYNC 全链路集成用例——`Client.dial` 替换是一行透传，单测覆盖到 `DialTimeout` 分支即够。

## 五、风险与回滚

- 全局 `Configure` 是进程级状态：单测必须显式复位模式，避免包间串扰（`TestMain` 或每用例首尾各调一次）。
- 混合版本集群（有的节点开 TLS、有的没开）dial 会失败——这是跟随全局开关的已知含义，不做自动降级（降级即中间人降级攻击面）。
- 回滚：任一接线点改回 `net.DialTimeout` 即单点回退；整包回滚只需把 `DialTimeout` 实现改回直通。
