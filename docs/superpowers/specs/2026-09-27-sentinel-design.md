# Sentinel 最小发现版设计（M7 #42）

- 日期：2026-09-27
- 范围：最小发现版 × 独立端口 × 真机录制对齐（原生 7.2.6）
- 路线：A（独立哨兵端口 + 静态 masters + 复用 REPLICAOF），B/C 已否决
- 状态：待用户评审，评审通过后调 writing-plans 进实现

## §1 架构与进程形态

- 同一二进制，双 listener：数据口（现有 `server.host/port`）+ 哨兵口（新增 `[sentinel] port`，默认 26379）。
- `[sentinel] enabled=false`（缺席）即关闭哨兵口，现有单节点启动零影响。
- `cmd/gedis/main.go`：解析 `[sentinel]` 后，若 enabled 则另起 `network.Router` + `internal/sentinel.Registry`，只注册哨兵命令（`SENTINEL/PING/INFO/SUBSCRIBE` 子集），不挂载 KV/Cluster intercept；数据面零改动。
- 新包 `internal/sentinel/`：静态注册表（master 名 → 主地址 + slaves 列表 + 当前主缓存 + down 标记），周期 `net.DialTimeout + PING` 探活，不做哨兵间 gossip/选举。
- `FAILOVER` 时哨兵作为客户端向新主发 `REPLICAOF NO ONE`、向旧主发 `REPLICAOF <newHost> <newPort>`，复用现有 `replication.Client` 链路语义。

## §2 配置形状（对标 `[cluster]` 静态风格，fail-fast）

```toml
[sentinel]
enabled = true
port = 26379
down_after_ms = 5000

[[sentinel.masters]]
name = "mymaster"
master_addr = "127.0.0.1:6380"
quorum = 1
slaves = ["127.0.0.1:6381"]
```

- `Config` 新增 `Sentinel` 结构 + `Specs()` 展开校验：master 名非空唯一、addr 合法、slaves 非空；缺席即关闭。
- `down_after_ms` 缺席默认 5000，显式 0 表示只报配置主、不探活；`<0` 启动报错退出。
- 多 master 即多条 `[[sentinel.masters]]`，按 name 分片互不干扰；`SENTINEL sentinels <name>` 固定返回本哨兵自身一行（静态，无 gossip）。

## §3 命令子集（哨兵口只 serving 这些）

- `SENTINEL masters`：全 masters 数组（`name/ip/port/quorum/down-after/flags`，`flags` 仅 `master`/`o-down`）。
- `SENTINEL master <name>` / `slaves <name>` / `sentinels <name>`：单 master / slaves 列表（`role/offset` 取静态 + 探活状态，`offset` 填 0）/ 哨兵列表（仅自身一行）。
- `SENTINEL get-master-addr-by-name <name>`：当前主 `[ip port]`（手动 FAILOVER 后翻转，未 failover 即配置主）。
- `SENTINEL failover <name>`：手动触发一次切换（见 §4），返回 `+OK`。
- `SENTINEL reset <pattern>` / `remove <name>` / `set <name> …`：最小版静态拒绝（`ERR Static sentinel …`，对齐 Cluster 拓扑变更拒绝风格）；`flushconfig` 回 `+OK`。
- `PING` / `INFO sentinel`（`sentinel_masters` 最小字段）/ `SUBSCRIBE __sentinel__:hello / +switch-master`（可订阅，`+switch-master` 仅手动 FAILOVER 后发布一次，不做 hello gossip）。
- 端口隔离：`SENTINEL` 在数据口返回 `ERR unknown command`；数据命令在哨兵口同样拒绝。

## §4 探活与手动切换流程

- 探活：按 `down_after_ms` 周期 `DialTimeout+PING` 逐个探 master/slaves；超时即标 `o-down`（仅影响展示，不触发自动切换）；`down_after_ms=0` 跳过探活。
- `SENTINEL failover <name>` 串行步骤（任一步失败即 abort 并回错）：
  1. 选目标：slaves 中第一个非 `o-down` 者；全 down 则 `ERR No healthy slave`。
  2. 向目标 `REPLICAOF NO ONE`（dial 数据口直发，使其变可写主）。
  3. 向旧主 `REPLICAOF <newIP> <newPort>`（使其变从；旧主不可达则只翻缓存 + 告警，不回滚第 2 步）。
  4. 翻转注册表当前主缓存 + 经 Hub 发布 `+switch-master <name> <oldIP> <oldPort> <newIP> <newPort>` 一次。
- 并发：同 name failover 用 mutex 串行；探活与 failover 经 RWMutex 互斥，切换瞬间探活暂停一次。

## §5 错误边界与非目标

- 不做：哨兵间投票/quorum 选举、epoch 自增选主、`SDOWN→ODOWN` quorum 判定、`__sentinel__:hello` gossip、多哨兵一致性；`quorum` 只展示固定 1。
- 全 down 时哨兵如实报 `o-down`，`get-master-addr` 仍返回最后缓存主（对齐真机“报最后已知主”，不返回空）。
- 哨兵口故障不影响数据口。
- 超时：探活 `DialTimeout=down_after_ms/2`；`FAILOVER` 中 `REPLICAOF` 单次 2s 超时，重试 1 次仍失败即 abort。

## §6 验收（原生 7.2.6 真机录制法）

- fixtures `testdata/sentinel/*.redis`：一主（6380）一从（6381）一哨兵（26379），录 `masters/master/slaves/sentinels/get-master-addr-by-name` 形状 + `failover mymaster` 后地址翻转 + `+switch-master` 消息。
- `internal/sentinel/*_test.go`（注册表翻转/选从/abort）+ `internal/commands/sentinel_accept_test.go`（fixtures 对齐）。
- 门禁：`go build/vet/test ./...` 全绿；`redis-cli -p 26379` 执行上述命令不报错。

## 自检

- 无 TBD/TODO 占位；§1–§6 无矛盾（静态注册表 ↔ 无选举 ↔ quorum=1 ↔ 拒绝 set/remove 一致）。
- 单计划可执行：配置 → 注册表 → 命令 → 切换 → 验收，无需拆子 spec。
- 歧义已收敛：`get-master-addr` 全 down 返回最后缓存主（非空）；`FAILOVER` 第 3 步旧主不可达不回滚。
