# ACL 设计（对齐 Redis 7）

- 日期：2026-09-27
- 范围决议：C 完整对齐 Redis 7 / A 独立 aclfile / A 默认开放兼容
- 路线：A 命令表驱动落设计，C 分两期实施（PR1 命令级，PR2 key/channel/selectors）
- 相关：`internal/network/router.go`（Dispatch）、`internal/network/server.go`（handle）、`internal/network/context.go`（ctx 注入模式）

## §1 架构：认证与鉴权位置

- 新增 `internal/acl` 包：用户表（并发 map）、密码校验、命令类别表、key-spec 注册表、aclfile 加载/保存。`internal/acl` 不反向依赖 `commands`，只消费接口。
- 身份携带：`network` 新增 `ContextWithUser`/`UserFromContext`（仿 `ContextWithConn` 模式）；`server.handle` 每连接维护当前用户名（初始 `default`），每次循环注入 ctx 后再 `Dispatch`。
- 检查点：`Router.Dispatch` 内、intercept 之后、handler 之前，加 `acl.Check(ctx, cmd)`，三阶：
  1. 未 AUTH 且存在非 nopass 用户 → 仅放行 `AUTH`/`HELLO`/`QUIT`，余者 `NOAUTH Authentication required.`；
  2. 命令 allow/deny（含 `@all`/`-@dangerous` 类别展开，顺序敏感叠加）→ `NOPERM ... '<cmd>' command`；
  3. key pattern（按 key-spec 提取，`~pattern` glob）→ `NOPERM ... keys ...`；channel（`&pattern`）→ `NOPERM ... channel ...`。
- 无用户配置：空表 + `default` nopass，检查点零开销通过，行为与今日逐字节一致。
- `AUTH` 切换用户：commands 层经 `ConnFromContext` 拿 conn，调 server 持有的 conn→user 映射回调（接口在实施时定，以 `server.go` 现有 `OnConnClose` 模式为准）。

## §2 命令元数据表与 key-spec

- 元数据：`{Name, Category, ReadOnly, Keys: KeySpec}`；`KeySpec={FirstKey, LastKey(-1=末尾), KeyStep}`，异形命令（SORT STORE、GEORADIUS STORE、ZUNIONSTORE 目的 key、EVAL numkeys）用 `Custom func(args) []int` 兜底。
- 类别对齐真机：`@all @string @hash @list @set @sortedset @stream @pubsub @transaction @scripting @connection @server @dangerous`。
- 元数据与各 handler 文件内 `Register` 调用点放一起（`var xxxMeta`），逐条核对真机 `COMMAND INFO` key-spec，录制 fixtures 锁定。

## §3 用户模型与 aclfile

- 用户：`flags(on/off/nopass)、passwords[](>明文存、运行时恒定时间比对；亦支持#hash)、commands(+/-与@类别)、keyPatterns(~)、channels(&)、selectors[]（OR）`。
- `default`：无配置时等价 `on nopass +@all ~* &*`；一旦被改写按改写生效。
- aclfile 与 Redis 逐行兼容：`user <name> [on|off] [>pass|#hash|!hash|<nopass|resetpass] [(+|-)cmd|@cat] [~pat] [&pat] [selectors...]`；启动 `Load`，行错拒绝启动并报行号；`ACL SAVE` 经 tmp+rename 原子写回。
- 主配置加 `aclfile <path>`；缺席=纯内存，`ACL SAVE` 回 `ERR No ACL file configured`。
- `AUTH user pass`/`AUTH pass(default)` 双形态；`HELLO 3 AUTH user pass` 同路径；失败 `WRONGPASS invalid username-password pair or user is disabled.`。
- `ACL LOG`：128 环形缓冲记 DENIED（client/对象/命令/时间戳），`ACL LOG [count|RESET]`。

## §4 命令子集与错误措辞

- `AUTH` + `ACL LIST/USERS/WHOAMI/SETUSER/DELUSER/GETUSER/CAT/GENPASS/LOG/SAVE/LOAD/HELP/DRYRUN` 全量；`DRYRUN user cmd args...` 回 `+OK`/`NOPERM ...`。
- 拒绝三件套逐字节对齐：
  - `NOAUTH Authentication required.`
  - `NOPERM this user has no permissions to run the '<cmd>' command`
  - `NOPERM this user has no permissions to access one of the keys used as arguments`
- `COMMAND INFO/DOCS` 透出 `acl_categories`（若 `COMMAND` 未实现，用静态类别表；`COMMAND` 本身列二期可选）。
- Lua 内按调用者用户同样三阶检查；`redis.acl_check_cmd` 不实现（注明 gap）。
- PubSub 按 `&pattern` 检查 SUBSCRIBE/PUBLISH；shard channel 未实现，仅普通 channel，注明。

## §5 分期与验收

- PR1：`internal/acl` + 身份 + `AUTH`/`ACL（除DRYRUN/LOG外）` + 命令级检查 + 默认开放兼容。验收：无配置全量回归零差异；AUTH 双形态；类别叠加；aclfile round-trip。
- PR2：全 key-spec + `~`/`&`/selectors + `DRYRUN`/`LOG` + Lua 内检查 + 真机录制对齐（含 NOPERM 逐字节比对、`ACL LIST` 同形）。
- 非目标：`redis.acl_check_cmd`、shard channel、Cluster/Sentinel 端口差异化策略、SHA256 外哈希。
- 兼容红线：零配置时现有 `go test ./...` 零修改通过；RESP 措辞以真机录制为准。

## 自检

- 无 TBD/TODO 占位；§1–§5 无矛盾（key-spec 体量大→PR2，命令级→PR1，与路线 C 一致）。
- 歧义已收敛：密码明文存（对齐 Redis，§3 明确）；`AUTH` 接口细节推迟到实施（§1 已声明，非占位）。
- 单 plan 可拆两期，writing-plans 时拆为两个 plan 文件或一个 plan 两阶段（由 plan 阶段定）。
