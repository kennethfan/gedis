# Cluster fixtures (原生 Redis 真机录制)

录制环境：Redis 7.2.6（本地 `redis-server`，无需 Docker），3 主 + 3 从。

| 端口 | 角色 | slots |
|------|------|-------|
| 7000 | master | 0–5460 |
| 7001 | master | 5461–10922 |
| 7002 | master | 10923–16383 |
| 7003/7004/7005 | replica | — |

## 复现

```bash
# 起 6 节点（配置模板见 nodes/<port>/redis.conf，按需改 dir/logfile 绝对路径）
for p in 7000 7001 7002 7003 7004 7005; do redis-server testdata/cluster/nodes/$p/redis.conf; done
redis-cli --cluster create 127.0.0.1:7000 127.0.0.1:7001 127.0.0.1:7002 \
  127.0.0.1:7003 127.0.0.1:7004 127.0.0.1:7005 --cluster-replicas 1 --cluster-yes
```

注意：macOS 下短时间大量建连会碰到 `Can't assign requested address`
（TIME_WAIT 堆积），命令之间加 `sleep 1` 即可。

## 文件一览

| 文件 | 内容 | 测试时当什么用 |
|------|------|----------------|
| `slots.redis` | `CLUSTER SLOTS` 输出形状 | 数组形状 oracle：`[start end master... replicas...]` |
| `shards.redis` | `CLUSTER SHARDS` 输出形状（Redis ≥ 7.0） | map 形状 oracle：`slots/endpoints/replicas` |
| `info.redis` | `CLUSTER INFO` 输出 | 键名 oracle（计数值/epoch 为实例相关） |
| `nodes.redis` | `CLUSTER NODES` 输出形状 | 行格式 oracle；ID/IP 为实例相关 |
| `moved.redis` | MOVED 全链路：错节点访问 → 精确字节 `-MOVED 12182 127.0.0.1:7002\r\n` → `-c` 自动跟随 | 错误回复措辞 oracle |
| `ask.redis` | ASK 全链路：MIGRATING/IMPORTING 设置 → `-ASK ...\r\n` → 无 ASKING 回 MOVED → 同连接 ASKING+GET 成功 → GETKEYSINSLOT → NODES `[->-/-<-]` 标记 | 迁移态行为 oracle |

## 关键行为（已验证，易错点）

- slot 号是 key 相关的（`foo`→12182）：换 key 则 MOVED/ASK 中的 slot 号不同，**格式**才是 oracle。
- 迁移源端 key 已搬走时返回的是 `ASK` 而非 `MOVED`。
- IMPORTING 端无 ASKING 时回的是 `MOVED`（指回 owner），不是 `ASK`。
- `ASKING` 是单连接一次性：`+OK` 后同一连接内的下一条命令才被放行。
- `CLUSTER SETSLOT ... STABLE` 只清标记不清数据；迁回 key 必须在
  MIGRATING/IMPORTING 标记**还在**时做，且 `MIGRATE` 要发在持有 key 的源端、
  目标端必须处于 IMPORTING（否则目标回 `ASK` 拒绝）。
- `SETSLOT ... MIGRATING` 只能由 slot owner 执行；`IMPORTING` 只能由非 owner 执行。
