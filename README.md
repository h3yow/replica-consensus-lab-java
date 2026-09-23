# 副本一致性协调组件（Replica Consensus Lab · Java）

在**单个 Spring Boot 进程内**用多个副本节点模拟一个 Raft 风格的一致性集群，
覆盖**领导者选举与任期推进、幂等指令提交、故障恢复与日志一致性**三类核心问题。

- 不依赖任何外部中间件（无 Kafka / Redis / 数据库）；
- 不发起真实网络连接，节点间 RPC 由 `Cluster` 在方法调用层面同步投递；
- 不使用后台定时器与随机超时，协议通过显式的 `tickElection()` / `tickReplication()`
  确定性驱动，因此每次运行结果可复现、可解释，日志逐步展示投票、提交判定与恢复过程。

## 1. 目录结构

```
src/main/java/com/github/h3yow/replicaconsensuslabjava/
├── consensus/
│   ├── RaftNode.java             # 单个副本：选举、复制、提交、快照、恢复
│   ├── Cluster.java              # 进程内集群：节点管理、分区模拟、RPC 投递、tick 驱动
│   ├── RaftMessages.java         # RequestVote / AppendEntries（含快照安装）消息
│   ├── LogEntry.java             # 日志条目（term/index/幂等键/指令/是否已提交）
│   ├── Snapshot.java             # 状态机快照 + 幂等表
│   ├── KeyValueStateMachine.java # 键值状态机与幂等去重表
│   ├── NodeStore.java            # 持久化抽象
│   ├── FileNodeStore.java        # 本地 JSON 文件实现（meta/log/snapshot）
│   ├── CommitResult.java         # 提交结论（含可区分原因）
│   ├── CommitReason.java         # APPLIED / DUPLICATE_SUPPRESSED / CONFLICT / NOT_LEADER / ...
│   ├── NodeRole.java             # FOLLOWER / CANDIDATE / LEADER
│   ├── ConsensusProperties.java  # consensus.* 可配置项
│   └── ConsensusConfig.java      # Spring Bean 装配
└── web/
    └── ConsensusController.java  # 本地 HTTP 入口（手工驱动与观察）
src/test/java/.../tests/
├── ElectionTest.java             # 分区下的选举、同任期双领导者拒绝、恢复收敛
├── IdempotencyTest.java          # 重复提交抑制、同键冲突、重试/分区一致性
└── RecoveryTest.java             # 崩溃重启回放、未提交不生效、分歧修复、掉队追赶
```

## 2. 一致性协议说明

### 2.1 领导者选举与任期（term）

1. 节点以跟随者（FOLLOWER）身份启动；选举 tick 到来且当前无法连通合法领导者时，
   节点将 `currentTerm + 1`、转为候选人（CANDIDATE）、先给自己投票，再向可见节点拉票。
2. 跟随者在一个任期内**至多投一张票**，且只投给“日志至少和自己一样新”的候选人。
3. 候选人获得**多数派**（`quorum = floor(n/2)+1`，3 节点为 2）选票后成为领导者。
4. **同一任期不可能出现两个被多数派承认的领导者**：旧领导者在其分区内即便仍自认为
   领导者，其 `AppendEntries` 携带的任期更小，会被新任期的节点拒绝；网络恢复后旧领导者
   发现更高任期立即退回跟随者，集群重新收敛到唯一领导者。
5. 少数派分区无法凑齐多数派，既选不出领导者，也无法让任何新指令“提交”。

### 2.2 日志复制与提交

- 领导者把客户端指令按 `term + 单调递增 index` 追加到本地日志，通过 `AppendEntries`
  复制给跟随者；当某条**当前任期**的日志被多数派复制，领导者推进 `commitIndex`，
  随后把已提交日志按顺序应用到状态机。
- 跟随者在 `AppendEntries` 中做标准的日志一致性检查：
  `prevLogIndex / prevLogTerm` 对不上则拒绝，领导者按“冲突任期首条索引”快速回退重发；
  同索引任期不同则截断本地分歧后缀；领导者日志更短时，跟随者删除本地多余的未提交后缀。
  由此保证**所有已提交日志在副本间一致**，历史分歧被检测并修复。
- 领导者只能直接提交“当前任期”的条目，更早任期的条目随当前任期条目一同提交，
  保证已提交条目不被覆盖（Raft Leader Completeness）。

### 2.3 幂等提交

每条提交必须携带客户端幂等键 `idempotencyKey`，判定原因通过 `CommitReason` 明确区分：

| 场景 | reason | 是否生效 | 说明 |
| --- | --- | --- | --- |
| 首次提交成功 | `APPLIED` | 是 | 已复制到多数派并应用 |
| 同键 + 相同内容重试 | `DUPLICATE_SUPPRESSED` | 否（只返回首次结果） | 绝不二次应用 |
| 同键 + **不同内容** | `IDEMPOTENCY_KEY_CONFLICT` | 否 | 与首次结果可明确区分 |
| 发到非领导者 | `NOT_LEADER` | 否 | 结果携带 `leaderId` 提示 |
| 多数派复制失败（如分区/宕机） | `REPLICATION_FAILED` | 否 | 记录未提交，不生效，可安全重试 |

- 状态机维护两张随快照持久化的幂等表：幂等键→首次指令内容（检测冲突）、
  幂等键→首次结果（重试原样返回）。副本重启后语义不变。
- 若一条指令此前已写入日志但尚未提交（复制未达多数派），相同重试会**复用同一条
  未决日志**重新驱动复制，而不会重复追加；不同内容仍按冲突拒绝。结论在重试与
  分区场景下保持一致。

### 2.4 快照、故障恢复与日志一致性

- 每个副本把数据持久化到本地目录：
  - `meta.json`：`currentTerm`、`votedFor`、`commitIndex`；
  - `log.json`：快照之后保留的日志（WAL）；
  - `snapshot.json`：最近一次状态机快照（含数据与两张幂等表）。
- 日志条目数超过 `consensus.snapshot-threshold` 时对已提交前缀做快照并压缩日志。
- `crash()` 模拟崩溃（内存态丢失、磁盘保留），`restart()` 时：
  1. 加载快照恢复状态机数据与幂等表；
  2. 重放快照之后**且 index ≤ commitIndex** 的日志；
  3. **未提交记录绝不应用**，因此不会生效；它们保留在日志中，重新连通后由
     合法领导者通过一致性检查截断或覆盖。
- 落后于领导者快照点的副本通过 `AppendRequest` 中携带的快照完成 InstallSnapshot 追赶。
- 任意时刻，各副本对外暴露的“已提交状态”都满足一致性约束：相同已提交键值一致，
  未提交内容不可见。

## 3. 可配置项（`application.properties`）

| 配置键 | 默认值 | 含义 |
| --- | --- | --- |
| `consensus.node-ids` | `n1,n2,n3` | 副本 ID 列表，长度决定多数派大小 |
| `consensus.data-dir` | `target/consensus-data` | 持久化根目录，每个副本一个子目录 |
| `consensus.snapshot-threshold` | `5` | 日志超过该长度触发快照压缩；`<=0` 关闭 |
| `consensus.election-ticks` | `1` | 选举 tick 步进参数（同步模型下保留） |
| `server.port` | `8080` | 本地 HTTP 端口 |

## 4. 本地验证方法

### 4.1 运行单元测试（推荐，结果确定）

```bash
mvn test
```

测试日志会打印 **选举投票**（`[投票]`/`[选举]`/`[当选]`）、**提交判定**
（`[提交判定]`/`[提交推进]`/`[应用]`）与**恢复过程**（`[快照]`/`[安装快照]`/`[恢复]`/
`[日志冲突]`/`[日志修复]`）。

- `ElectionTest`：初始唯一领导者 → 隔离旧领导者 → 多数派选出更高任期领导者 →
  旧领导者少数派提交失败 → 恢复网络后收敛唯一领导者，未提交记录不生效。
- `IdempotencyTest`：`APPLIED` / `DUPLICATE_SUPPRESSED` / `IDEMPOTENCY_KEY_CONFLICT` /
  `NOT_LEADER` / `REPLICATION_FAILED` 五类可区分结论，以及分区与重试下结论一致。
- `RecoveryTest`：快照 + 日志重启回放、未提交记录重启不生效、分歧日志被截断修复、
  停机副本经快照/日志追平。

### 4.2 启动应用，通过 HTTP 手工驱动

```bash
mvn spring-boot:run
```

示例（在另一个终端执行）：

```bash
# 查看集群状态
curl -s localhost:8080/api/consensus/status | jq

# 选举出领导者（可多执行几次直到出现 leader）
curl -s -X POST localhost:8080/api/consensus/elect | jq

# 提交带幂等键的指令（自动发往当前领导者）
curl -s -X POST localhost:8080/api/consensus/submit \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","command":"SET account A"}'

# 相同键相同内容重试 -> DUPLICATE_SUPPRESSED，值与首次相同
curl -s -X POST localhost:8080/api/consensus/submit \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","command":"SET account A"}'

# 相同键不同内容 -> IDEMPOTENCY_KEY_CONFLICT
curl -s -X POST localhost:8080/api/consensus/submit \
  -H 'Content-Type: application/json' \
  -d '{"idempotencyKey":"k1","command":"SET account B"}'

# 驱动心跳复制
curl -s -X POST localhost:8080/api/consensus/replicate | jq

# 制造分区 / 节点崩溃 / 重启 / 恢复
curl -s -X POST localhost:8080/api/consensus/partition \
  -H 'Content-Type: application/json' -d '{"isolate":["n1"]}' | jq
curl -s -X POST localhost:8080/api/consensus/nodes/n2/crash | jq
curl -s -X POST localhost:8080/api/consensus/nodes/n2/restart | jq
curl -s -X POST localhost:8080/api/consensus/heal | jq

# 清空全部持久化数据并回到初始空集群
curl -s -X POST localhost:8080/api/consensus/reset | jq
```

指令语法：`SET key value`（返回 `OK:value`）、`DELETE key`（返回 `DELETED:key`）。

## 5. 设计边界

本组件用于在本机直观、确定地验证副本一致性机制，刻意简化了生产 Raft 的部分内容：
网络与定时器由同步方法调用与显式 tick 代替；日志采用 JSON 文件顺序写入
（而非崩溃原子的批量 fsync）；成员变更、线性一致读租约、客户端会话等未实现。
选举安全、日志匹配、领导者完整性与状态机安全等核心一致性性质在上述模型内成立，
并有单元测试覆盖。
