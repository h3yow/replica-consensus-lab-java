# Replica Consensus Lab（进程内副本一致性协调组件）

一个**完全在本机独立验证**的副本一致性实验组件：用同一 JVM 进程内的多个副本节点模拟
一个 3/5 节点集群，完整实现一套简化的 Raft 协议，覆盖

- **领导者选举与任期推进**（多数派投票、同任期唯一领导、分区恢复后重新收敛）；
- **带幂等标识的指令提交**（重复提交返回首次结论且不二次应用、同键异内容明确拒绝）；
- **故障恢复与日志一致性**（快照 + 操作历史回放、未提交记录不生效、日志分歧检测与修复）；
- **落后副本追赶**（InstallSnapshot：副本离线期间历史被压缩后，重新加入仍能整体追平，
  追赶期间集群照常提交，过期快照不覆盖更新状态）。

不依赖任何外部中间件（无 Redis/Zookeeper/数据库），也不依赖真实网络（无 socket）：
节点间消息由一个**单线程、确定性的 tick 驱动器**在内存中按"先定时、后统一投递"的方式
派发，网络分区与节点宕机均为进程内模拟，结果可重复、可解释。

---

## 1. 目录结构

```
src/main/java/com/github/h3yow/replicaconsensuslabjava/consensus/
├── NodeRole.java            # FOLLOWER / CANDIDATE / LEADER
├── HardState.java           # currentTerm / votedFor / commitIndex（必须持久化）
├── LogEntry.java            # 日志条目（term,index,clientId,requestId,command）
├── Snapshot.java            # 快照（截至某 index 的 KV 状态 + 幂等表）
├── IdempotencyRecord.java   # 幂等结论（指令哈希 + 提交下标 + 应用结果）
├── RaftMessage.java         # RequestVote / AppendEntries / InstallSnapshot 及其响应
├── SubmitResult.java        # 提交结论（状态可区分 + reason 可解释）
├── KeyValueStateMachine.java# 确定性 KV 状态机（SET/DELETE/NOP）
├── NodeStorage.java         # 内存字节模拟磁盘（Java 序列化持久化）
├── RaftNode.java            # 单个副本：选举/复制/提交/快照/恢复
└── RaftCluster.java         # 集群：节点装配、分区、统一 tick、收敛等待
```

测试位于 `src/test/.../tests/`：

| 测试类 | 覆盖场景 |
| --- | --- |
| `ElectionTest` | 多数派当选、同任期唯一领导、少数派既不能当选也不能提交、分区恢复后收敛 |
| `IdempotencyTest` | 重复提交复用首次结论、同键异内容被拒绝且原因可区分、跟随者入口转发、分区下重试结论一致 |
| `RecoveryTest` | 崩溃后快照+历史回放、未提交记录重启不生效、历史分歧被检测并截断修复 |
| `CatchUpTest` | 落后副本跨压缩追平、追赶期间集群照常提交且未提交不生效、推送中途领导/跟随者重启仍收敛、过期快照不覆盖新状态、多轮压缩下反复掉队追平 |

---

## 2. 一致性协议说明

### 2.1 领导者选举与任期

1. 节点初始为跟随者，选举超时在 `[electionTimeoutMin, electionTimeoutMin+range)` 个 tick 内
   随机；超时后自增任期、转候选者、先投自己并向所有节点拉票。
2. 跟随者在**同一任期只投一次票**，且仅在候选者日志"不旧于"自己时投出
   （比较最后一条日志的 term、再比较 index）。
3. 候选者获得**多数派**（`votes*2 > N`）选票后成为领导者并立即广播心跳（AppendEntries）。
4. 任何节点在 RPC 中看到**更高任期**立即退回跟随者并更新任期。
   因此"同一任期两个领导者"不可能持续：分区恢复后，低任期旧领导收到高任期心跳即退位，
   集群重新收敛到唯一领导者（测试 `ElectionTest` 用 `leadersByTerm()` 断言每个任期恰有一个领导）。
5. 少数派分区凑不出多数派，既选不出新领导也无法提交（测试明确断言其提交只能得到
   `TIMEOUT` 或 `TERM_CHANGED`）。

### 2.2 日志复制与提交安全

- 领导把客户端指令追加到本地日志，通过 AppendEntries 复制；跟随者做
  **一致性检查**（`prevLogIndex/prevLogTerm`）：
  - 检查通过则补齐缺失、截断本地冲突后缀（分歧检测与修复，日志中有"截断"记录）；
  - 检查失败则返回冲突提示与自身日志末尾，领导据此回退 `nextIndex` 重试，直到追平。
- 领导只在某条**当前任期**的条目复制到多数派时推进 `commitIndex`（Raft 领导人完整性约束），
  并通过心跳把提交下标传播给跟随者；只有 `index <= commitIndex` 的条目才会应用到状态机。
- AppendEntries 成功响应携带跟随者**自报的日志末尾下标**，领导据此更新 `matchIndex/nextIndex`，
  不做乐观假设，从机制上避免"未复制却被计为已提交"。

### 2.3 幂等提交

每次客户端提交携带 `(clientId, requestId)` 幂等键，节点另存指令内容的 SHA-256 哈希。

- **已提交幂等表命中**且哈希相同 → 返回 `DUPLICATE`，附带与首次**完全相同**的
  `commitIndex` 与应用结果，**不再应用**（状态机结果字符串包含 `(new)/(updated)`，
  测试借此证明只应用了一次）。
- 幂等键命中但**哈希不同** → 返回 `IDEMPOTENCY_CONFLICT`，reason 中同时给出
  已存哈希与本次哈希，与 `DUPLICATE` 状态明确区分。
- **在途未提交日志**中出现相同键：同内容则复用该条复制（不重复追加），异内容同样拒绝。
- 全新请求：领导追加日志返回内部态 `PENDING`，由集群循环 tick 等待多数派；
  等待期间领导失任则返回 `TERM_CHANGED`，客户端可凭同一幂等键安全重试；
  分区凑不齐多数派返回 `TIMEOUT`。因为未提交不生效，所以"分区超时后恢复重试"
  得到的仍然是确定的唯一结论（先 COMMITTED，再 DUPLICATE）。

结论状态枚举：`COMMITTED / PENDING / DUPLICATE / IDEMPOTENCY_CONFLICT /
NOT_LEADER / TIMEOUT / TERM_CHANGED`，每个失败都带可解释的中文 `reason`。

### 2.4 故障恢复与快照

- `NodeStorage` 用内存字节数组模拟磁盘，分别持久化 `HardState`、`Snapshot`、压缩后日志。
  `crash()` 只销毁节点内存状态并停止 tick，`restart()` 用**同一份 storage** 重建节点：
  先加载快照重置状态机与幂等表，再顺序回放 `snapshotIndex < index <= commitIndex` 的日志；
  `commitIndex` 之后的未提交条目**保留在日志中但绝不应用**。
- 快照在"快照后保留日志条数 ≥ snapshotThreshold"时自动生成，状态机与幂等表一并入镜，
  之前的日志被压缩。`pendingOutcome` 以提交下标 + 在途登记表判定结论，
  因此压缩不影响正在等待的客户端。
- 重启后角色一律退回跟随者，重新参与选举；恢复后所有副本的已提交状态满足一致性约束。

### 2.5 落后副本追赶（InstallSnapshot）

当跟随者落后的起点已被领导压缩（`nextIndex <= 领导快照边界`）时，AppendEntries 无从
对齐，领导改为推送 **InstallSnapshot**：把截至快照边界的已提交 KV 状态与幂等表整体发给
落后副本，让其一步跨过已压缩的历史，再用 AppendEntries 补齐快照之后的条目。

- **触发路径**：跟随者拒绝 AppendEntries 时会自报日志末尾/冲突起点，领导的
  `nextIndex` 据此回退；一旦退到快照边界以内即改发 InstallSnapshot。整条路径只依赖
  常规心跳与拒绝重试，不需要额外的外部干预。
- **跟随者安装快照**：先持久化快照再截断日志（崩溃于两步之间时，重启以快照为准丢弃
  冗余旧日志，不丢已提交状态）；若本地在快照边界处有同任期条目，边界之后的未提交
  后缀予以保留，否则整段丢弃。安装后 `commitIndex/lastApplied` 直接推进到快照边界
  ——快照内全是已提交内容，不存在"未提交先生效"，也不会重复应用（`lastApplied`
  单调向前，幂等表随快照整体替换）。
- **过期/重复快照不覆盖新状态**：若快照的 `lastIncludedIndex <= 本地 commitIndex`，
  说明本地已提交状态更新，跟随者忽略快照内容、只回成功确认（领导据此把
  `nextIndex` 跳到跟随者自报的日志末尾之后），已提交的键值与日志绝不回退。
- **被打断也能收敛**：领导在推送中途崩溃，新领导会从自己的快照边界重新走
  "拒绝-回退-推快照"流程；落后副本自己中途崩溃，重启后按"快照 + 快照后日志"恢复，
  已安装的快照不会丢，未安装完的会由领导重推。
- **不拖住集群**：快照推送与后续补齐都复用心跳驱动的复制通道，领导与多数派照常
  接收并提交新指令；落后副本追平前只应用 `<= commitIndex` 的内容。
- **与快照/压缩机制的兼容范围**：InstallSnapshot 复用本地压缩生成的同一份
  `Snapshot`（KV + 幂等表），因此幂等结论跨压缩、跨追赶保持一致——追赶完成后用
  相同 `(clientId, requestId)` 重试，任何副本上的首次结论都与压缩前相同
  （`CatchUpTest` 用 SET 结果的 `(new)/(updated)` 差异证明没有二次应用）。
  本地压缩的触发条件（`snapshotThreshold`）与快照格式均未改变。

---

## 3. 可配置项

集群构造参数 `RaftCluster.Config`（也可直接用 `Config.defaults()`）：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `electionTimeoutMin` | 8 | 选举超时下界（tick 数） |
| `electionTimeoutRange` | 8 | 选举超时随机抖动宽度，实际超时 ∈ [8,16) ticks |
| `heartbeatInterval` | 3 | 领导者心跳间隔（tick 数） |
| `snapshotThreshold` | 6 | 快照后保留日志条数达到该值即触发快照压缩 |

tick 是逻辑时间单位而非真实时间，因此调大超时预算只影响循环次数，不影响墙钟。
指令语法：`SET key value`（value 可含空格）、`DELETE key`、`NOP`。

---

## 4. 本地验证方法

前置：JDK 21、Maven 3.6+（无需启动任何外部服务）。

```bash
# 运行全部测试（选举/幂等/恢复/追赶），控制台会打印投票、提交判定、恢复与追赶过程
mvn test

# 只跑某一类场景
mvn -Dtest=ElectionTest test       # 分区下的领导者选举与收敛
mvn -Dtest=IdempotencyTest test    # 重复提交 / 冲突拒绝 / 分区重试
mvn -Dtest=RecoveryTest test       # 快照回放 / 未提交不生效 / 分歧修复
mvn -Dtest=CatchUpTest test        # 落后副本跨压缩追赶 / 推送被打断 / 过期快照
```

日志关键词：

- `[选举]` 发起选举 / 获得多数派成为领导者；
- `[投票]` 投票授权或拒绝（含拒绝原因：已投他人 / 对方日志过旧）；
- `[复制]` 一致性检查失败、截断分歧后缀、回退重试；
- `[提交]` 追加、多数派提交、应用结果、通知客户端；
- `[幂等]` 命中重复（返回首次结论）或拒绝内容冲突（含双方哈希）；
- `[快照]` 生成快照与日志压缩；`[恢复]` 崩溃后按快照+历史回放；
- `[追赶]` 落后副本触发 InstallSnapshot、安装快照推进、领导确认后继续补齐；
- `[网络]` 制造分区 / 分区恢复；`[故障]` 节点崩溃。

也可在任意 main 中手工编排：

```java
RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3"));
String leader = cluster.runUntilLeaderElected(60);
SubmitResult r1 = cluster.submit("client-1", "req-1", "SET color blue", 30); // COMMITTED
SubmitResult r2 = cluster.submit("client-1", "req-1", "SET color blue", 30); // DUPLICATE
SubmitResult r3 = cluster.submit("client-1", "req-1", "SET color red",  30); // IDEMPOTENCY_CONFLICT
```

---

## 5. 边界与简化说明

- 消息"同 tick 产生、下一轮统一投递"，分区期间消息直接丢弃，模拟不可达而非挂起。
- 选举超时使用确定性随机数（单线程），极端情况下仍可能平票并进入下一任期重试；
  测试通过足够的 tick 预算（多个超时间隔）验证**最终必然收敛**，而非某一固定 tick 数。
- 本实验聚焦选举、幂等提交、恢复与落后追赶四条主线，未实现成员变更、
  分块（chunked）传输大快照等工程优化；快照当前随 InstallSnapshot 一次性整体投递，
  对本机验证的规模足够，这些可在现有结构上扩展。
