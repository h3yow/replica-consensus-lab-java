# Replica Consensus Lab（进程内副本一致性协调组件）

一个**完全在本机独立验证**的副本一致性实验组件：用同一 JVM 进程内的多个副本节点模拟
一个 3/5 节点集群，完整实现一套简化的 Raft 协议，覆盖

- **领导者选举与任期推进**（多数派投票、同任期唯一领导、分区恢复后重新收敛）；
- **带幂等标识的指令提交**（重复提交返回首次结论且不二次应用、同键异内容明确拒绝）；
- **故障恢复与日志一致性**（快照 + 操作历史回放、未提交记录不生效、日志分歧检测与修复）；
- **落后副本追赶**（离线再久、缺失历史已被多代压缩，也能经 InstallSnapshot 追上并对齐；
  追赶途中领导持续提交、可被打断后续传、已更新副本不被旧快照覆盖）。

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
├── RaftNode.java            # 单个副本：选举/复制/提交/快照/追赶/恢复
└── RaftCluster.java         # 集群：节点装配、分区、统一 tick、收敛等待
```

测试位于 `src/test/.../tests/`：

| 测试类 | 覆盖场景 |
| --- | --- |
| `ElectionTest` | 多数派当选、同任期唯一领导、少数派既不能当选也不能提交、分区恢复后收敛 |
| `IdempotencyTest` | 重复提交复用首次结论、同键异内容被拒绝且原因可区分、跟随者入口转发、分区下重试结论一致 |
| `RecoveryTest` | 崩溃后快照+历史回放、未提交记录重启不生效、历史分歧被检测并截断修复 |
| `CatchUpTest` | 跨多代压缩的落后副本追赶、追赶中领导持续提交、追赶后幂等结论不变且不重复应用、领导/落后者推送途中重启续传、旧快照不覆盖更新副本、反复掉队跨多代压缩仍一致 |

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

当一个副本离线很久，期间集群持续提交并做过多次快照压缩后，它缺失的那段历史可能
**已经不在任何节点的日志里**（只剩领导的最新快照）。普通 AppendEntries 找不到
`prevLogIndex/prevLogTerm` 对齐点，无法补齐。为此新增一条 InstallSnapshot 通道：

1. **何时走快照通道**：领导为每个跟随者维护 `nextIndex`；心跳/复制时若
   `nextIndex <= 自己的快照边界`，说明该跟随者缺的历史已被本节点压缩，
   不再发 AppendEntries，而发一条 InstallSnapshot，携带快照边界 `index/term`、
   完整 KV 状态与幂等表（进程内不分片，整条一次送达），以及 `leaderCommit`。
2. **跟随者如何安装**：
   - 先做**新旧判断**：快照边界 `<= 本地 commitIndex` 一律拒绝——这说明本地已提交内容
     不比它旧（典型如旧领导迟到/乱序的推送、或追赶途中集群又压缩过），**绝不回退状态**，
     从机制上杜绝"已更新副本被旧内容覆盖"；
   - 否则用快照整体替换已提交视图（状态机 `resetFrom`、幂等表替换、快照与日志落盘），
     按标准 Raft 规则保留边界之后 term 不冲突且严格连续的尾部日志，丢弃其余分歧后缀；
     `commitIndex` 至少推进到快照边界，但**不越过本地实际日志末尾**——边界之后尚未收到
     的条目不被提前标记为已提交，更不会被应用，因此"未提交内容在追赶完成前不生效"；
   - 边界之后、新 `commitIndex` 之前已持有的日志顺序回放一次（快照内容已整体生效，
     不重复应用），幂等表随快照同步，故追赶区间内的历史请求重试仍返回首次结论。
3. **可重复、可打断、可续传**：快照安装是幂等的，领导每轮心跳都可重发同一边界的快照，
   因此推送途中响应丢失、领导重启、跟随者自己重启都不会"卡死"；重启双方从磁盘上
   已落盘的快照/日志继续，下一轮心跳重新决策（继续推快照或转增量复制），最终收敛。
4. **追赶不拖集群**：追赶只影响落后者与领导之间的复制，领导提交仍只要求**多数派**，
   其余健康副本够多数派时，新指令照常追加、复制、提交，不等待落后者。
5. **与现有快照/压缩完全兼容**：InstallSnapshot 复用既有 `Snapshot`/`NodeStorage`/
   幂等表机制，不改变快照自动生成条件，也不引入任何外部依赖；节点重启回放时额外做了
   一层"只接受快照边界之后日志"的防御，保证压缩边界与日志位置始终一致。

日志中以 `[追赶]` 前缀完整呈现：领导发现落后、改推快照、跟随者安装与提交推进、
确认后转增量复制、拒绝旧快照等每一步，可逐条对照落后副本的追赶路径。

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
# 运行全部测试（选举/幂等/恢复/追赶，共 16 个用例），控制台会打印投票、提交判定、恢复与追赶过程
mvn test

# 只跑某一类场景
mvn -Dtest=ElectionTest test       # 分区下的领导者选举与收敛
mvn -Dtest=IdempotencyTest test    # 重复提交 / 冲突拒绝 / 分区重试
mvn -Dtest=RecoveryTest test       # 快照回放 / 未提交不生效 / 分歧修复
mvn -Dtest=CatchUpTest test        # 跨压缩追赶 / 打断续传 / 防旧覆盖 / 多代压缩反复掉队
```

日志关键词：

- `[选举]` 发起选举 / 获得多数派成为领导者；
- `[投票]` 投票授权或拒绝（含拒绝原因：已投他人 / 对方日志过旧）；
- `[复制]` 一致性检查失败、截断分歧后缀、回退重试；
- `[提交]` 追加、多数派提交、应用结果、通知客户端；
- `[幂等]` 命中重复（返回首次结论）或拒绝内容冲突（含双方哈希）；
- `[快照]` 生成快照与日志压缩；`[恢复]` 崩溃后按快照+历史回放；
- `[追赶]` 领导发现落后改推快照、跟随者安装快照并推进提交、确认后转增量、拒绝旧快照；
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
- 本次已补齐落后副本追赶（InstallSnapshot，见 2.5）：进程内整条快照一次送达，
  **未做快照分片/偏移/流量控制**，因为消息不经过真实网络、快照大小可控；
  生产级实现需要 offset/done 分片、背压与快照传输期间的并发快照版本管理，
  这些都能在现有消息与存储结构上继续扩展。
- 仍未实现成员变更、日志压缩后的旧快照 GC 与分块快照传输；这些不影响本实验在
  3/5 节点、多代压缩下对"离线再久也能追平"的验证目标。
