package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 进程内多副本"集群"：持有节点、模拟网络分区，并以确定性的单线程 tick 推进
 * 选举与日志复制。不使用真实线程、socket 或任何外部中间件。
 *
 * <p>每个 tick：先让各节点处理定时逻辑（发起选举/心跳），再统一投递本 tick
 * 产出的消息（同 tick 不立即可见，避免依赖消息顺序的不确定行为）。</p>
 */
public class RaftCluster {

    /** 集群可调参数。 */
    public record Config(int electionTimeoutMin, int electionTimeoutRange,
                         int heartbeatInterval, long snapshotThreshold) {
        public static Config defaults() {
            return new Config(8, 8, 3, 6);
        }
    }

    private final Config config;
    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, NodeStorage> storages = new LinkedHashMap<>();
    private final Set<String> partitioned = new HashSet<>();
    /** 测试用：发往这些节点的消息直接丢弃（比双向分区更轻量，只"黑"一个接收方）。 */
    private final Set<String> dropIncomingTo = new HashSet<>();

    private long tickCount = 0;

    private record SourcedEnvelope(String from, String to, Object message) {
    }

    public RaftCluster(List<String> nodeIds) {
        this(nodeIds, Config.defaults());
    }

    public RaftCluster(List<String> nodeIds, Config config) {
        this.config = config;
        for (String id : nodeIds) {
            NodeStorage storage = new NodeStorage(id);
            storages.put(id, storage);
            RaftNode node = new RaftNode(id, storage,
                    config.electionTimeoutMin(), config.electionTimeoutRange(),
                    config.heartbeatInterval(), config.snapshotThreshold());
            nodes.put(id, node);
        }
        for (RaftNode node : nodes.values()) {
            node.attachCluster(this);
        }
        System.out.printf("[集群] 初始化完成：节点=%s，超时区间=[%d,%d) ticks，心跳=%d ticks，快拍阈值=%d%n",
                nodeIds, config.electionTimeoutMin(),
                config.electionTimeoutMin() + config.electionTimeoutRange(),
                config.heartbeatInterval(), config.snapshotThreshold());
    }

    // ---------- 拓扑与网络 ----------

    public List<String> nodeIds() {
        return List.copyOf(nodes.keySet());
    }

    List<String> peerIds(String self) {
        List<String> result = new ArrayList<>();
        for (String id : nodes.keySet()) {
            if (!id.equals(self)) {
                result.add(id);
            }
        }
        return result;
    }

    boolean isMajority(int count) {
        return count * 2 > nodes.size();
    }

    public int size() {
        return nodes.size();
    }

    /** 在两组节点之间制造双向分区（组内互通，组间不通）。 */
    public void partition(List<String> groupA, List<String> groupB) {
        partitioned.clear();
        for (String a : groupA) {
            for (String b : groupB) {
                partitioned.add(linkKey(a, b));
            }
        }
        System.out.printf("[网络] 制造分区：%s <X> %s%n", groupA, groupB);
    }

    /** 恢复全部网络连接。 */
    public void heal() {
        if (!partitioned.isEmpty()) {
            System.out.printf("[网络] 分区恢复，全部 %d 个节点恢复互通%n", nodes.size());
        }
        partitioned.clear();
    }

    private static String linkKey(String a, String b) {
        return a.compareTo(b) < 0 ? a + "|" + b : b + "|" + a;
    }

    private boolean canDeliver(String from, String to) {
        return !partitioned.contains(linkKey(from, to));
    }

    /** 测试用：丢弃所有发往该节点的入站消息（节点仍在线、仍在 tick，但不主动发起选举）。 */
    public void blockIncoming(String id) {
        dropIncomingTo.add(id);
        nodes.get(id).setElectionTimerPaused(true);
        System.out.printf("[网络] 丢弃发往 %s 的全部入站消息（测试钩子）%n", id);
    }

    /** 测试用：恢复对该节点的正常投递与选举计时。 */
    public void unblockIncoming(String id) {
        dropIncomingTo.remove(id);
        RaftNode node = nodes.get(id);
        if (node.isUp()) {
            node.setElectionTimerPaused(false);
        }
        System.out.printf("[网络] 恢复发往 %s 的入站消息（测试钩子）%n", id);
    }

    // ---------- 故障与重启 ----------

    /** 宕机：节点停止一切活动；"磁盘"（storage）保留。 */
    public void crash(String id) {
        nodes.get(id).crash();
    }

    /** 用同一份持久化数据重启节点。 */
    public void restart(String id) {
        nodes.get(id).restart(storages.get(id));
    }

    /**
     * 测试用：模拟 {@code fromLeader} 的一份 InstallSnapshot 直接投递到 {@code to}
     * （进程内绕过正常网络），用于确定性构造"旧领导迟到快照"等打断/乱序时序。
     * 消息在下一次 {@link #tick()} 随收件箱一起被处理。
     */
    public void injectInstallSnapshot(String fromLeader, String to, Snapshot snapshot,
                                     long leaderCommit) {
        RaftNode leader = nodes.get(fromLeader);
        nodes.get(to).injectMessage(new RaftMessage.InstallSnapshot(
                leader.getCurrentTerm(), fromLeader,
                snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                snapshot.getKv(), snapshot.getIdempotency(), leaderCommit), fromLeader);
    }

    // ---------- 节点访问 ----------

    public RaftNode getNode(String id) {
        return nodes.get(id);
    }

    public Map<String, RaftNode> allNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    public long getTickCount() {
        return tickCount;
    }

    // ---------- 时间推进与消息投递 ----------

    public void tick() {
        tickCount++;
        List<SourcedEnvelope> batch = new ArrayList<>();

        // 阶段一：定时逻辑 + 生成消息
        for (RaftNode node : nodes.values()) {
            if (node.isUp()) {
                node.tick();
                while (node.hasOutgoing()) {
                    RaftNode.Envelope envelope = node.pollOutgoing();
                    batch.add(new SourcedEnvelope(node.getId(), envelope.to(), envelope.message()));
                }
            }
        }

        // 阶段二：统一投递，受分区、宕机与测试用入站丢弃约束
        for (SourcedEnvelope envelope : batch) {
            RaftNode target = nodes.get(envelope.to());
            if (target == null || !target.isUp()) {
                continue;
            }
            if (dropIncomingTo.contains(envelope.to())) {
                continue;
            }
            if (!canDeliver(envelope.from(), envelope.to())) {
                continue; // 分区期间消息被丢弃
            }
            target.deliver(envelope.message(), envelope.from());
        }
    }

    /** 收集当前所有自认领导者的节点（分区期不同任期下可能短暂并存）。 */
    private List<String> selfProclaimedLeaders() {
        List<String> result = new ArrayList<>();
        for (RaftNode node : nodes.values()) {
            if (node.isUp() && node.getRole() == NodeRole.LEADER) {
                result.add(node.getId());
            }
        }
        return result;
    }

    /**
     * 推进直到集群收敛到唯一领导者（分区恢复时需要若干心跳 tick 让旧领导退位）。
     * 返回唯一领导者编号；超时返回 null。
     */
    public String runUntilLeaderElected(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            List<String> leaders = selfProclaimedLeaders();
            if (leaders.size() == 1) {
                return leaders.get(0);
            }
            tick();
        }
        List<String> leaders = selfProclaimedLeaders();
        return leaders.size() == 1 ? leaders.get(0) : null;
    }

    /** 当前唯一在任领导者；存在多个或没有时返回 null。 */
    public String getLeaderId() {
        List<String> leaders = selfProclaimedLeaders();
        return leaders.size() == 1 ? leaders.get(0) : null;
    }

    /** 各任期的领导者集合，用于断言同一任期不会出现两个领导者。 */
    public Map<Long, Set<String>> leadersByTerm() {
        Map<Long, Set<String>> result = new LinkedHashMap<>();
        for (RaftNode node : nodes.values()) {
            if (node.isUp() && node.getRole() == NodeRole.LEADER) {
                result.computeIfAbsent(node.getCurrentTerm(), k -> new HashSet<>()).add(node.getId());
            }
        }
        return result;
    }

    /** 推进直到所有在线节点的提交下标都达到 leader 的提交下标（日志收敛）。 */
    public boolean waitForConvergence(int maxTicks) {
        return ticksToConvergence(maxTicks) >= 0;
    }

    /**
     * 与 {@link #waitForConvergence(int)} 相同的收敛判定，但返回实际耗费的 tick 数；
     * 超时返回 -1。用于显式验证"落后副本在合理次数推进内追平"。
     */
    public long ticksToConvergence(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            String leaderId = getLeaderId();
            if (leaderId != null) {
                long leaderCommit = nodes.get(leaderId).getCommitIndex();
                boolean allCaughtUp = nodes.values().stream()
                        .filter(RaftNode::isUp)
                        .allMatch(n -> n.getCommitIndex() >= leaderCommit);
                if (allCaughtUp) {
                    return i;
                }
            }
            tick();
        }
        return -1;
    }

    /** 推进直到所有在线节点对某个 key 的已提交状态一致。 */
    public boolean waitForValue(String key, String expected, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            boolean ok = nodes.values().stream().filter(RaftNode::isUp)
                    .allMatch(n -> expected == null ? n.read(key) == null
                            : expected.equals(n.read(key)));
            if (ok) {
                return true;
            }
            tick();
        }
        return false;
    }

    // ---------- 客户端提交 ----------

    /** 自动路由到当前领导者的便捷提交；尚无领导者时先在超时预算内推进选举。 */
    public SubmitResult submit(String clientId, String requestId, String command, int maxTicks) {
        String target = getLeaderId();
        if (target == null) {
            target = runUntilLeaderElected(maxTicks);
            if (target == null) {
                return new SubmitResult(SubmitResult.Status.NOT_LEADER, null, -1, null,
                        "集群当前没有可用领导者（" + maxTicks + " ticks 内未选出）");
            }
        }
        return submitVia(nodes.get(target), clientId, requestId, command, maxTicks);
    }

    /**
     * 通过指定入口节点提交：非领导者入口按 leaderHint 路由；领导者入口追加日志并
     * 由 tick 驱动多数派复制。结论（COMMITTED/DUPLICATE/IDEMPOTENCY_CONFLICT/
     * TIMEOUT/TERM_CHANGED）在重试与分区下保持一致。
     */
    public SubmitResult submitVia(RaftNode entry, String clientId, String requestId,
                                  String command, int maxTicks) {
        SubmitResult accepted = entry.submit(clientId, requestId, command);

        if (accepted.status() == SubmitResult.Status.NOT_LEADER) {
            String hint = accepted.leaderId();
            if (hint != null && nodes.containsKey(hint) && !hint.equals(entry.getId())) {
                System.out.printf("[客户端] 入口 %s 非领导，按提示转发到 %s%n", entry.getId(), hint);
                return submitVia(nodes.get(hint), clientId, requestId, command, maxTicks);
            }
            String elected = runUntilLeaderElected(maxTicks);
            if (elected == null) {
                return new SubmitResult(SubmitResult.Status.TIMEOUT, null, -1, null,
                        "非领导入口且在 " + maxTicks + " ticks 内未选出领导者（可能处于少数派分区）");
            }
            return submitVia(nodes.get(elected), clientId, requestId, command, maxTicks);
        }

        if (accepted.status() != SubmitResult.Status.PENDING) {
            return accepted; // DUPLICATE / IDEMPOTENCY_CONFLICT / TIMEOUT(宕机)
        }

        long index = accepted.commitIndex();
        long term = entry.getCurrentTerm();
        for (int i = 0; i < maxTicks; i++) {
            tick();
            SubmitResult outcome = entry.pendingOutcome(index, term);
            if (outcome != null) {
                return outcome;
            }
            if (entry.getRole() != NodeRole.LEADER || entry.getCurrentTerm() != term) {
                return new SubmitResult(SubmitResult.Status.TERM_CHANGED,
                        getLeaderId(), -1, null,
                        "原领导在提交确认前失去领导地位（term=" + term + "），可凭相同幂等标识安全重试");
            }
        }
        return new SubmitResult(SubmitResult.Status.TIMEOUT, entry.getId(), index, null,
                "在 " + maxTicks + " ticks 内未达成多数派（疑似分区）");
    }
}
