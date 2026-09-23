package com.github.h3yow.replicaconsensuslabjava.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 进程内模拟集群：持有全部 {@link RaftNode}，模拟网络分区，并充当节点间 RPC 传输层。
 *
 * <p>设计为<b>同步、确定性</b>：不启动真实网络与定时器，测试通过显式调用
 * {@link #tickElection()} / {@link #tickReplication()} 推进协议，从而得到可复现、
 * 可解释的选举、提交与恢复结果。分区用“双向断链集合”表示，{@code heal()} 后全部恢复。
 */
public class Cluster {

    private static final Logger log = LoggerFactory.getLogger(Cluster.class);

    private final Map<String, RaftNode> nodes = new LinkedHashMap<>();
    private final Map<String, FileNodeStore> stores = new LinkedHashMap<>();
    private final List<String> nodeIds;
    private final String dataDir;
    private final ConsensusProperties properties;
    private final int quorum;

    /** 断链集合：包含无序端点对 "a|b"，出现即表示双向不可达。 */
    private final List<String> blockedLinks = new ArrayList<>();

    public Cluster(List<String> nodeIds, String dataDir, ConsensusProperties properties) {
        this.nodeIds = List.copyOf(nodeIds);
        this.dataDir = dataDir;
        this.properties = properties;
        for (String id : this.nodeIds) {
            FileNodeStore store = new FileNodeStore(dataDir, id);
            stores.put(id, store);
            nodes.put(id, new RaftNode(id, store, properties));
        }
        for (RaftNode node : nodes.values()) {
            node.attach(this);
        }
        this.quorum = this.nodeIds.size() / 2 + 1;
        log.info("[集群] 初始化进程内集群 nodes={} 多数派={}", this.nodeIds, quorum);
    }

    public List<RaftNode> nodes() {
        return List.copyOf(nodes.values());
    }

    public RaftNode node(String id) {
        return nodes.get(id);
    }

    public int quorum() {
        return quorum;
    }

    /** 当前存活且自认为领导者的节点（正常情况下至多一个）。 */
    public RaftNode leader() {
        RaftNode found = null;
        for (RaftNode n : nodes.values()) {
            if (n.alive() && n.role() == NodeRole.LEADER) {
                if (found != null) {
                    log.warn("[集群] 检测到同视图下两个存活领导者: {} 与 {}（任期检查将拒绝其中之一）",
                            found.id(), n.id());
                }
                found = n;
            }
        }
        return found;
    }

    /**
     * 一轮选举 tick：
     * 跟随者若能连通已认可的存活（更高/同任期）领导者则保持安静；
     * 候选人，以及可能“过期”的领导者（例如刚从分区恢复）都会发起一轮拉票，
     * 以便立刻发现更高任期并退回跟随者，使集群快速收敛到唯一领导者。
     */
    public void tickElection() {
        for (RaftNode n : nodes.values()) {
            if (!n.alive()) {
                continue;
            }
            // 实时查询领导者（本轮 tick 可能刚刚选出新领导者）。
            RaftNode currentLeader = leader();
            // 跟随者：若能连通任期不低于自己的存活领导者，则保持安静。
            if (n.role() == NodeRole.FOLLOWER
                    && currentLeader != null
                    && !currentLeader.id().equals(n.id())
                    && currentLeader.currentTerm() >= n.currentTerm()
                    && canDeliver(n.id(), currentLeader.id())) {
                continue;
            }
            // 其余情况（无领导者、领导者不可达，或节点本身是过期的候选/领导者）发起拉票。
            n.startElection();
        }
    }

    /** 一轮复制 tick：存活领导者发送心跳并推进复制/提交。 */
    public void tickReplication() {
        RaftNode currentLeader = leader();
        if (currentLeader != null) {
            currentLeader.broadcastAppend();
        }
    }

    /** 将给定节点与其余所有节点隔离（模拟其落入少数派分区）。 */
    public void partition(String... isolatedIds) {
        List<String> isolated = Arrays.asList(isolatedIds);
        for (String a : isolated) {
            for (String b : nodeIds) {
                if (!a.equals(b)) {
                    block(a, b);
                }
            }
        }
        log.info("[分区] 隔离节点 {}，断链={}", isolated, blockedLinks);
    }

    /** 在两组之间制造双向分区。 */
    public void partitionBetween(List<String> sideA, List<String> sideB) {
        for (String a : sideA) {
            for (String b : sideB) {
                block(a, b);
            }
        }
        log.info("[分区] 制造组间分区 {} <-> {}，断链={}", sideA, sideB, blockedLinks);
    }

    /** 修复全部网络分区。 */
    public void heal() {
        blockedLinks.clear();
        log.info("[分区] 网络已完全恢复");
    }

    private void block(String a, String b) {
        String link = a.compareTo(b) <= 0 ? a + "|" + b : b + "|" + a;
        if (!blockedLinks.contains(link)) {
            blockedLinks.add(link);
        }
    }

    /** 两个节点当前是否可互相通信（受分区与宕机影响）。 */
    public boolean canDeliver(String from, String to) {
        if (from.equals(to)) {
            return true;
        }
        RaftNode a = nodes.get(from);
        RaftNode b = nodes.get(to);
        if (a == null || b == null || !a.alive() || !b.alive()) {
            return false;
        }
        String link = from.compareTo(to) <= 0 ? from + "|" + to : to + "|" + from;
        return !blockedLinks.contains(link);
    }

    /** 投票 RPC 投递：不可达返回 null（调用方按“丢票”处理）。 */
    public RaftMessages.VoteResponse deliverVote(String from, String to, RaftMessages.VoteRequest request) {
        if (!canDeliver(from, to)) {
            return null;
        }
        RaftNode target = nodes.get(to);
        return target == null ? null : target.handleRequestVote(request);
    }

    /** 追加条目 RPC 投递：不可达返回 null（调用方按“复制失败/心跳丢失”处理）。 */
    public RaftMessages.AppendResponse deliverAppend(String from, RaftMessages.AppendRequest request, String to) {
        if (!canDeliver(from, to)) {
            return null;
        }
        RaftNode target = nodes.get(to);
        return target == null ? null : target.handleAppend(request);
    }

    /**
     * 驱动最多 maxTicks 轮（先选举、后复制），直到出现存活领导者。
     *
     * @return 领导者；超时仍未选出返回 null
     */
    public RaftNode awaitLeader(int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            RaftNode leader = leader();
            if (leader != null) {
                return leader;
            }
            tickElection();
            tickReplication();
        }
        return leader();
    }

    /** 驱动若干轮复制，使领导者日志尽量传播到所有可达副本。 */
    public void settle(int rounds) {
        for (int i = 0; i < rounds; i++) {
            tickReplication();
        }
    }

    /** 重置全部副本持久化数据并重启为空集群视图（测试隔离用）。 */
    public void resetAllStores() {
        blockedLinks.clear();
        for (FileNodeStore store : stores.values()) {
            store.reset();
        }
        // 重新构造内存节点状态（模拟全部副本全新启动）。
        nodes.clear();
        for (String id : nodeIds) {
            nodes.put(id, new RaftNode(id, stores.get(id), properties));
        }
        for (RaftNode node : nodes.values()) {
            node.attach(this);
        }
    }

    /** 集群整体状态快照（供 HTTP/测试观察）。 */
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeIds", nodeIds);
        m.put("quorum", quorum);
        m.put("blockedLinks", List.copyOf(blockedLinks));
        RaftNode leader = leader();
        m.put("leader", leader == null ? null : leader.id());
        List<Map<String, Object>> nodeStates = new ArrayList<>();
        for (RaftNode n : nodes.values()) {
            nodeStates.add(n.describe());
        }
        m.put("nodes", nodeStates);
        return m;
    }
}
