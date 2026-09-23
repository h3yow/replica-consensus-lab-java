package com.github.h3yow.replicaconsensuslabjava.consensus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单个 Raft 副本。
 *
 * <p>采用“进程内 + 同步 + 手动 tick 驱动”的确定性模型，不使用真实网络与定时器：
 * <ul>
 *     <li>{@link #startElection()}：候选人任期加一、给自己投票，并向所有可达节点拉票，
 *         仅当获得多数派（含自己）支持时才成为领导者；</li>
 *     <li>{@link #broadcastAppend()}：领导者向所有可达跟随者复制日志/心跳，并依据
 *         多数派 matchIndex 推进 commitIndex；</li>
 *     <li>{@link #submit(String, String)}：仅领导者可处理，基于状态机幂等表去重，
 *         同键不同内容返回 {@link CommitReason#IDEMPOTENCY_KEY_CONFLICT}，
 *         重复提交返回 {@link CommitReason#DUPLICATE_SUPPRESSED}；</li>
 *     <li>{@link #restart()}：从快照 + 操作历史恢复，只重放已提交记录。</li>
 * </ul>
 * 同一任期的第二个领导者会因其 AppendEntries 携带的过期任期被拒绝（term 检查），
 * 历史分歧则按 Raft 日志一致性检查在 AppendEntries 中截断修复。
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final NodeStore store;
    private final ConsensusProperties properties;
    private final KeyValueStateMachine stateMachine = new KeyValueStateMachine();
    private final List<LogEntry> raftLog = new ArrayList<>();
    private final Map<String, Long> matchIndex = new HashMap<>();

    private Cluster cluster;

    private long currentTerm;
    private String votedFor;
    private long commitIndex;
    private long lastSnapshottedIndex;
    private NodeRole role = NodeRole.FOLLOWER;
    private String leaderHint;
    private boolean alive = true;

    public RaftNode(String nodeId, NodeStore store, ConsensusProperties properties) {
        this.nodeId = nodeId;
        this.store = store;
        this.properties = properties;
        recoverFromDisk(true);
    }

    // ============================== 基本访问器 ==============================

    public String id() {
        return nodeId;
    }

    public NodeRole role() {
        return role;
    }

    public long currentTerm() {
        return currentTerm;
    }

    public long commitIndex() {
        return commitIndex;
    }

    public boolean alive() {
        return alive;
    }

    public String leaderHint() {
        return leaderHint;
    }

    /** 日志中最后一条的全局索引（快照之前的也算）。 */
    public long lastLogIndex() {
        long base = lastSnapshottedIndex;
        return base + raftLog.size();
    }

    public long lastLogTerm() {
        if (raftLog.isEmpty()) {
            return lastSnapshottedIndex == 0 ? 0 : termAt(lastSnapshottedIndex);
        }
        return raftLog.get(raftLog.size() - 1).term();
    }

    void attach(Cluster cluster) {
        this.cluster = cluster;
    }

    // ============================== RequestVote ==============================

    public synchronized RaftMessages.VoteResponse handleRequestVote(RaftMessages.VoteRequest request) {
        if (!alive) {
            return null;
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        boolean grant = false;
        if (request.term() == currentTerm) {
            boolean upToDate = request.lastLogTerm() > lastLogTerm()
                    || (request.lastLogTerm() == lastLogTerm() && request.lastLogIndex() >= lastLogIndex());
            boolean canVote = votedFor == null || votedFor.equals(request.candidateId());
            if (upToDate && canVote) {
                grant = true;
                votedFor = request.candidateId();
                persistMeta();
            }
        }
        log.info("[投票] node={} 收到 {} 的拉票 term={} 候选日志=(idx={},term={}) 本地日志=(idx={},term={}) -> {}",
                nodeId, request.candidateId(), request.term(),
                request.lastLogIndex(), request.lastLogTerm(), lastLogIndex(), lastLogTerm(),
                grant ? "同意" : "拒绝(已投他人/日志落后)");
        return new RaftMessages.VoteResponse(nodeId, currentTerm, grant);
    }

    // ============================== AppendEntries ==============================

    public synchronized RaftMessages.AppendResponse handleAppend(RaftMessages.AppendRequest request) {
        if (!alive) {
            return null;
        }
        // 同任期第二个领导者（旧领导者）会在这里因 term 更小被拒绝。
        if (request.term() < currentTerm) {
            log.info("[拒绝追加] node={} 拒绝 {} 的 AppendEntries：对方 term={} < 本地 term={}",
                    nodeId, request.leaderId(), request.term(), currentTerm);
            return new RaftMessages.AppendResponse(nodeId, currentTerm, false, lastLogIndex());
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        // 选举安全：本任期已认可某领导者时，同任期来自其他身份的“领导者”心跳必须拒绝，
        // 从而杜绝同一任期出现两个被接受的领导者（脑裂）。
        if (request.term() == currentTerm && leaderHint != null
                && !leaderHint.equals(request.leaderId())) {
            log.info("[拒绝追加] node={} 拒绝 {}：term={} 内已认可领导者 {}",
                    nodeId, request.leaderId(), request.term(), leaderHint);
            return new RaftMessages.AppendResponse(nodeId, currentTerm, false, lastLogIndex());
        }
        role = NodeRole.FOLLOWER;
        leaderHint = request.leaderId();

        // 落后于领导者快照点：先安装快照，再续接快照之后的新日志。
        if (request.snapshot() != null
                && request.snapshot().lastIncludedIndex() > lastSnapshottedIndex) {
            installSnapshot(request.snapshot());
        }

        // 日志一致性检查。
        long prevIndex = request.prevLogIndex();
        if (prevIndex > lastLogIndex()) {
            // 响应方比领导者短：直接以本地末索引作为回退提示。
            return new RaftMessages.AppendResponse(nodeId, currentTerm, false, lastLogIndex());
        }
        if (prevIndex >= firstLogIndex()
                && !java.util.Objects.equals(termAt(prevIndex), request.prevLogTerm())) {
            // 同一索引任期不同：返回冲突任期的第一条索引，领导者可一次回退到位（Raft 优化）。
            long conflictTerm = termAt(prevIndex) == null ? 0 : termAt(prevIndex);
            long hint = prevIndex;
            while (hint > firstLogIndex()
                    && java.util.Objects.equals(termAt(hint - 1), conflictTerm)) {
                hint--;
            }
            log.info("[日志冲突] node={} 与领导者 {} 在 idx={} 任期冲突(本地term={})，回退提示 idx={}",
                    nodeId, request.leaderId(), prevIndex, conflictTerm, hint - 1);
            return new RaftMessages.AppendResponse(nodeId, currentTerm, false, hint - 1);
        }
        if (prevIndex < firstLogIndex() && prevIndex > lastSnapshottedIndex) {
            // 请求的前置位置落在本地快照与日志之间的空洞：回退到快照点。
            return new RaftMessages.AppendResponse(nodeId, currentTerm, false, lastSnapshottedIndex);
        }

        // 逐条追加：同任期同索引但内容不同 -> 删除本地及之后的分歧记录（Raft 规则）。
        for (LogEntry incoming : request.entries()) {
            long idx = incoming.index();
            if (idx <= lastSnapshottedIndex) {
                continue;
            }
            int pos = positionOf(idx);
            if (pos < raftLog.size()) {
                LogEntry existing = raftLog.get(pos);
                if (existing.term() != incoming.term()
                        || !sameCommand(existing, incoming)) {
                    truncateSuffixFrom(pos);
                    appendIncoming(incoming);
                    log.info("[日志修复] node={} 截断 idx>={} 的分歧记录并改为 term={} 的新记录",
                            nodeId, idx, incoming.term());
                }
            } else {
                appendIncoming(incoming);
            }
        }

        // 关键：若领导者日志更短（entries 为空的心跳也要处理），跟随者本地多出的后缀
        // 属于未提交分歧，必须删除。领导者日志末索引 = prevLogIndex + entries.size()。
        long leaderLastIndex = request.prevLogIndex() + request.entries().size();
        while (!raftLog.isEmpty() && (lastSnapshottedIndex + raftLog.size()) > leaderLastIndex) {
            LogEntry removed = raftLog.remove(raftLog.size() - 1);
            log.info("[日志修复] node={} 删除领导者日志中不存在的多余未提交后缀 idx={} term={}",
                    nodeId, removed.index(), removed.term());
        }
        persistLog();

        // 推进本地提交位置（不超过领导者已知提交位置与本地日志长度）。
        long newCommit = Math.min(request.leaderCommit(), lastLogIndex());
        advanceCommit(newCommit);

        return new RaftMessages.AppendResponse(nodeId, currentTerm, true, lastLogIndex());
    }

    private boolean sameCommand(LogEntry a, LogEntry b) {
        return java.util.Objects.equals(a.idempotencyKey(), b.idempotencyKey())
                && java.util.Objects.equals(a.command(), b.command());
    }

    private void appendIncoming(LogEntry incoming) {
        raftLog.add(new LogEntry(incoming.term(), incoming.index(),
                incoming.idempotencyKey(), incoming.command(),
                incoming.index() <= commitIndex));
    }

    // ============================== 候选人 / 领导者驱动 ==============================

    /** 发起一次选举：任期加一、投自己，并收集可见节点多数派投票。 */
    public synchronized void startElection() {
        if (!alive || role == NodeRole.LEADER) {
            return;
        }
        currentTerm++;
        role = NodeRole.CANDIDATE;
        votedFor = nodeId;
        leaderHint = null;
        persistMeta();
        int votes = 1;
        log.info("[选举] node={} 在 term={} 成为候选人并自投一票，开始向可见节点拉票", nodeId, currentTerm);
        for (RaftNode peer : peers()) {
            RaftMessages.VoteResponse resp = cluster.deliverVote(nodeId, peer.id(),
                    new RaftMessages.VoteRequest(nodeId, currentTerm, lastLogIndex(), lastLogTerm()));
            if (resp == null) {
                log.info("[选举] node={} 拉票 {} 不可达（分区/宕机），计票跳过", nodeId, peer.id());
                continue;
            }
            if (resp.term() > currentTerm) {
                stepDown(resp.term());
                return;
            }
            if (resp.voteGranted()) {
                votes++;
            }
        }
        int quorum = cluster.quorum();
        if (role == NodeRole.CANDIDATE && votes >= quorum) {
            becomeLeader();
        } else {
            log.info("[选举] node={} term={} 未获多数派（得票 {}/{}），回到跟随者",
                    nodeId, currentTerm, votes, quorum);
            role = NodeRole.FOLLOWER;
        }
    }

    private void becomeLeader() {
        role = NodeRole.LEADER;
        leaderHint = nodeId;
        matchIndex.clear();
        for (RaftNode peer : peers()) {
            matchIndex.put(peer.id(), 0L);
        }
        matchIndex.put(nodeId, lastLogIndex());
        log.info("[当选] node={} 获得多数派支持，成为 term={} 的唯一领导者", nodeId, currentTerm);
        broadcastAppend();
    }

    /** 领导者向所有可达跟随者发送日志/心跳，收集 ack 并推进提交。 */
    public synchronized void broadcastAppend() {
        if (!alive || role != NodeRole.LEADER) {
            return;
        }
        for (RaftNode peer : peers()) {
            replicateTo(peer);
        }
        advanceLeaderCommit();
    }

    private void replicateTo(RaftNode peer) {
        long peerMatch = matchIndex.getOrDefault(peer.id(), 0L);

        // 跟随者落后于领导者快照点：发送 InstallSnapshot（复用 AppendRequest 通道）。
        if (peerMatch < lastSnapshottedIndex) {
            Snapshot snapshot = store.loadSnapshot();
            RaftMessages.AppendRequest req = new RaftMessages.AppendRequest(
                    nodeId, currentTerm, snapshot,
                    snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                    List.of(), commitIndex);
            RaftMessages.AppendResponse resp = cluster.deliverAppend(nodeId, req, peer.id());
            if (resp != null && resp.term() > currentTerm) {
                stepDown(resp.term());
                return;
            }
            if (resp != null && resp.success()) {
                matchIndex.put(peer.id(), Math.max(peerMatch, snapshot.lastIncludedIndex()));
                log.info("[快照同步] 领导者 {} -> {} 安装快照至 idx={}",
                        nodeId, peer.id(), snapshot.lastIncludedIndex());
            }
            return;
        }

        long nextIndex = Math.max(peerMatch + 1, firstLogIndex());
        List<LogEntry> entries = entriesFrom(nextIndex);
        long prevIndex = nextIndex - 1;
        long prevTerm = prevIndex == 0 ? 0 : termAt(prevIndex);
        RaftMessages.AppendRequest request = new RaftMessages.AppendRequest(
                nodeId, currentTerm, null, prevIndex, prevTerm, List.copyOf(entries), commitIndex);
        RaftMessages.AppendResponse resp = cluster.deliverAppend(nodeId, request, peer.id());
        if (resp == null) {
            return;
        }
        if (resp.term() > currentTerm) {
            // 发现更高任期（例如分区恢复后对面的新领导者）：立即下台，
            // 但不打断本轮其余复制，避免旧领导者“卡”在领导者角色。
            stepDown(resp.term());
            return;
        }
        if (resp.success()) {
            // 跟随者最后匹配的全局索引 = 前置索引 + 本次实际追加的条目数，
            // 不能使用领导者自身的 lastLogIndex（心跳时 entries 可能为空）。
            long newMatch = prevIndex + entries.size();
            matchIndex.put(peer.id(), Math.max(matchIndex.getOrDefault(peer.id(), 0L), newMatch));
            if (!entries.isEmpty()) {
                log.info("[复制] 领导者 {} -> {} 复制 {} 条，peerMatch={}",
                        nodeId, peer.id(), entries.size(), matchIndex.get(peer.id()));
            }
        } else {
            // 跟随者日志落后/分歧：采用其回退提示（冲突任期首条索引-1），下一轮即可对齐。
            long hint = Math.max(0, resp.matchIndex());
            long old = matchIndex.getOrDefault(peer.id(), 0L);
            // 提示是该节点的真实日志位置上界，直接采纳（仅在更小时回退）。
            matchIndex.put(peer.id(), Math.min(old, hint));
            log.info("[复制回退] 领导者 {} 对 {} 的 matchIndex {} -> {}",
                    nodeId, peer.id(), old, Math.min(old, hint));
        }
    }

    private void advanceLeaderCommit() {
        if (role != NodeRole.LEADER) {
            return;
        }
        for (long candidate = lastLogIndex(); candidate > commitIndex; candidate--) {
            // Raft 安全性：只能直接提交当前任期的条目；更早任期随当前任期条目一并提交。
            if (termAt(candidate) != currentTerm) {
                continue;
            }
            int replicas = 1; // 领导者自身
            for (RaftNode peer : peers()) {
                if (matchIndex.getOrDefault(peer.id(), 0L) >= candidate) {
                    replicas++;
                }
            }
            if (replicas >= cluster.quorum()) {
                advanceCommit(candidate);
                log.info("[提交推进] 领导者 {} term={} commitIndex -> {}（多数派已复制）",
                        nodeId, currentTerm, commitIndex);
                break;
            }
        }
    }

    // ============================== 客户端提交（幂等） ==============================

    /**
     * 提交一条带幂等键的指令。
     *
     * @return 可区分原因的提交结论
     */
    public synchronized CommitResult submit(String idempotencyKey, String command) {
        if (!alive) {
            return new CommitResult(false, CommitReason.NOT_LEADER, null, currentTerm,
                    idempotencyKey, null, "节点已停机，无法提交");
        }
        if (role != NodeRole.LEADER) {
            return new CommitResult(false, CommitReason.NOT_LEADER, leaderHint, currentTerm,
                    idempotencyKey, null, "当前节点不是领导者，领导者提示: " + leaderHint);
        }

        // 1) 已应用过：必须区分“重复提交”与“同键不同内容”。
        if (stateMachine.isApplied(idempotencyKey)) {
            if (stateMachine.conflictsWith(idempotencyKey, command)) {
                log.info("[提交判定] 幂等键={} 携带不同内容，拒绝（IDEMPOTENCY_KEY_CONFLICT）", idempotencyKey);
                return new CommitResult(false, CommitReason.IDEMPOTENCY_KEY_CONFLICT, nodeId, currentTerm,
                        idempotencyKey, null, "幂等键 " + idempotencyKey + " 已对应不同指令内容");
            }
            String same = stateMachine.appliedResult(idempotencyKey);
            log.info("[提交判定] 幂等键={} 重复提交且内容一致，抑制二次应用，返回首次结果 {}", idempotencyKey, same);
            return new CommitResult(true, CommitReason.DUPLICATE_SUPPRESSED, nodeId, currentTerm,
                    idempotencyKey, same, "重复提交已抑制，返回首次结果");
        }

        // 2) 尚未应用：可能存在一条“未提交”的同键日志（先前复制未达多数派）。
        //    内容相同则复用该既有条目，绝不重复追加；内容不同则按冲突拒绝。
        LogEntry pending = findUncommittedByKey(idempotencyKey);
        if (pending != null) {
            if (!pending.command().equals(command)) {
                log.info("[提交判定] 幂等键={} 与未提交日志 idx={} 内容不同，拒绝（IDEMPOTENCY_KEY_CONFLICT）",
                        idempotencyKey, pending.index());
                return new CommitResult(false, CommitReason.IDEMPOTENCY_KEY_CONFLICT, nodeId, currentTerm,
                        idempotencyKey, null, "幂等键 " + idempotencyKey + " 已存在不同内容的未提交记录");
            }
            log.info("[提交判定] 幂等键={} 命中未提交日志 idx={}，复用并重新驱动多数派复制（不重复追加）",
                    idempotencyKey, pending.index());
            driveReplication();
            if (role != NodeRole.LEADER) {
                return new CommitResult(false, CommitReason.NOT_LEADER, leaderHint, currentTerm,
                        idempotencyKey, null, "复制期间发现更高领导者");
            }
            if (commitIndex < pending.index()) {
                return new CommitResult(false, CommitReason.REPLICATION_FAILED, nodeId, currentTerm,
                        idempotencyKey, null, "多数派复制尚未完成，指令未提交，结论可安全重试");
            }
            String existing = stateMachine.appliedResult(idempotencyKey);
            return new CommitResult(true, CommitReason.APPLIED, nodeId, currentTerm,
                    idempotencyKey, existing, "先前未决的指令本次已提交应用");
        }

        // 3) 全新指令：追加新日志条目并复制到多数派。
        long newIndex = lastLogIndex() + 1;
        LogEntry entry = new LogEntry(currentTerm, newIndex, idempotencyKey, command, false);
        raftLog.add(entry);
        persistLog();
        matchIndex.put(nodeId, newIndex);
        log.info("[提交判定] 幂等键={} 新指令追加为 idx={} term={}，开始多数派复制", idempotencyKey, newIndex, currentTerm);

        driveReplication();
        if (role != NodeRole.LEADER) {
            // 复制过程中发现更高任期，已下台。
            return new CommitResult(false, CommitReason.NOT_LEADER, leaderHint, currentTerm,
                    idempotencyKey, null, "复制期间发现更高领导者");
        }

        if (commitIndex < newIndex) {
            log.info("[提交判定] 幂等键={} 未能复制到多数派，提交失败（记录未应用、不生效）", idempotencyKey);
            return new CommitResult(false, CommitReason.REPLICATION_FAILED, nodeId, currentTerm,
                    idempotencyKey, null, "多数派复制失败，指令未提交");
        }

        String value = stateMachine.appliedResult(idempotencyKey);
        log.info("[提交判定] 幂等键={} 已提交并应用 idx={}，结果 {}", idempotencyKey, newIndex, value);
        maybeSnapshot();
        return new CommitResult(true, CommitReason.APPLIED, nodeId, currentTerm,
                idempotencyKey, value, "指令已提交并应用");
    }

    /** 在尚未应用（索引大于 commitIndex）的本地日志尾中按幂等键查找。 */
    private LogEntry findUncommittedByKey(String idempotencyKey) {
        for (LogEntry e : raftLog) {
            if (e.index() > commitIndex && e.idempotencyKey().equals(idempotencyKey)) {
                return e;
            }
        }
        return null;
    }

    /** 向所有可达跟随者复制并尝试推进提交（客户端提交路径复用）。 */
    private void driveReplication() {
        for (RaftNode peer : peers()) {
            replicateTo(peer);
            if (role != NodeRole.LEADER) {
                return;
            }
        }
        advanceLeaderCommit();
    }

    // ============================== 读接口 ==============================

    public String read(String key) {
        return committedState().get(key);
    }

    public synchronized Map<String, String> committedState() {
        return stateMachine.data();
    }

    public synchronized List<LogEntry> logView() {
        return List.copyOf(raftLog);
    }

    // ============================== 故障与恢复 ==============================

    /** 模拟崩溃：停止参与一切协议，已持久化数据保留。 */
    public synchronized void crash() {
        alive = false;
        log.info("[故障] node={} 崩溃（持久化数据保留：term={}, commitIndex={}, 日志 {} 条）",
                nodeId, currentTerm, commitIndex, raftLog.size());
    }

    /** 模拟重启：加载快照与操作历史，只重放已提交记录，未提交记录不生效。 */
    public synchronized void restart() {
        recoverFromDisk(false);
        alive = true;
        log.info("[恢复] node={} 重启完成：term={}, role=FOLLOWER, 快照至 idx={}, commitIndex={}, 已应用至 idx={}",
                nodeId, currentTerm, lastSnapshottedIndex, commitIndex, stateMachine.lastAppliedIndex());
    }

    private void recoverFromDisk(boolean bootstrap) {
        currentTerm = store.loadCurrentTerm();
        votedFor = store.loadVotedFor();
        commitIndex = store.loadCommitIndex();
        Snapshot snapshot = store.loadSnapshot();
        raftLog.clear();
        raftLog.addAll(store.loadLog());
        stateMachine.restoreSnapshot(snapshot);
        lastSnapshottedIndex = snapshot == null ? 0 : snapshot.lastIncludedIndex();
        role = NodeRole.FOLLOWER;
        leaderHint = null;
        if (snapshot != null) {
            log.info("[恢复] node={} 加载快照至 idx={}（含 {} 个键、{} 条幂等记录）",
                    nodeId, snapshot.lastIncludedIndex(),
                    snapshot.data().size(), snapshot.appliedResults().size());
        }
        long appliedBefore = stateMachine.lastAppliedIndex();
        // 只重放“已提交”且位于快照之后的日志；未提交记录绝不应用到状态机。
        for (LogEntry e : raftLog) {
            if (e.index() <= commitIndex && e.index() > lastSnapshottedIndex) {
                stateMachine.apply(e);
            }
        }
        long replayed = stateMachine.lastAppliedIndex() - appliedBefore;
        if (!bootstrap || replayed > 0 || snapshot != null) {
            log.info("[恢复] node={} 重放已提交日志 {} 条，已应用至 idx={}（未提交记录不生效）",
                    nodeId, Math.max(0, replayed), stateMachine.lastAppliedIndex());
        }
    }

    // ============================== 内部工具 ==============================

    private void advanceCommit(long target) {
        if (target <= commitIndex) {
            return;
        }
        long upper = Math.min(target, lastLogIndex());
        for (int i = 0; i < raftLog.size(); i++) {
            LogEntry e = raftLog.get(i);
            if (e.index() <= upper && e.index() > commitIndex) {
                String value = stateMachine.apply(e);
                raftLog.set(i, e.withCommitted(true));
                log.info("[应用] node={} 应用已提交 idx={} term={} key={} -> {}",
                        nodeId, e.index(), e.term(), e.idempotencyKey(), value);
            }
        }
        commitIndex = upper;
        persistMeta();
        persistLog();
        maybeSnapshot();
    }

    /** 安装领导者快照：替换状态机数据，丢弃快照已覆盖的本地日志。 */
    private void installSnapshot(Snapshot snapshot) {
        stateMachine.restoreSnapshot(snapshot);
        while (!raftLog.isEmpty() && raftLog.get(0).index() <= snapshot.lastIncludedIndex()) {
            raftLog.remove(0);
        }
        lastSnapshottedIndex = snapshot.lastIncludedIndex();
        store.saveSnapshot(snapshot);
        persistLog();
        if (commitIndex < snapshot.lastIncludedIndex()) {
            commitIndex = snapshot.lastIncludedIndex();
            persistMeta();
        }
        log.info("[安装快照] node={} 从领导者安装快照至 idx={} term={}（含 {} 个键）",
                nodeId, snapshot.lastIncludedIndex(), snapshot.lastIncludedTerm(),
                snapshot.data().size());
    }

    private void maybeSnapshot() {        int threshold = properties.getSnapshotThreshold();
        if (threshold <= 0 || raftLog.size() <= threshold) {
            return;
        }
        long snapIndex = Math.min(commitIndex, lastLogIndex());
        long snapTerm = termAt(snapIndex);
        Snapshot snapshot = stateMachine.buildSnapshot(snapIndex, snapTerm);
        store.saveSnapshot(snapshot);
        // 删除快照已覆盖的日志前缀。
        int removeCount = 0;
        while (!raftLog.isEmpty() && raftLog.get(0).index() <= snapIndex) {
            raftLog.remove(0);
            removeCount++;
        }
        lastSnapshottedIndex = snapIndex;
        persistLog();
        log.info("[快照] node={} 生成快照至 idx={} term={}，压缩日志 {} 条，保留 {} 条",
                nodeId, snapIndex, snapTerm, removeCount, raftLog.size());
    }

    private void stepDown(long newTerm) {
        currentTerm = newTerm;
        role = NodeRole.FOLLOWER;
        votedFor = null;
        leaderHint = null;
        matchIndex.clear();
        persistMeta();
        log.info("[任期更新] node={} 发现更高任期，步进为 term={} 并转为跟随者", nodeId, newTerm);
    }

    private List<RaftNode> peers() {
        List<RaftNode> result = new ArrayList<>();
        for (RaftNode n : cluster.nodes()) {
            if (!n.id().equals(nodeId)) {
                result.add(n);
            }
        }
        return result;
    }

    private long firstLogIndex() {
        return lastSnapshottedIndex + 1;
    }

    private int positionOf(long globalIndex) {
        return (int) (globalIndex - firstLogIndex());
    }

    private Long termAt(long globalIndex) {
        if (globalIndex <= lastSnapshottedIndex) {
            Snapshot s = store.loadSnapshot();
            return s == null ? null : s.lastIncludedTerm();
        }
        int pos = positionOf(globalIndex);
        return pos >= 0 && pos < raftLog.size() ? raftLog.get(pos).term() : null;
    }

    private List<LogEntry> entriesFrom(long globalIndex) {
        List<LogEntry> out = new ArrayList<>();
        for (LogEntry e : raftLog) {
            if (e.index() >= globalIndex) {
                out.add(e);
            }
        }
        return out;
    }

    private void truncateSuffixFrom(int position) {
        while (raftLog.size() > position) {
            raftLog.remove(raftLog.size() - 1);
        }
    }

    private void persistMeta() {
        store.saveMeta(currentTerm, votedFor, commitIndex);
    }

    private void persistLog() {
        store.saveLog(raftLog);
    }

    @Override
    public String toString() {
        return "RaftNode{" + nodeId + ", role=" + role + ", term=" + currentTerm
                + ", commitIndex=" + commitIndex + ", lastLogIndex=" + lastLogIndex()
                + ", alive=" + alive + "}";
    }

    /** 测试/状态接口用：各副本的简明状态。 */
    public synchronized Map<String, Object> describe() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("nodeId", nodeId);
        m.put("role", role.name());
        m.put("term", currentTerm);
        m.put("commitIndex", commitIndex);
        m.put("lastLogIndex", lastLogIndex());
        m.put("alive", alive);
        m.put("leaderHint", leaderHint);
        m.put("state", committedState());
        return m;
    }
}
