package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 单个 Raft 副本节点（进程内、单线程 tick 驱动，无线程与真实网络不确定性）。
 *
 * <p>职责：</p>
 * <ul>
 *   <li><b>领导者选举与任期推进</b>：多数派投票才可当选；同一任期最多一个领导者；
 *       遇到更高任期立即退回跟随者；</li>
 *   <li><b>幂等提交</b>：以 {@code (clientId,requestId)} 为去重键，指令内容哈希二次校验，
 *       重复请求返回首次结论且不二次应用，内容冲突以 {@link SubmitResult.Status#IDEMPOTENCY_CONFLICT} 拒绝；</li>
 *   <li><b>日志复制与故障恢复</b>：提交前持久化，仅多数派提交的条目应用到状态机；
 *       按快照 + 快照后日志恢复，未提交条目保留但永不生效；分歧由 AppendEntries 一致性检查截断修复。</li>
 *   <li><b>落后副本追赶</b>：跟随者的同步位置落入领导快照边界之内（其缺失历史已被压缩）时，
 *       领导改发 InstallSnapshot 把已提交状态整体推送，跟随者据此重建并从边界继续增量复制；
 *       推送幂等可重复、可在领导/跟随者重启后续传，且拒绝不新于本地提交进度的旧快照，
 *       已更新副本绝不会被旧内容覆盖。</li>
 * </ul>
 */
public class RaftNode {

    /** 节点发出的消息信封，由 {@link RaftCluster} 按分区规则投递。 */
    record Envelope(String to, Object message) {
    }

    private static final class Pending {
        final String clientId;
        final String requestId;
        final long term;
        final long index;

        Pending(String clientId, String requestId, long term, long index) {
            this.clientId = clientId;
            this.requestId = requestId;
            this.term = term;
            this.index = index;
        }
    }

    private final String id;
    private NodeStorage storage;
    private RaftCluster cluster;

    private final int electionTimeoutMin;
    private final int electionTimeoutRange;
    private final int heartbeatInterval;
    private final long snapshotThreshold;

    // ---- 持久化状态（从 storage 加载/写回） ----
    private long currentTerm;
    private String votedFor;
    private long commitIndex;
    private Snapshot snapshot;
    private final List<LogEntry> log = new ArrayList<>();

    // ---- 易失状态 ----
    private NodeRole role = NodeRole.FOLLOWER;
    private String leaderHint;
    private final KeyValueStateMachine stateMachine = new KeyValueStateMachine();
    private final Map<String, IdempotencyRecord> idempotency = new LinkedHashMap<>();
    private long lastApplied;

    private int electionElapsed;
    private int electionTimeoutTicks;
    private int heartbeatElapsed;
    private final Random random = new Random();
    private final Map<String, Boolean> votesGranted = new HashMap<>();
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, Pending> pendingByIndex = new HashMap<>();

    private record Directed(Object message, String from) {
    }

    private final Deque<Directed> inbox = new ArrayDeque<>();
    private final Deque<Envelope> outbox = new ArrayDeque<>();
    private boolean up = true;
    /** 测试钩子：暂停选举计时（用于只黑入站消息、但不想让该节点发起选举的场景）。 */
    private boolean electionTimerPaused = false;

    public RaftNode(String nodeId, NodeStorage storage, int electionTimeoutMin,
                    int electionTimeoutRange, int heartbeatInterval,
                    long snapshotThreshold) {
        this.id = nodeId;
        this.storage = storage;
        this.electionTimeoutMin = electionTimeoutMin;
        this.electionTimeoutRange = electionTimeoutRange;
        this.heartbeatInterval = heartbeatInterval;
        this.snapshotThreshold = snapshotThreshold;
        restoreFromStorage();
        resetElectionTimer();
    }

    void attachCluster(RaftCluster cluster) {
        this.cluster = cluster;
    }

    // ==================== 恢复 ====================

    private void restoreFromStorage() {
        HardState hard = storage.loadHardState();
        this.currentTerm = hard.getCurrentTerm();
        this.votedFor = hard.getVotedFor();
        this.commitIndex = hard.getCommitIndex();

        this.snapshot = storage.loadSnapshot();
        this.log.clear();
        // 防御：只接受严格位于快照边界之后的日志（边界内条目已在快照中，保留会造成位置错位）
        for (LogEntry entry : storage.loadLog()) {
            if (entry.getIndex() > snapshot.getLastIncludedIndex()) {
                log.add(entry);
            }
        }

        this.stateMachine.resetFrom(snapshot.getKv());
        this.idempotency.clear();
        this.idempotency.putAll(snapshot.getIdempotency());

        // 回放快照之后、已提交（commitIndex）之前的日志；未提交条目不生效。
        lastApplied = snapshot.getLastIncludedIndex();
        for (LogEntry entry : log) {
            if (entry.getIndex() <= lastApplied) {
                continue;
            }
            if (entry.getIndex() > commitIndex) {
                break;
            }
            String result = stateMachine.apply(entry);
            idempotency.put(dedupKey(entry.getClientId(), entry.getRequestId()),
                    new IdempotencyRecord(entry.getClientId(), entry.getRequestId(),
                            hash(entry.getCommand()), entry.getIndex(), result));
            lastApplied = entry.getIndex();
        }
        System.out.printf("[恢复] 节点 %s: term=%d votedFor=%s commitIndex=%d 快照至=%d 日志末=%d 已回放至=%d%n",
                id, currentTerm, votedFor, commitIndex, snapshot.getLastIncludedIndex(),
                lastLogIndex(), lastApplied);
    }

    // ==================== 基本查询 ====================

    public String getId() { return id; }
    public NodeRole getRole() { return role; }
    public long getCurrentTerm() { return currentTerm; }
    public long getCommitIndex() { return commitIndex; }
    public String getLeaderHint() { return leaderHint; }
    public boolean isUp() { return up; }

    public long lastLogIndex() {
        return log.isEmpty() ? snapshot.getLastIncludedIndex() : log.get(log.size() - 1).getIndex();
    }

    public long lastLogTerm() {
        return log.isEmpty() ? snapshot.getLastIncludedTerm() : log.get(log.size() - 1).getTerm();
    }

    /** 当前快照（压缩）边界下标；落后者的同步位置落入该边界之内时需要 InstallSnapshot 追赶。 */
    public long getSnapshotIndex() {
        return snapshot.getLastIncludedIndex();
    }

    /** 当前快照内容的防御性副本（测试用：构造"迟到的旧快照"验证防旧覆盖）。 */
    public Snapshot snapshotCopy() {
        return new Snapshot(snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                new LinkedHashMap<>(snapshot.getKv()),
                new LinkedHashMap<>(snapshot.getIdempotency()));
    }

    public long getLastLogIndex() { return lastLogIndex(); }
    public long getLastLogTerm() { return lastLogTerm(); }

    /** 下标可能落在快照内或快照后日志中。 */
    private long termAt(long index) {
        if (index == 0) {
            return 0;
        }
        if (index == snapshot.getLastIncludedIndex()) {
            return snapshot.getLastIncludedTerm();
        }
        LogEntry entry = entryAt(index);
        return entry == null ? -1 : entry.getTerm();
    }

    private LogEntry entryAt(long index) {
        int pos = (int) (index - snapshot.getLastIncludedIndex() - 1);
        if (pos < 0 || pos >= log.size()) {
            return null;
        }
        return log.get(pos);
    }

    public String read(String key) {
        return stateMachine.get(key);
    }

    public List<LogEntry> committedEntries() {
        List<LogEntry> result = new ArrayList<>();
        for (LogEntry entry : log) {
            if (entry.getIndex() <= commitIndex) {
                result.add(entry);
            }
        }
        return result;
    }

    // ==================== 消息队列 ====================

    void deliver(Object message, String from) {
        inbox.addLast(new Directed(message, from));
    }

    /**
     * 测试用：直接把一条消息注入本节点收件箱（绕过正常网络路径），
     * 用于确定性地构造"旧领导迟到的 InstallSnapshot"等边界时序。
     */
    void injectMessage(Object message, String from) {
        deliver(message, from);
    }

    /** 测试钩子：暂停/恢复跟随者选举计时。 */
    public void setElectionTimerPaused(boolean paused) {
        this.electionTimerPaused = paused;
        if (!paused) {
            resetElectionTimer();
        }
    }

    boolean hasOutgoing() {
        return !outbox.isEmpty();
    }

    Envelope pollOutgoing() {
        return outbox.pollFirst();
    }

    // ==================== 故障与重启 ====================

    /** 模拟崩溃：丢弃全部内存状态（"进程死亡"），持久化字节保留。 */
    public void crash() {
        System.out.printf("[故障] 节点 %s 崩溃（磁盘保留：term=%d commit=%d 日志末=%d）%n",
                id, currentTerm, commitIndex, lastLogIndex());
        up = false;
    }

    /** 用同一份"磁盘"重启：回放快照 + 已提交历史，角色退回跟随者。 */
    public void restart(NodeStorage sameStorage) {
        this.storage = sameStorage;
        this.role = NodeRole.FOLLOWER;
        this.leaderHint = null;
        this.inbox.clear();
        this.outbox.clear();
        this.votesGranted.clear();
        this.nextIndex.clear();
        this.matchIndex.clear();
        this.pendingByIndex.clear();
        this.up = true;
        restoreFromStorage();
        resetElectionTimer();
        System.out.printf("[恢复] 节点 %s 重启完成，角色=FOLLOWER，当前 term=%d%n", id, currentTerm);
    }

    // ==================== 主驱动 ====================

    public void tick() {
        if (!up) {
            return;
        }
        if (role == NodeRole.LEADER) {
            heartbeatElapsed++;
            if (heartbeatElapsed >= heartbeatInterval) {
                heartbeatElapsed = 0;
                broadcastAppendEntries();
            }
        } else {
            if (!electionTimerPaused) {
                electionElapsed++;
                if (electionElapsed >= electionTimeoutTicks) {
                    startElection();
                }
            }
        }
        drainInbox();
    }

    private void resetElectionTimer() {
        electionElapsed = 0;
        electionTimeoutTicks = electionTimeoutMin + random.nextInt(electionTimeoutRange);
    }

    // ==================== 选举 ====================

    private void startElection() {
        currentTerm++;
        role = NodeRole.CANDIDATE;
        leaderHint = null;
        votedFor = id;
        persistHardState();
        votesGranted.clear();
        votesGranted.put(id, true);
        resetElectionTimer();
        heartbeatElapsed = 0;

        System.out.printf("[选举] 节点 %s 在 term=%d 发起选举（日志末=%d/%d），先投自己%n",
                id, currentTerm, lastLogIndex(), lastLogTerm());

        RaftMessage.RequestVote request =
                new RaftMessage.RequestVote(currentTerm, id, lastLogIndex(), lastLogTerm());
        for (String peer : cluster.peerIds(id)) {
            outbox.addLast(new Envelope(peer, request));
        }
        // 单节点集群立即胜选
        if (cluster.isMajority(votesGranted.size())) {
            becomeLeader();
        }
    }

    private void handleRequestVote(RaftMessage.RequestVote request, String from) {
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        boolean grant = false;
        if (request.term() == currentTerm) {
            boolean notVotedOther = votedFor == null || votedFor.equals(request.candidateId());
            boolean logUpToDate = request.lastLogTerm() > lastLogTerm()
                    || (request.lastLogTerm() == lastLogTerm()
                        && request.lastLogIndex() >= lastLogIndex());
            if (notVotedOther && logUpToDate) {
                grant = true;
                votedFor = request.candidateId();
                persistHardState();
                resetElectionTimer();
            }
        }
        if (grant) {
            System.out.printf("[投票] 节点 %s 在 term=%d 投票给候选者 %s（日志 %d/%d 不劣于本地 %d/%d）%n",
                    id, currentTerm, request.candidateId(),
                    request.lastLogIndex(), request.lastLogTerm(), lastLogIndex(), lastLogTerm());
        } else {
            System.out.printf("[投票] 节点 %s 拒绝候选者 %s 的 term=%d 投票请求（已投=%s 对方日志=%d/%d 本地=%d/%d）%n",
                    id, request.candidateId(), request.term(), votedFor,
                    request.lastLogIndex(), request.lastLogTerm(), lastLogIndex(), lastLogTerm());
        }
        outbox.addLast(new Envelope(from,
                new RaftMessage.RequestVoteResponse(currentTerm, grant)));
    }

    private void handleRequestVoteResponse(RaftMessage.RequestVoteResponse response, String from) {
        if (response.term() > currentTerm) {
            stepDown(response.term());
            return;
        }
        if (role != NodeRole.CANDIDATE || response.term() != currentTerm) {
            return;
        }
        if (response.voteGranted()) {
            votesGranted.put(from, true);
            if (cluster.isMajority(votesGranted.size()) && role == NodeRole.CANDIDATE) {
                becomeLeader();
            }
        }
    }

    private void becomeLeader() {
        role = NodeRole.LEADER;
        leaderHint = id;
        heartbeatElapsed = 0;
        long start = lastLogIndex() + 1;
        for (String peer : cluster.peerIds(id)) {
            nextIndex.put(peer, start);
            matchIndex.put(peer, snapshot.getLastIncludedIndex());
        }
        System.out.printf("[选举] ★ 节点 %s 获得多数派，在 term=%d 成为领导者 ★%n", id, currentTerm);
        // 新官上任：立即发一轮心跳宣告权威并提交一条 NOP，便于尽快提交前任遗留条目
        appendNopAndReplicate();
    }

    private void appendNopAndReplicate() {
        LogEntry nop = new LogEntry(currentTerm, lastLogIndex() + 1, "$system", "nop-" + currentTerm, "NOP");
        appendLocal(nop);
        broadcastAppendEntries();
    }

    private void stepDown(long newTerm) {
        if (newTerm > currentTerm) {
            System.out.printf("[任期] 节点 %s 发现更高任期 %d -> 退回 FOLLOWER（本地原 term=%d）%n",
                    id, newTerm, currentTerm);
        }
        currentTerm = newTerm;
        role = NodeRole.FOLLOWER;
        votedFor = null;
        persistHardState();
        votesGranted.clear();
        resetElectionTimer();
    }

    // ==================== 日志复制 ====================

    private void broadcastAppendEntries() {
        for (String peer : cluster.peerIds(id)) {
            sendAppendEntries(peer);
        }
    }

    private void sendAppendEntries(String peer) {
        long snapshotIndex = snapshot.getLastIncludedIndex();
        long next = nextIndex.getOrDefault(peer, lastLogIndex() + 1);

        if (next <= snapshotIndex) {
            // 跟随者缺失的历史已被本节点压缩：日志追加无从对齐 prevLog，
            // 改用 InstallSnapshot 把快照边界内的已提交状态整体送过去（落后副本追赶）。
            // 每轮心跳都可重发：快照安装是幂等的（相同边界再次安装结果不变），
            // 目标宕机导致响应丢失时也不会"卡死"在等待中。
            System.out.printf("[追赶] 节点 %s(领导) 发现 %s 的同步位置 %d 已落在快照边界 %d 之内，改推快照（term=%d，leaderCommit=%d）%n",
                    id, peer, next, snapshotIndex, snapshot.getLastIncludedTerm(), commitIndex);
            outbox.addLast(new Envelope(peer, new RaftMessage.InstallSnapshot(
                    currentTerm, id,
                    snapshotIndex, snapshot.getLastIncludedTerm(),
                    snapshot.getKv(), snapshot.getIdempotency(), commitIndex)));
            return;
        }

        long prevLogIndex = next - 1;
        long prevLogTerm = termAt(prevLogIndex);
        List<LogEntry> entries = new ArrayList<>();
        for (long i = next; i <= lastLogIndex(); i++) {
            LogEntry e = entryAt(i);
            if (e != null) {
                entries.add(e);
            }
        }
        outbox.addLast(new Envelope(peer, new RaftMessage.AppendEntries(
                currentTerm, id, prevLogIndex, prevLogTerm, entries, commitIndex)));
    }

    private void handleAppendEntries(RaftMessage.AppendEntries request, String from) {
        if (request.term() < currentTerm) {
            outbox.addLast(new Envelope(from,
                    new RaftMessage.AppendEntriesResponse(currentTerm, false, -1, lastLogIndex())));
            return;
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        role = NodeRole.FOLLOWER;
        leaderHint = request.leaderId();
        resetElectionTimer();

        long snapshotIndex = snapshot.getLastIncludedIndex();
        long prevLogIndex = request.prevLogIndex();

        if (prevLogIndex < snapshotIndex) {
            // 领导者过旧：直接拒绝，conflictHint 无意义
            outbox.addLast(new Envelope(from,
                    new RaftMessage.AppendEntriesResponse(currentTerm, false, -1, lastLogIndex())));
            return;
        }

        if (prevLogIndex > lastLogIndex() || termAt(prevLogIndex) != request.prevLogTerm()) {
            // 一致性检查失败：给出冲突 term 的起点提示，帮助领导快速回退
            long hint;
            if (prevLogIndex <= lastLogIndex()) {
                long conflictTerm = termAt(prevLogIndex);
                hint = prevLogIndex;
                for (LogEntry e : log) {
                    if (e.getTerm() == conflictTerm) {
                        hint = e.getIndex();
                        break;
                    }
                }
                System.out.printf("[复制] 节点 %s 在 index=%d 与领导 %s 日志分歧（本地 term=%d，对方 term=%d），回退提示=%d%n",
                        id, prevLogIndex, from, termAt(prevLogIndex), request.prevLogTerm(), hint);
            } else {
                hint = lastLogIndex() + 1;
            }
            outbox.addLast(new Envelope(from,
                    new RaftMessage.AppendEntriesResponse(currentTerm, false, hint, lastLogIndex())));
            return;
        }

        // 合并条目：删除冲突后缀，追加缺失部分
        for (LogEntry incoming : request.entries()) {
            long index = incoming.getIndex();
            if (index <= snapshotIndex) {
                continue;
            }
            LogEntry existing = entryAt(index);
            if (existing == null) {
                if (index != lastLogIndex() + 1) {
                    // 理论上不会出现空洞，保守处理
                    continue;
                }
                log.add(incoming);
            } else if (existing.getTerm() != incoming.getTerm()) {
                System.out.printf("[复制] 节点 %s 截断 index=%d 起的分歧日志（term %d -> %d）%n",
                        id, index, existing.getTerm(), incoming.getTerm());
                int pos = (int) (index - snapshotIndex - 1);
                log.subList(pos, log.size()).clear();
                log.add(incoming);
            }
        }
        storage.saveLog(log);

        if (request.leaderCommit() > commitIndex) {
            long newCommit = Math.min(request.leaderCommit(), lastLogIndex());
            advanceCommit(newCommit);
        }
        outbox.addLast(new Envelope(from,
                new RaftMessage.AppendEntriesResponse(currentTerm, true, -1, lastLogIndex())));
    }

    private void handleAppendEntriesResponse(RaftMessage.AppendEntriesResponse response, String from) {
        if (response.term() > currentTerm) {
            stepDown(response.term());
            return;
        }
        if (role != NodeRole.LEADER || response.term() != currentTerm) {
            return;
        }
        if (response.success()) {
            long peerMatch = matchIndex.getOrDefault(from, snapshot.getLastIncludedIndex());
            long updated = Math.max(peerMatch, response.followerLastIndex());
            matchIndex.put(from, updated);
            // 以跟随者实际日志末尾为权威，修复截断修复后 nextIndex 悬空的问题
            nextIndex.put(from, response.followerLastIndex() + 1);
            advanceLeaderCommit();
        } else {
            // 回退下界允许落到快照边界本身：一旦 next <= 快照边界，下一轮即转 InstallSnapshot。
            long base = snapshot.getLastIncludedIndex();
            // 跟随者自报其日志末尾：从末尾之后重发，配合 conflictHint 快速定位分歧
            long byFollower = Math.max(base, response.followerLastIndex() + 1);
            long next;
            if (response.conflictHint() > 0) {
                next = Math.max(base, Math.min(response.conflictHint(), byFollower));
            } else {
                next = byFollower;
            }
            nextIndex.put(from, next);
            sendAppendEntries(from);
        }
    }

    private void handleInstallSnapshot(RaftMessage.InstallSnapshot request, String from) {
        if (request.term() < currentTerm) {
            // 旧任期领导者：不安装、不退位
            outbox.addLast(new Envelope(from,
                    new RaftMessage.InstallSnapshotResponse(currentTerm, false,
                            snapshot.getLastIncludedIndex())));
            return;
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        role = NodeRole.FOLLOWER;
        leaderHint = request.leaderId();
        resetElectionTimer();

        long incomingIndex = request.lastIncludedIndex();

        // 防旧覆盖：快照边界不新于本地已提交进度，说明手上内容已不旧于这份快照
        // （可能是旧领导迟到的推送，或追赶途中集群又压缩过）。拒绝它，绝不回退状态。
        if (incomingIndex <= commitIndex) {
            System.out.printf("[追赶] 节点 %s 拒绝来自 %s 的旧快照（快照边界=%d 已不新于本地提交=%d），本地状态不回退%n",
                    id, from, incomingIndex, commitIndex);
            outbox.addLast(new Envelope(from,
                    new RaftMessage.InstallSnapshotResponse(currentTerm, false,
                            snapshot.getLastIncludedIndex())));
            return;
        }

        long oldSnapshotIndex = snapshot.getLastIncludedIndex();
        // 若本地恰有边界处的条目且 term 与快照不一致，则边界之后的尾部日志全部丢弃；
        // 否则保留边界之后严格连续（首条 index==incomingIndex+1）的尾部日志（标准 Raft 规则）。
        LogEntry atBoundary = entryAt(incomingIndex);
        boolean discardTail = atBoundary != null && atBoundary.getTerm() != request.lastIncludedTerm();
        List<LogEntry> retained = new ArrayList<>();
        if (!discardTail) {
            for (LogEntry e : log) {
                if (e.getIndex() > incomingIndex) {
                    retained.add(e);
                }
            }
            if (retained.isEmpty() || retained.get(0).getIndex() != incomingIndex + 1) {
                retained.clear();
            }
        }

        // 用快照内容整体替换已提交视图
        Snapshot installed = new Snapshot(incomingIndex, request.lastIncludedTerm(),
                new LinkedHashMap<>(request.kv()),
                new LinkedHashMap<>(request.idempotency()));
        storage.saveSnapshot(installed);
        log.clear();
        log.addAll(retained);
        storage.saveLog(log);
        snapshot = installed;
        stateMachine.resetFrom(snapshot.getKv());
        idempotency.clear();
        idempotency.putAll(snapshot.getIdempotency());
        lastApplied = incomingIndex;

        // 快照内状态天然已提交：提交进度至少推进到边界；leaderCommit 更靠前时一并推进，
        // 但绝不越过本地实际日志末尾（未持有条目不得被标为已提交、更不会被应用）。
        long newCommit = Math.min(Math.max(request.leaderCommit(), incomingIndex), lastLogIndex());
        System.out.printf("[追赶] 节点 %s 安装来自领导 %s 的快照：边界 %d/term=%d（原快照边界=%d），保留尾部日志 %d 条，提交推进 %d -> %d%n",
                id, from, incomingIndex, request.lastIncludedTerm(),
                oldSnapshotIndex, log.size(), commitIndex, newCommit);
        commitIndex = newCommit;
        persistHardState();
        // 快照本身已整体生效（resetFrom），只需回放边界之后、提交点之前的日志，不重复应用快照内容
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            LogEntry entry = entryAt(i);
            if (entry != null) {
                applyCommitted(entry);
                lastApplied = i;
            }
        }

        outbox.addLast(new Envelope(from,
                new RaftMessage.InstallSnapshotResponse(currentTerm, true, incomingIndex)));
    }

    private void handleInstallSnapshotResponse(RaftMessage.InstallSnapshotResponse response, String from) {
        if (response.term() > currentTerm) {
            stepDown(response.term());
            return;
        }
        if (role != NodeRole.LEADER || response.term() != currentTerm) {
            return;
        }
        if (!response.success()) {
            // 跟随者拒绝（例如该快照比它已提交进度还旧）：跟随者手上内容更新，
            // 让 nextIndex 从其当前进度之后继续，由下一轮增量复制对齐。
            System.out.printf("[追赶] 节点 %s(领导) 推送快照给 %s 被拒绝（快照边界=%d），跟随者内容更新，改走增量探测%n",
                    id, from, response.lastIncludedIndex());
            long probe = Math.max(response.lastIncludedIndex() + 1, snapshot.getLastIncludedIndex() + 1);
            nextIndex.put(from, probe);
            sendAppendEntries(from);
            return;
        }
        long boundary = response.lastIncludedIndex();
        matchIndex.put(from, Math.max(matchIndex.getOrDefault(from, snapshot.getLastIncludedIndex()), boundary));
        nextIndex.put(from, boundary + 1);
        System.out.printf("[追赶] 节点 %s(领导) 确认 %s 已安装快照至 index=%d，改为从 %d 起增量复制%n",
                id, from, boundary, boundary + 1);
        advanceLeaderCommit();
        sendAppendEntries(from);
    }

    private void drainInbox() {
        Directed directed;
        while ((directed = inbox.pollFirst()) != null) {
            Object message = directed.message();
            String from = directed.from();
            if (message instanceof RaftMessage.RequestVote rv) {
                handleRequestVote(rv, from);
            } else if (message instanceof RaftMessage.RequestVoteResponse rvr) {
                handleRequestVoteResponse(rvr, from);
            } else if (message instanceof RaftMessage.AppendEntries ae) {
                handleAppendEntries(ae, from);
            } else if (message instanceof RaftMessage.AppendEntriesResponse aer) {
                handleAppendEntriesResponse(aer, from);
            } else if (message instanceof RaftMessage.InstallSnapshot is) {
                handleInstallSnapshot(is, from);
            } else if (message instanceof RaftMessage.InstallSnapshotResponse isr) {
                handleInstallSnapshotResponse(isr, from);
            }
        }
    }

    // ==================== 提交与应用 ====================

    private void appendLocal(LogEntry entry) {
        log.add(entry);
        storage.saveLog(log);
    }

    private void advanceLeaderCommit() {
        long target = commitIndex;
        for (long n = lastLogIndex(); n > commitIndex; n--) {
            // Raft 安全约束：领导只直接提交当前任期的条目
            if (termAt(n) != currentTerm) {
                continue;
            }
            int replicated = 1;
            for (String peer : cluster.peerIds(id)) {
                if (matchIndex.getOrDefault(peer, 0L) >= n) {
                    replicated++;
                }
            }
            if (cluster.isMajority(replicated)) {
                target = n;
                break;
            }
        }
        if (target > commitIndex) {
            advanceCommit(target);
        }
    }

    private void advanceCommit(long newCommitIndex) {
        if (newCommitIndex <= commitIndex) {
            return;
        }
        commitIndex = newCommitIndex;
        persistHardState();
        for (long i = lastApplied + 1; i <= commitIndex; i++) {
            LogEntry entry = entryAt(i);
            if (entry != null) {
                applyCommitted(entry);
                lastApplied = i;
            }
        }
        maybeSnapshot();
    }

    private void applyCommitted(LogEntry entry) {
        if (entry.getIndex() <= snapshot.getLastIncludedIndex()) {
            return;
        }
        String result = stateMachine.apply(entry);
        String key = dedupKey(entry.getClientId(), entry.getRequestId());
        idempotency.put(key, new IdempotencyRecord(entry.getClientId(), entry.getRequestId(),
                hash(entry.getCommand()), entry.getIndex(), result));
        System.out.printf("[提交] 节点 %s 应用 index=%d term=%d 指令='%s' 结果='%s'（幂等键=%s）%n",
                id, entry.getIndex(), entry.getTerm(), entry.getCommand(), result, key);
        notifyPending(entry, result);
    }

    private void notifyPending(LogEntry entry, String result) {
        Pending pending = pendingByIndex.get(entry.getIndex());
        if (pending != null) {
            System.out.printf("[提交] 节点 %s 通知客户端 %s 请求 %s 已提交于 index=%d%n",
                    id, pending.clientId, pending.requestId, entry.getIndex());
        }
    }

    private void maybeSnapshot() {
        long retained = lastLogIndex() - snapshot.getLastIncludedIndex();
        if (retained < snapshotThreshold) {
            return;
        }
        long compactThrough = commitIndex;
        LogEntry boundary = entryAt(compactThrough);
        if (boundary == null) {
            return;
        }
        Snapshot newSnapshot = new Snapshot(compactThrough, boundary.getTerm(),
                stateMachine.snapshot(), new LinkedHashMap<>(idempotency));
        storage.saveSnapshot(newSnapshot);

        int keepFrom = (int) (compactThrough - snapshot.getLastIncludedIndex());
        if (keepFrom > 0 && keepFrom <= log.size()) {
            log.subList(0, keepFrom).clear();
            storage.saveLog(log);
        }
        snapshot = newSnapshot;
        System.out.printf("[快照] 节点 %s 生成快照至 index=%d/term=%d，保留日志 %d 条%n",
                id, compactThrough, boundary.getTerm(), log.size());
    }

    // ==================== 客户端提交 ====================

    public SubmitResult submit(String clientId, String requestId, String command) {
        if (!up) {
            return new SubmitResult(SubmitResult.Status.TIMEOUT, null, -1, null,
                    "节点处于宕机状态");
        }
        if (role != NodeRole.LEADER) {
            return new SubmitResult(SubmitResult.Status.NOT_LEADER, leaderHint, -1, null,
                    "当前节点不是领导者，term=" + currentTerm
                            + (leaderHint != null ? "，已知领导者=" + leaderHint : "，暂无已知领导者"));
        }
        return submitToLeader(clientId, requestId, command);
    }

    private SubmitResult submitToLeader(String clientId, String requestId, String command) {
        String key = dedupKey(clientId, requestId);
        String contentHash = hash(command);

        // 1) 已提交幂等表：直接复用首次结论
        IdempotencyRecord committed = idempotency.get(key);
        if (committed != null) {
            if (!committed.getCommandHash().equals(contentHash)) {
                System.out.printf("[幂等] 拒绝冲突请求 %s：相同标识携带不同内容（已存hash=%s 本次hash=%s）%n",
                        key, committed.getCommandHash(), contentHash);
                return new SubmitResult(SubmitResult.Status.IDEMPOTENCY_CONFLICT, id,
                        committed.getCommitIndex(), null,
                        "幂等标识 " + key + " 已提交过不同内容；首次提交的指令哈希="
                                + committed.getCommandHash() + "，本次=" + contentHash);
            }
            System.out.printf("[幂等] 命中重复请求 %s，返回首次结果（未二次应用），index=%d%n",
                    key, committed.getCommitIndex());
            return new SubmitResult(SubmitResult.Status.DUPLICATE, id,
                    committed.getCommitIndex(), committed.getResult(),
                    "重复提交：返回首次提交结论，未再次应用");
        }

        // 2) 在途（未提交）日志：同键同内容不重复追加，冲突立即拒绝
        for (LogEntry entry : log) {
            if (entry.getIndex() <= commitIndex) {
                continue;
            }
            if (entry.getClientId().equals(clientId) && entry.getRequestId().equals(requestId)) {
                if (!hash(entry.getCommand()).equals(contentHash)) {
                    return new SubmitResult(SubmitResult.Status.IDEMPOTENCY_CONFLICT, id,
                            entry.getIndex(), null,
                            "幂等标识 " + key + " 存在一条不同内容的在途日志（index="
                                    + entry.getIndex() + "），拒绝冲突提交");
                }
                if (!pendingByIndex.containsKey(entry.getIndex())) {
                    pendingByIndex.put(entry.getIndex(),
                            new Pending(clientId, requestId, currentTerm, entry.getIndex()));
                }
                return new SubmitResult(SubmitResult.Status.PENDING, id, entry.getIndex(), null,
                        "命中在途重复请求，复用 index=" + entry.getIndex() + " 的复制，不重复追加");
            }
        }

        // 3) 全新指令：追加、广播、等待多数派
        LogEntry entry = new LogEntry(currentTerm, lastLogIndex() + 1, clientId, requestId, command);
        appendLocal(entry);
        pendingByIndex.put(entry.getIndex(), new Pending(clientId, requestId, currentTerm, entry.getIndex()));
        System.out.printf("[提交] 领导 %s 接收 %s 指令='%s'，追加为 index=%d term=%d 并开始复制%n",
                id, key, command, entry.getIndex(), currentTerm);
        broadcastAppendEntries();
        return new SubmitResult(SubmitResult.Status.PENDING, id, entry.getIndex(), null,
                "已追加到领导日志，等待多数派复制");
    }


    /**
     * 查询某次追加（index）的最终结论。快照压缩后条目可能已不在日志中，
     * 故以提交下标 + 在途登记表为准，保证压缩不影响客户端结论。
     */
    SubmitResult pendingOutcome(long index, long submitTerm) {
        Pending pending = pendingByIndex.get(index);
        if (index <= commitIndex && pending != null) {
            IdempotencyRecord record = idempotency.get(dedupKey(pending.clientId, pending.requestId));
            if (record != null) {
                return new SubmitResult(SubmitResult.Status.COMMITTED, id, index, record.getResult(),
                        "已由多数派复制并应用");
            }
        }
        if (currentTerm != submitTerm || role != NodeRole.LEADER) {
            return new SubmitResult(SubmitResult.Status.TERM_CHANGED, leaderHint, -1, null,
                    "等待期间任期由 " + submitTerm + " 变为 " + currentTerm + "，请向新领导重试（幂等安全）");
        }
        return null;
    }

    // ==================== 工具 ====================

    private void persistHardState() {
        storage.saveHardState(new HardState(currentTerm, votedFor, commitIndex));
    }

    static String dedupKey(String clientId, String requestId) {
        return clientId + "#" + requestId;
    }

    static String hash(String content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(content.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 4; i++) {
                sb.append(String.format("%02x", bytes[i]));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
