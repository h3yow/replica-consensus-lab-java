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
 *   <li><b>落后副本追赶</b>：跟随者落后的起点已被压缩时，领导改用 InstallSnapshot
 *       整体推送已提交状态（KV + 幂等表），再补齐快照后条目；过期快照不覆盖本地更新状态。</li>
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
        this.log.addAll(storage.loadLog());
        // 安装快照是"先存快照、再截日志"：若崩溃于两步之间，磁盘日志里会残留
        // 已被快照覆盖的旧条目，恢复时以快照为准丢弃，保证 lastLogIndex 不回退
        log.removeIf(e -> e.getIndex() <= snapshot.getLastIncludedIndex());

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

    public long getLastLogIndex() { return lastLogIndex(); }
    public long getLastLogTerm() { return lastLogTerm(); }

    /** 当前快照边界（lastIncludedIndex）：已压缩进快照的已提交下标。 */
    public long getSnapshotIndex() { return snapshot.getLastIncludedIndex(); }

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

    /** 测试/排障用：查询某幂等标识在本副本上的首次提交结论（未提交过返回 null）。 */
    public String idempotentResult(String clientId, String requestId) {
        IdempotencyRecord record = idempotency.get(dedupKey(clientId, requestId));
        return record == null ? null : record.getResult();
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
            electionElapsed++;
            if (electionElapsed >= electionTimeoutTicks) {
                startElection();
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
            // 落后者的起点已被本机压缩：直接推送快照让其整体追赶
            sendInstallSnapshot(peer);
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

    /** 向落后副本推送当前快照（含 KV 状态与幂等表），让其跨越已压缩的历史。 */
    private void sendInstallSnapshot(String peer) {
        System.out.printf("[追赶] 节点 %s(领导) 发现 %s 的 nextIndex=%d 不晚于快照边界 %d，改发 InstallSnapshot（快照至=%d/term=%d）%n",
                id, peer, nextIndex.getOrDefault(peer, -1L),
                snapshot.getLastIncludedIndex(), snapshot.getLastIncludedIndex(),
                snapshot.getLastIncludedTerm());
        outbox.addLast(new Envelope(peer, new RaftMessage.InstallSnapshot(
                currentTerm, id, snapshot.getLastIncludedIndex(), snapshot.getLastIncludedTerm(),
                snapshot.getKv(), snapshot.getIdempotency())));
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
            return; // 对端任期更高时会通过 term 体现；此处无需重试，心跳会再触发
        }
        long snapshotIndex = snapshot.getLastIncludedIndex();
        // 跟随者可能自报更新的日志末尾（它手上已有比快照更新的内容），直接跳过去
        long peerMatch = Math.max(snapshotIndex, response.followerLastIndex());
        matchIndex.put(from, Math.max(matchIndex.getOrDefault(from, 0L), peerMatch));
        nextIndex.put(from, peerMatch + 1);
        System.out.printf("[追赶] 节点 %s(领导) 确认 %s 已安装快照，nextIndex 推进到 %d，继续用 AppendEntries 补齐后续条目%n",
                id, from, peerMatch + 1);
        advanceLeaderCommit();
        sendAppendEntries(from);
    }

    /**
     * 跟随者安装领导者的快照：
     * <ul>
     *   <li>任期过旧直接拒绝；</li>
     *   <li>快照不比本地已提交状态新（lastIncludedIndex &lt;= commitIndex）时<b>忽略内容</b>，
     *       只回成功确认——本地已提交/已应用的更新状态绝不能被旧快照覆盖；</li>
     *   <li>否则持久化快照、按边界对齐截断日志（边界条目任期一致则保留其后未提交后缀，
     *       否则整段丢弃）、用快照重置状态机与幂等表，并把 commitIndex/lastApplied
     *       推进到快照边界——快照内全是已提交内容，不存在"未提交先生效"。</li>
     * </ul>
     */
    private void handleInstallSnapshot(RaftMessage.InstallSnapshot request, String from) {
        if (request.term() < currentTerm) {
            outbox.addLast(new Envelope(from,
                    new RaftMessage.InstallSnapshotResponse(currentTerm, false, lastLogIndex())));
            return;
        }
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }
        role = NodeRole.FOLLOWER;
        leaderHint = request.leaderId();
        resetElectionTimer();

        long snapIndex = request.lastIncludedIndex();
        if (snapIndex <= commitIndex) {
            System.out.printf("[追赶] 节点 %s 收到 %s 的快照(至=%d)，但本地已提交到 %d，忽略旧快照内容，仅回确认%n",
                    id, from, snapIndex, commitIndex);
            outbox.addLast(new Envelope(from,
                    new RaftMessage.InstallSnapshotResponse(currentTerm, true, lastLogIndex())));
            return;
        }

        // 边界对齐：本地若在快照边界处有同任期条目，其后的未提交后缀仍然有效，予以保留
        List<LogEntry> retained = new ArrayList<>();
        LogEntry boundary = entryAt(snapIndex);
        if (boundary != null && boundary.getTerm() == request.lastIncludedTerm()) {
            for (LogEntry e : log) {
                if (e.getIndex() > snapIndex) {
                    retained.add(e);
                }
            }
        } else if (!retained.isEmpty() || boundary != null) {
            // 边界任期不一致：快照之后的本地条目与领导历史分歧，整段丢弃
        }

        Snapshot installed = new Snapshot(snapIndex, request.lastIncludedTerm(),
                request.kv(), request.idempotency());
        // 先落快照再截日志：崩溃于两者之间时，重启会以快照为准丢弃冗余旧日志，不丢已提交状态
        storage.saveSnapshot(installed);
        snapshot = installed;
        log.clear();
        log.addAll(retained);
        storage.saveLog(log);

        stateMachine.resetFrom(request.kv());
        idempotency.clear();
        idempotency.putAll(request.idempotency());
        commitIndex = snapIndex;
        lastApplied = snapIndex;
        persistHardState();

        System.out.printf("[追赶] 节点 %s 安装来自 %s 的快照：推进至 index=%d/term=%d，KV=%d 项，幂等表=%d 条，保留快照后日志 %d 条%n",
                id, from, snapIndex, request.lastIncludedTerm(),
                request.kv().size(), request.idempotency().size(), retained.size());
        outbox.addLast(new Envelope(from,
                new RaftMessage.InstallSnapshotResponse(currentTerm, true, lastLogIndex())));
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
            // 跟随者自报其日志末尾/冲突起点：nextIndex 允许退到快照边界以内，
            // 下一步 sendAppendEntries 会据此改发 InstallSnapshot（落后太多的唯一追赶通道）
            long next;
            if (response.conflictHint() > 0) {
                next = Math.min(response.conflictHint(), response.followerLastIndex() + 1);
            } else {
                next = response.followerLastIndex() + 1;
            }
            nextIndex.put(from, next);
            sendAppendEntries(from);
        }
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
