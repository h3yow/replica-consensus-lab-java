package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.List;

/** Raft RPC 消息定义：投票与日志复制（追加条目，必要时附带快照安装）。 */
public final class RaftMessages {

    private RaftMessages() {
    }

    /** RequestVote 请求。 */
    public record VoteRequest(String candidateId, long term, long lastLogIndex, long lastLogTerm) {
    }

    /** RequestVote 响应。 */
    public record VoteResponse(String fromId, long term, boolean voteGranted) {
    }

    /**
     * AppendEntries 请求（同时承担心跳与 InstallSnapshot 职责）。
     *
     * @param snapshot            当跟随者落后于领导者快照点时携带的快照，否则为 null
     * @param prevLogIndex        新条目前置日志的索引
     * @param prevLogTerm         前置日志的任期
     * @param entries             待追加日志（可能为空表示心跳）
     * @param leaderCommit        领导者已知提交位置
     */
    public record AppendRequest(String leaderId,
                                long term,
                                Snapshot snapshot,
                                long prevLogIndex,
                                long prevLogTerm,
                                List<LogEntry> entries,
                                long leaderCommit) {
    }

    /** AppendEntries 响应。 */
    public record AppendResponse(String fromId, long term, boolean success, long matchIndex) {
    }
}
