package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.List;

/**
 * 节点之间交换的 RPC 消息（请求投票 / 追加条目），含对应的响应。
 * 本实现进程内直接投递，不经过真实网络。
 */
public final class RaftMessage {

    private RaftMessage() {
    }

    public record RequestVote(long term, String candidateId,
                              long lastLogIndex, long lastLogTerm) implements java.io.Serializable {
    }

    public record RequestVoteResponse(long term, boolean voteGranted) implements java.io.Serializable {
    }

    public record AppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                                List<LogEntry> entries, long leaderCommit) implements java.io.Serializable {
    }

    public record AppendEntriesResponse(long term, boolean success, long conflictHint,
                                        long followerLastIndex) implements java.io.Serializable {
    }
}
