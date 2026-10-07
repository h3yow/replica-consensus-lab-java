package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.List;
import java.util.Map;

/**
 * 节点之间交换的 RPC 消息（请求投票 / 追加条目 / 安装快照），含对应的响应。
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

    /**
     * 安装快照：当跟随者落后的起点已被领导者压缩（nextIndex <= 快照边界）时，
     * 领导者直接推送截至 lastIncludedIndex 的已提交状态，帮助落后副本整体追赶。
     */
    public record InstallSnapshot(long term, String leaderId,
                                  long lastIncludedIndex, long lastIncludedTerm,
                                  Map<String, String> kv,
                                  Map<String, IdempotencyRecord> idempotency) implements java.io.Serializable {
    }

    public record InstallSnapshotResponse(long term, boolean success,
                                          long followerLastIndex) implements java.io.Serializable {
    }
}
