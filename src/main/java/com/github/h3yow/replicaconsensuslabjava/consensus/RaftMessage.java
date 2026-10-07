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
     * 落后追赶：领导者发现跟随者的下一同步位置已落在本地快照边界之内
     * （其缺失历史已被压缩）时，改用 InstallSnapshot 直接把快照状态推过去。
     *
     * <p>进程内模拟不分片，整条快照（边界 index/term、KV 状态、幂等表）一次送达；
     * leaderCommit 随快照一并下发，供跟随者把已提交进度推进到快照边界。</p>
     */
    public record InstallSnapshot(long term, String leaderId,
                                  long lastIncludedIndex, long lastIncludedTerm,
                                  Map<String, String> kv,
                                  Map<String, IdempotencyRecord> idempotency,
                                  long leaderCommit) implements java.io.Serializable {
    }

    /**
     * 快照安装结果：{@code success=false} 表示跟随者基于任期/新旧判断拒绝了该快照，
     * 领导者不得据此推进该跟随者的匹配进度。
     */
    public record InstallSnapshotResponse(long term, boolean success,
                                          long lastIncludedIndex) implements java.io.Serializable {
    }
}
