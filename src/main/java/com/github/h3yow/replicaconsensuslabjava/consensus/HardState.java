package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * 硬状态：必须在响应投票/提交前持久化的易失性最小集合。
 */
public class HardState implements java.io.Serializable {
    private final long currentTerm;
    private final String votedFor;
    private final long commitIndex;

    public HardState(long currentTerm, String votedFor, long commitIndex) {
        this.currentTerm = currentTerm;
        this.votedFor = votedFor;
        this.commitIndex = commitIndex;
    }

    public long getCurrentTerm() { return currentTerm; }
    public String getVotedFor() { return votedFor; }
    public long getCommitIndex() { return commitIndex; }

    public static HardState initial() {
        return new HardState(0, null, 0);
    }
}
