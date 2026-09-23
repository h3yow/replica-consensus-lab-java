package com.github.h3yow.replicaconsensuslabjava.consensus;

/** 指令提交判定结果的可区分原因。 */
public enum CommitReason {
    /** 首次提交成功，指令已应用。 */
    APPLIED,
    /** 幂等重试：与首次内容一致，直接返回首次结果，不重复应用。 */
    DUPLICATE_SUPPRESSED,
    /** 同一幂等键携带了不同指令内容，拒绝。 */
    IDEMPOTENCY_KEY_CONFLICT,
    /** 当前节点不是领导者，无法处理提交。 */
    NOT_LEADER,
    /** 未能复制到多数派，提交失败（指令未生效）。 */
    REPLICATION_FAILED
}
