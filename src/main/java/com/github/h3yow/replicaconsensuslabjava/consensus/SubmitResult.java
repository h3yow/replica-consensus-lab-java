package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * 指令提交结论。状态可区分，reason 给出可解释原因。
 */
public record SubmitResult(Status status, String leaderId, long commitIndex,
                           String value, String reason) {

    public enum Status {
        /** 首次提交成功并完成应用 */
        COMMITTED,
        /** 指令已追加到领导日志，正在等待多数派复制（内部中间态） */
        PENDING,
        /** 重复请求：返回与首次完全一致的结论，未二次应用 */
        DUPLICATE,
        /** 同一幂等标识携带了不同内容，被拒绝 */
        IDEMPOTENCY_CONFLICT,
        /** 当前节点不是领导者（leaderId 可能为最新已知领导者） */
        NOT_LEADER,
        /** 超时未达成多数派（如分区期间） */
        TIMEOUT,
        /** 领导者已切换到更高任期，本次结果不可信 */
        TERM_CHANGED
    }

    public static SubmitResult committed(long commitIndex, String value) {
        return new SubmitResult(Status.COMMITTED, null, commitIndex, value, null);
    }
}
