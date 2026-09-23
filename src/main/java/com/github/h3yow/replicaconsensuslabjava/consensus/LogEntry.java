package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * Raft 日志条目。
 *
 * @param term        条目所属任期
 * @param index       条目在日志中的索引（从 1 开始）
 * @param idempotencyKey 客户端提供的幂等标识（去重键）
 * @param command     指令内容
 * @param committed   是否已提交（仅用于本地展示，权威提交位置以 commitIndex 为准）
 */
public record LogEntry(long term, long index, String idempotencyKey, String command, boolean committed) {
    public LogEntry withCommitted(boolean value) {
        return new LogEntry(term, index, idempotencyKey, command, value);
    }
}
