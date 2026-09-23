package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.Map;

/**
 * 状态机快照及其覆盖到的日志位置。
 *
 * @param lastIncludedIndex 快照覆盖的最后一条日志索引
 * @param lastIncludedTerm  该日志条目的任期
 * @param data              键值状态机数据
 * @param appliedCommands   幂等键 -> 首次提交的指令内容（重启后仍可检测内容冲突）
 * @param appliedResults    幂等键 -> 首次执行结果（保证重启后重复提交仍返回一致结果）
 */
public record Snapshot(long lastIncludedIndex,
                       long lastIncludedTerm,
                       Map<String, String> data,
                       Map<String, String> appliedCommands,
                       Map<String, String> appliedResults) {
}
