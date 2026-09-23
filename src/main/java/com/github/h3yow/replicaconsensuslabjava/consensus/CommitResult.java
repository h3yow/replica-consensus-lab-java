package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * 客户端提交指令的结论。
 *
 * @param success       是否成功生效（或为被抑制的重复提交）
 * @param reason        可区分的判定原因
 * @param leaderId      当时认知的领导者（可能为 null）
 * @param term          判定时节点所处任期
 * @param idempotencyKey 幂等键
 * @param value         成功/重复时返回的值
 * @param message       说明信息
 */
public record CommitResult(boolean success,
                           CommitReason reason,
                           String leaderId,
                           long term,
                           String idempotencyKey,
                           String value,
                           String message) {
}
