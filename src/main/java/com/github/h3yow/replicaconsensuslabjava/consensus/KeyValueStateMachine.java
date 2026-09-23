package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 基于键值存储的确定性状态机。
 *
 * <p>支持的指令：
 * <ul>
 *     <li>{@code SET key value}：写入键值，返回 {@code OK:value}</li>
 *     <li>{@code DELETE key}：删除键，返回 {@code DELETED:key}</li>
 * </ul>
 *
 * <p>状态机同时维护两张幂等表：{@code appliedCommands}（幂等键 -> 首次指令内容，
 * 用于检测“同键不同内容”）与 {@code appliedResults}（幂等键 -> 首次结果，
 * 用于重复提交时原样返回）。两张表随快照持久化，副本重启后语义保持不变。
 */
public class KeyValueStateMachine {

    private final Map<String, String> data = new LinkedHashMap<>();
    private final Map<String, String> appliedCommands = new LinkedHashMap<>();
    private final Map<String, String> appliedResults = new LinkedHashMap<>();
    private long lastAppliedIndex;

    public KeyValueStateMachine() {
    }

    /** 应用一条已提交指令（同一幂等键绝不二次应用），返回状态机产生的结果值。 */
    public String apply(LogEntry entry) {
        if (appliedCommands.containsKey(entry.idempotencyKey())) {
            return appliedResults.get(entry.idempotencyKey());
        }
        String result = execute(entry.command());
        appliedCommands.put(entry.idempotencyKey(), entry.command());
        appliedResults.put(entry.idempotencyKey(), result);
        lastAppliedIndex = Math.max(lastAppliedIndex, entry.index());
        return result;
    }

    private String execute(String command) {
        String[] parts = command.split("\\s+", 3);
        String op = parts[0].toUpperCase();
        return switch (op) {
            case "SET" -> {
                if (parts.length < 3) {
                    throw new IllegalArgumentException("SET 需要参数: SET key value");
                }
                data.put(parts[1], parts[2]);
                yield "OK:" + parts[2];
            }
            case "DELETE" -> {
                if (parts.length < 2) {
                    throw new IllegalArgumentException("DELETE 需要参数: DELETE key");
                }
                data.remove(parts[1]);
                yield "DELETED:" + parts[1];
            }
            default -> throw new IllegalArgumentException("不支持的指令: " + command);
        };
    }

    /** 幂等键此前是否已应用。 */
    public boolean isApplied(String idempotencyKey) {
        return appliedCommands.containsKey(idempotencyKey);
    }

    /** 幂等键首次应用时的结果。 */
    public String appliedResult(String idempotencyKey) {
        return appliedResults.get(idempotencyKey);
    }

    /**
     * 判定幂等键是否携带了与首次不同的指令内容。
     *
     * @return true 表示该键已存在且内容不一致（必须拒绝）
     */
    public boolean conflictsWith(String idempotencyKey, String command) {
        String first = appliedCommands.get(idempotencyKey);
        return first != null && !first.equals(command);
    }

    /** 生成覆盖到给定日志位置的快照。 */
    public Snapshot buildSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        return new Snapshot(
                lastIncludedIndex,
                lastIncludedTerm,
                new LinkedHashMap<>(data),
                new LinkedHashMap<>(appliedCommands),
                new LinkedHashMap<>(appliedResults));
    }

    /** 用快照重置状态机（恢复时使用）。 */
    public void restoreSnapshot(Snapshot snapshot) {
        data.clear();
        appliedCommands.clear();
        appliedResults.clear();
        if (snapshot != null) {
            data.putAll(snapshot.data());
            appliedCommands.putAll(snapshot.appliedCommands());
            appliedResults.putAll(snapshot.appliedResults());
            lastAppliedIndex = snapshot.lastIncludedIndex();
        }
    }

    /** 当前已提交键值状态的不可变视图。 */
    public Map<String, String> data() {
        return Map.copyOf(data);
    }

    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }
}
