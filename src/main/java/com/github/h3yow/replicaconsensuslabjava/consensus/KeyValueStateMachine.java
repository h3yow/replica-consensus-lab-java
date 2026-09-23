package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 极简 KV 状态机：所有节点对相同日志序列应用后必然得到相同状态。
 *
 * <p>支持的指令语法：</p>
 * <ul>
 *   <li>{@code SET key value}：写入（value 可含空格）；</li>
 *   <li>{@code DELETE key}：删除；</li>
 *   <li>{@code NOP}：空操作（日志连续性占位）。</li>
 * </ul>
 * 应用结果是确定的字符串，便于幂等重放时向客户端返回与首次完全一致的结论。
 */
public class KeyValueStateMachine {

    private final Map<String, String> data = new LinkedHashMap<>();

    /** 用快照中的 KV 视图重置内存状态（重启恢复时调用）。 */
    public void resetFrom(Map<String, String> snapshot) {
        data.clear();
        data.putAll(snapshot);
    }

    /**
     * 应用一条已提交日志，返回可复现的结果字符串。
     */
    public String apply(LogEntry entry) {
        String command = entry.getCommand();
        String[] parts = command.split(" ", 3);
        String op = parts[0];
        return switch (op) {
            case "SET" -> {
                if (parts.length < 3) {
                    throw new IllegalArgumentException("SET 指令格式应为: SET key value");
                }
                String existed = data.put(parts[1], parts[2]);
                yield "OK:" + parts[1] + "=" + parts[2]
                        + (existed == null ? "(new)" : "(updated)");
            }
            case "DELETE" -> {
                if (parts.length < 2) {
                    throw new IllegalArgumentException("DELETE 指令格式应为: DELETE key");
                }
                String removed = data.remove(parts[1]);
                yield removed == null ? "MISS:" + parts[1] : "DELETED:" + parts[1];
            }
            case "NOP" -> "NOP";
            default -> throw new IllegalArgumentException("未知指令: " + op);
        };
    }

    public String get(String key) {
        return data.get(key);
    }

    /** 返回当前状态的有序快照副本。 */
    public Map<String, String> snapshot() {
        return new LinkedHashMap<>(data);
    }
}
