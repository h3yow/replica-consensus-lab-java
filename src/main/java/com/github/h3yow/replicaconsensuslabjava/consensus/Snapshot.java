package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.Map;

/**
 * 快照：截至 lastIncludedIndex/lastIncludedTerm 已提交状态的持久化副本，
 * 供副本重启后快速恢复。
 */
public class Snapshot implements java.io.Serializable {
    private final long lastIncludedIndex;
    private final long lastIncludedTerm;
    private final Map<String, String> kv;
    private final Map<String, IdempotencyRecord> idempotency;

    public Snapshot(long lastIncludedIndex, long lastIncludedTerm,
                    Map<String, String> kv,
                    Map<String, IdempotencyRecord> idempotency) {
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.kv = kv;
        this.idempotency = idempotency;
    }

    public long getLastIncludedIndex() { return lastIncludedIndex; }
    public long getLastIncludedTerm() { return lastIncludedTerm; }
    public Map<String, String> getKv() { return kv; }
    public Map<String, IdempotencyRecord> getIdempotency() { return idempotency; }

    public static Snapshot empty() {
        return new Snapshot(0, 0, Map.of(), Map.of());
    }
}
