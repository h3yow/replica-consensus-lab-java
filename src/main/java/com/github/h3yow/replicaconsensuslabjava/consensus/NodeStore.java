package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.util.List;

/** 单个副本的持久化（WAL + 快照 + 元数据）抽象，默认使用本地文件实现。 */
public interface NodeStore {

    /** 保存投票元数据。 */
    void saveMeta(long currentTerm, String votedFor, long commitIndex);

    long loadCurrentTerm();

    String loadVotedFor();

    long loadCommitIndex();

    /** 追加/覆盖日志。 */
    void saveLog(List<LogEntry> entries);

    List<LogEntry> loadLog();

    /** 保存与加载快照。 */
    void saveSnapshot(Snapshot snapshot);

    Snapshot loadSnapshot();

    /** 清空全部持久化数据（测试用）。 */
    void reset();
}
