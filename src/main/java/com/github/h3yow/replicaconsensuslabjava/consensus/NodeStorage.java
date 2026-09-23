package com.github.h3yow.replicaconsensuslabjava.consensus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 节点持久化存储（用内存字节数组模拟本机磁盘，不依赖任何外部中间件）。
 *
 * <p>持久化三类数据：</p>
 * <ol>
 *   <li>{@link HardState}：当前任期、投票对象、已提交下标——响应投票与推进提交前必须落盘；</li>
 *   <li>{@link Snapshot}：已压缩到快照内的已提交状态；</li>
 *   <li>日志：快照之后保留的条目（含未提交条目，重启后仍可见但不会生效）。</li>
 * </ol>
 *
 * <p>节点 {@code crash()} 只销毁内存中的节点对象状态，{@code NodeStorage}（"磁盘"）
 * 被保留下来并传给重启后的新节点，从而验证重启回放。</p>
 */
public class NodeStorage {

    private final String nodeId;
    private byte[] hardStateBytes;
    private byte[] snapshotBytes;
    private byte[] logBytes;

    public NodeStorage(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getNodeId() {
        return nodeId;
    }

    // ---------- 硬状态 ----------

    public synchronized HardState loadHardState() {
        if (hardStateBytes == null) {
            return HardState.initial();
        }
        return deserialize(hardStateBytes, HardState.class);
    }

    public synchronized void saveHardState(HardState state) {
        this.hardStateBytes = serialize(state);
    }

    // ---------- 快照 ----------

    public synchronized Snapshot loadSnapshot() {
        if (snapshotBytes == null) {
            return Snapshot.empty();
        }
        return deserialize(snapshotBytes, Snapshot.class);
    }

    public synchronized void saveSnapshot(Snapshot snapshot) {
        this.snapshotBytes = serialize(snapshot);
    }

    // ---------- 日志 ----------

    @SuppressWarnings("unchecked")
    public synchronized List<LogEntry> loadLog() {
        if (logBytes == null) {
            return new ArrayList<>();
        }
        return deserialize(logBytes, ArrayList.class);
    }

    public synchronized void saveLog(List<LogEntry> log) {
        this.logBytes = serialize(new ArrayList<>(log));
    }

    // ---------- 序列化辅助 ----------

    private static byte[] serialize(Object object) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("持久化序列化失败", e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T deserialize(byte[] bytes, Class<T> type) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return (T) ois.readObject();
        } catch (IOException e) {
            throw new UncheckedIOException("持久化反序列化失败", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("持久化数据类型缺失", e);
        }
    }
}
