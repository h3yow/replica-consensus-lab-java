package com.github.h3yow.replicaconsensuslabjava.consensus;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 基于本地 JSON 文件的 {@link NodeStore} 实现。
 *
 * <p>每个副本一个目录，包含：
 * <ul>
 *     <li>{@code meta.json}：当前任期、投过票的候选者、已知提交位置</li>
 *     <li>{@code log.json}：快照之后保留的全部日志条目（WAL）</li>
 *     <li>{@code snapshot.json}：最近一次状态机快照（可不存在）</li>
 * </ul>
 * 崩溃后重启时，先加载快照再重放日志即可恢复已提交状态。
 */
public class FileNodeStore implements NodeStore {

    private static final String META_FILE = "meta.json";
    private static final String LOG_FILE = "log.json";
    private static final String SNAPSHOT_FILE = "snapshot.json";

    private final Path dir;
    private final ObjectMapper mapper = new ObjectMapper();

    public FileNodeStore(String dataDir, String nodeId) {
        this.dir = Path.of(dataDir, nodeId);
        try {
            Files.createDirectories(dir);
            if (Files.notExists(dir.resolve(META_FILE))) {
                Files.writeString(dir.resolve(META_FILE),
                        "{\"currentTerm\":0,\"votedFor\":null,\"commitIndex\":0}");
            }
            if (Files.notExists(dir.resolve(LOG_FILE))) {
                Files.writeString(dir.resolve(LOG_FILE), "[]");
            }
        } catch (IOException e) {
            throw new UncheckedIOException("初始化节点存储失败: " + dir, e);
        }
    }

    @Override
    public void saveMeta(long currentTerm, String votedFor, long commitIndex) {
        Meta meta = new Meta(currentTerm, votedFor, commitIndex);
        writeJson(dir.resolve(META_FILE), meta);
    }

    @Override
    public long loadCurrentTerm() {
        return readMeta().currentTerm();
    }

    @Override
    public String loadVotedFor() {
        return readMeta().votedFor();
    }

    @Override
    public long loadCommitIndex() {
        return readMeta().commitIndex();
    }

    @Override
    public void saveLog(List<LogEntry> entries) {
        writeJson(dir.resolve(LOG_FILE), entries);
    }

    @Override
    public List<LogEntry> loadLog() {
        Path path = dir.resolve(LOG_FILE);
        if (Files.notExists(path)) {
            return List.of();
        }
        try {
            List<LogEntry> entries = mapper.readValue(path.toFile(), new TypeReference<List<LogEntry>>() {
            });
            return entries == null ? List.of() : entries;
        } catch (IOException e) {
            throw new UncheckedIOException("加载日志失败: " + path, e);
        }
    }

    @Override
    public void saveSnapshot(Snapshot snapshot) {
        writeJson(dir.resolve(SNAPSHOT_FILE), snapshot);
    }

    @Override
    public Snapshot loadSnapshot() {
        Path path = dir.resolve(SNAPSHOT_FILE);
        if (Files.notExists(path)) {
            return null;
        }
        try {
            return mapper.readValue(path.toFile(), Snapshot.class);
        } catch (IOException e) {
            throw new UncheckedIOException("加载快照失败: " + path, e);
        }
    }

    @Override
    public void reset() {
        try {
            Files.deleteIfExists(dir.resolve(META_FILE));
            Files.deleteIfExists(dir.resolve(LOG_FILE));
            Files.deleteIfExists(dir.resolve(SNAPSHOT_FILE));
            Files.writeString(dir.resolve(META_FILE),
                    "{\"currentTerm\":0,\"votedFor\":null,\"commitIndex\":0}");
            Files.writeString(dir.resolve(LOG_FILE), "[]");
        } catch (IOException e) {
            throw new UncheckedIOException("重置节点存储失败: " + dir, e);
        }
    }

    private Meta readMeta() {
        try {
            Meta meta = mapper.readValue(dir.resolve(META_FILE).toFile(), Meta.class);
            return meta == null ? new Meta(0, null, 0) : meta;
        } catch (IOException e) {
            throw new UncheckedIOException("加载元数据失败: " + dir, e);
        }
    }

    private void writeJson(Path path, Object value) {
        try {
            mapper.writerWithDefaultPrettyPrinter().writeValue(path.toFile(), value);
        } catch (IOException e) {
            throw new UncheckedIOException("写入失败: " + path, e);
        }
    }

    /** 持久化的投票与提交元数据。 */
    private record Meta(long currentTerm, String votedFor, long commitIndex) {
    }
}
