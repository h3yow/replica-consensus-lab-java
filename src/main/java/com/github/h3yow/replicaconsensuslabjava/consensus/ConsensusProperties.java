package com.github.h3yow.replicaconsensuslabjava.consensus;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 一致性组件可配置项（前缀 consensus.*）。 */
@ConfigurationProperties(prefix = "consensus")
public class ConsensusProperties {

    /** 集群副本 ID 列表，决定多数派大小。 */
    private java.util.List<String> nodeIds = new java.util.ArrayList<>(java.util.List.of("n1", "n2", "n3"));

    /** 持久化数据目录（WAL、快照、元数据）。 */
    private String dataDir = "target/consensus-data";

    /** 日志超过该长度后触发快照压缩。 */
    private int snapshotThreshold = 5;

    /** 每个选举 tick 的步进次数。 */
    private int electionTicks = 1;

    public java.util.List<String> getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(java.util.List<String> nodeIds) {
        this.nodeIds = nodeIds;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public int getSnapshotThreshold() {
        return snapshotThreshold;
    }

    public void setSnapshotThreshold(int snapshotThreshold) {
        this.snapshotThreshold = snapshotThreshold;
    }

    public int getElectionTicks() {
        return electionTicks;
    }

    public void setElectionTicks(int electionTicks) {
        this.electionTicks = electionTicks;
    }
}
