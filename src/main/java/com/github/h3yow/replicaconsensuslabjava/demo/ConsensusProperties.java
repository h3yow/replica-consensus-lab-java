package com.github.h3yow.replicaconsensuslabjava.demo;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 演示组件的可配置项（application.properties 中以 consensus.demo.* 配置）。
 */
@Component
@ConfigurationProperties(prefix = "consensus.demo")
public class ConsensusProperties {

    /** 启动时是否运行进程内一致性演示。 */
    private boolean enabled = true;
    /** 集群节点数（建议 3 或 5）。 */
    private int nodes = 5;
    /** 选举超时下界（tick）。 */
    private int electionTimeoutMin = 8;
    /** 选举超时随机抖动宽度（tick）。 */
    private int electionTimeoutRange = 8;
    /** 心跳间隔（tick）。 */
    private int heartbeatInterval = 3;
    /** 快照后保留日志条数阈值。 */
    private long snapshotThreshold = 6;
    /** 每个等待操作的最大 tick 预算。 */
    private int tickBudget = 120;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getNodes() { return nodes; }
    public void setNodes(int nodes) { this.nodes = nodes; }
    public int getElectionTimeoutMin() { return electionTimeoutMin; }
    public void setElectionTimeoutMin(int v) { this.electionTimeoutMin = v; }
    public int getElectionTimeoutRange() { return electionTimeoutRange; }
    public void setElectionTimeoutRange(int v) { this.electionTimeoutRange = v; }
    public int getHeartbeatInterval() { return heartbeatInterval; }
    public void setHeartbeatInterval(int v) { this.heartbeatInterval = v; }
    public long getSnapshotThreshold() { return snapshotThreshold; }
    public void setSnapshotThreshold(long v) { this.snapshotThreshold = v; }
    public int getTickBudget() { return tickBudget; }
    public void setTickBudget(int tickBudget) { this.tickBudget = tickBudget; }
}
