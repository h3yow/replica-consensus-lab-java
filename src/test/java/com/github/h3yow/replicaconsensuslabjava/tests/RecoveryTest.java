package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.Cluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.CommitReason;
import com.github.h3yow.replicaconsensuslabjava.consensus.ConsensusProperties;
import com.github.h3yow.replicaconsensuslabjava.consensus.LogEntry;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 故障恢复与日志一致性测试：
 * 已提交状态在副本崩溃重启后可基于快照 + 操作历史恢复；未提交记录重启后不生效；
 * 落后/分歧副本在重新连通后被领导者日志修复；任意时刻各副本已提交状态一致。
 */
class RecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(RecoveryTest.class);

    private Cluster newCluster(String name, int snapshotThreshold) {
        ConsensusProperties props = new ConsensusProperties();
        props.setDataDir("target/consensus-test/" + name + "-" + System.nanoTime());
        props.setSnapshotThreshold(snapshotThreshold);
        Cluster cluster = new Cluster(List.of("n1", "n2", "n3"), props.getDataDir(), props);
        cluster.resetAllStores();
        return cluster;
    }

    @Test
    void restartReplaysCommittedStateFromSnapshotAndLog() {
        // 阈值设为 3：提交多条后必然触发快照压缩，重启需同时依赖快照与剩余日志。
        Cluster cluster = newCluster("recovery", 3);
        RaftNode leader = cluster.awaitLeader(5);
        assertNotNull(leader);

        for (int i = 1; i <= 7; i++) {
            var r = leader.submit("k" + i, "SET key" + i + " v" + i);
            assertTrue(r.success(), "第 " + i + " 条应提交成功: " + r.reason());
            cluster.settle(1);
        }
        long commitBefore = leader.commitIndex();
        log.info(">>> 提交 7 条后 commitIndex={}，领导者保留日志 {} 条（其余已被快照压缩）",
                commitBefore, leader.logView().size());
        assertTrue(leader.logView().size() <= 4, "应已触发快照并压缩日志前缀");

        // 各副本已提交状态一致。
        for (RaftNode n : cluster.nodes()) {
            assertEquals("v7", n.read("key7"));
            assertEquals("v1", n.read("key1"));
        }

        // 让一个跟随者崩溃后重启：应从快照 + 日志恢复全部已提交状态。
        RaftNode follower = cluster.nodes().stream()
                .filter(n -> !n.id().equals(leader.id())).findFirst().orElseThrow();
        follower.crash();
        follower.restart();
        log.info(">>> 跟随者 {} 重启后 commitIndex={} state={}",
                follower.id(), follower.commitIndex(), follower.committedState());
        assertEquals(commitBefore, follower.commitIndex(), "重启后提交位置应恢复");
        for (int i = 1; i <= 7; i++) {
            assertEquals("v" + i, follower.read("key" + i),
                    "快照/日志恢复后 key" + i + " 必须一致");
        }
    }

    @Test
    void uncommittedEntriesDoNotTakeEffectAfterRestart() {
        Cluster cluster = newCluster("uncommitted", 100);
        RaftNode leader = cluster.awaitLeader(5);

        // 正常提交一条。
        var ok = leader.submit("c1", "SET a 1");
        assertTrue(ok.success());
        cluster.settle(1);

        // 隔离领导者，制造一条无法达成多数派的未提交记录。
        cluster.partition(leader.id());
        var pending = leader.submit("c2", "SET b 2");
        assertEquals(CommitReason.REPLICATION_FAILED, pending.reason());
        long pendingIndex = leader.lastLogIndex();
        log.info(">>> 未提交记录 idx={} 仅存在于被隔离领导者日志中", pendingIndex);

        // 多数派分区（n2/n3）在旧领导者缺席期间选出更高任期的新领导者。
        RaftNode majorityLeader = null;
        for (int i = 0; i < 5; i++) {
            cluster.tickElection();
            RaftNode candidate = cluster.leader();
            if (candidate != null && !candidate.id().equals(leader.id())) {
                majorityLeader = candidate;
                break;
            }
        }
        assertNotNull(majorityLeader, "多数派分区必须选出自己的更高任期领导者");
        assertTrue(majorityLeader.currentTerm() > 1);
        log.info(">>> 多数派在分区期间选出新领导者 {} term={}",
                majorityLeader.id(), majorityLeader.currentTerm());

        // 崩溃并重启旧领导者：未提交记录仍在日志中（可被新领导者覆盖），但绝不能应用到状态机。
        leader.crash();
        leader.restart();
        assertNull(leader.read("b"), "重启后未提交记录不得生效");
        assertEquals("1", leader.read("a"), "已提交状态必须保留");
        log.info(">>> 重启后 b={}（期望 null），a={}（期望 1）", leader.read("b"), leader.read("a"));

        // 恢复网络，更高任期的新领导者心跳检测冲突并截断旧日志，未提交记录被清除。
        cluster.heal();
        for (int i = 0; i < 5; i++) {
            cluster.tickReplication();
        }
        for (RaftNode n : cluster.nodes()) {
            assertNull(n.read("b"), "节点 " + n.id() + " 上未提交记录最终也不得生效");
            assertEquals("1", n.read("a"));
        }
        // 旧领导者的日志分歧应被修复为与新领导者一致（不存在 b=2 的条目）。
        for (LogEntry e : leader.logView()) {
            assertTrue(!"SET b 2".equals(e.command()), "分歧日志必须被截断修复: " + e);
        }
        log.info(">>> 分歧修复完成，新领导者={}，全集群 b 均为 null", majorityLeader.id());
    }

    @Test
    void laggingReplicaCatchesUpAfterRejoin() {
        Cluster cluster = newCluster("catchup", 3);
        RaftNode leader = cluster.awaitLeader(5);

        // 让一个跟随者停机，其余两个多数派继续提交。
        RaftNode down = cluster.nodes().stream()
                .filter(n -> !n.id().equals(leader.id())).findFirst().orElseThrow();
        down.crash();
        for (int i = 1; i <= 6; i++) {
            var r = leader.submit("j" + i, "SET j" + i + " " + i);
            assertTrue(r.success());
            cluster.settle(1);
        }
        log.info(">>> {} 停机期间多数派已提交 6 条并可能生成快照", down.id());

        // 重新启动并恢复网络（crash 不造成分区，重启后即可连通）。
        down.restart();
        for (int i = 0; i < 5; i++) {
            cluster.tickReplication();
        }
        for (int i = 1; i <= 6; i++) {
            assertEquals(String.valueOf(i), down.read("j" + i),
                    "掉队副本经快照/日志追赶后 j" + i + " 必须一致");
        }
        assertEquals(leader.commitIndex(), down.commitIndex(), "追赶后提交位置应一致");
        log.info(">>> 掉队副本 {} 已追平至 commitIndex={}", down.id(), down.commitIndex());
    }
}
