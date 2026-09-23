package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.Cluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.ConsensusProperties;
import com.github.h3yow.replicaconsensuslabjava.consensus.NodeRole;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 选举与任期推进测试：
 * 初始多数派可选出唯一领导者；隔离少数派后旧领导者在分区内不能继续提交，
 * 多数派分区选出更高任期的新领导者；网络恢复后集群收敛到唯一（更高任期）领导者，
 * 同一任期出现两个领导者的情形被任期检查拒绝。
 */
class ElectionTest {

    private static final Logger log = LoggerFactory.getLogger(ElectionTest.class);

    private Cluster newCluster(Path dir) {
        ConsensusProperties props = new ConsensusProperties();
        props.setDataDir(dir.toString());
        return new Cluster(java.util.List.of("n1", "n2", "n3"), dir.toString(), props);
    }

    @Test
    void electsSingleLeaderAndConvergesAfterPartition() {
        Path dir = Path.of("target", "consensus-test", "election-" + System.nanoTime());
        Cluster cluster = newCluster(dir);
        cluster.resetAllStores();

        // 1) 初始集群应选出唯一领导者。
        RaftNode first = cluster.awaitLeader(5);
        assertNotNull(first, "初始集群应在多数派支持下选出领导者");
        long firstTerm = first.currentTerm();
        log.info(">>> 初始领导者={} term={}", first.id(), firstTerm);
        assertEquals(1, cluster.nodes().stream().filter(n -> n.role() == NodeRole.LEADER).count(),
                "任一时刻只能有一个领导者");

        // 2) 把旧领导者 n1 单独隔离（1 票少数派），n2/n3 构成多数派。
        cluster.partition(first.id());
        log.info(">>> 已隔离旧领导者 {}，多数派分区应产生更高任期的新领导者", first.id());
        RaftNode majorityLeader = null;
        for (int i = 0; i < 5; i++) {
            cluster.tickElection();
            cluster.tickReplication();
            RaftNode candidate = cluster.node("n2").role() == NodeRole.LEADER ? cluster.node("n2")
                    : cluster.node("n3").role() == NodeRole.LEADER ? cluster.node("n3") : null;
            if (candidate != null) {
                majorityLeader = candidate;
                break;
            }
        }
        assertNotNull(majorityLeader, "多数派分区必须能独立选出新领导者");
        assertTrue(majorityLeader.currentTerm() > firstTerm,
                "新领导者任期必须高于旧领导者: " + majorityLeader.currentTerm() + " > " + firstTerm);
        log.info(">>> 多数派新领导者={} term={}（旧领导者 n1 term={} 被隔离）",
                majorityLeader.id(), majorityLeader.currentTerm(), firstTerm);

        // 3) 旧领导者在少数派分区里提交应失败（无法获得多数派 ack）。
        var staleResult = first.submit("stale-key", "SET x 999");
        log.info(">>> 隔离区旧领导者提交结论: success={} reason={}", staleResult.success(), staleResult.reason());
        assertEquals(false, staleResult.success());
        assertEquals(com.github.h3yow.replicaconsensuslabjava.consensus.CommitReason.REPLICATION_FAILED,
                staleResult.reason(), "少数派分区的旧领导者无法达成多数派复制");

        // 4) 网络恢复：选举 tick 中旧领导者拉票遇到更高任期即下台，随后心跳收敛。
        cluster.heal();
        for (int i = 0; i < 3; i++) {
            cluster.tickElection();
            cluster.tickReplication();
        }
        assertEquals(NodeRole.FOLLOWER, first.role(), "分区恢复后旧领导者必须退回跟随者");
        RaftNode finalLeader = cluster.leader();
        assertNotNull(finalLeader, "恢复后必须重新收敛出唯一领导者");
        assertEquals(majorityLeader.id(), finalLeader.id(), "恢复后唯一领导者应为多数派分区的领导者");
        assertEquals(1, cluster.nodes().stream().filter(n -> n.role() == NodeRole.LEADER).count(),
                "恢复后同一任期内不得存在两个领导者");
        // 少数派的未提交记录绝不能生效。
        for (RaftNode n : cluster.nodes()) {
            assertNull(n.read("x"), "节点 " + n.id() + " 上未提交记录不得生效");
        }
        log.info(">>> 网络恢复后集群唯一领导者={} term={}，各节点状态一致",
                finalLeader.id(), finalLeader.currentTerm());
    }
}
