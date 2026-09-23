package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.Cluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.CommitReason;
import com.github.h3yow.replicaconsensuslabjava.consensus.CommitResult;
import com.github.h3yow.replicaconsensuslabjava.consensus.ConsensusProperties;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等提交测试：
 * 首次提交返回 APPLIED；相同键相同内容重试返回相同结果且只应用一次（DUPLICATE_SUPPRESSED）；
 * 相同键不同内容被拒绝并给出可区分原因（IDEMPOTENCY_KEY_CONFLICT）；
 * 向非领导者提交得到 NOT_LEADER；重试与分区场景下结论保持一致。
 */
class IdempotencyTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyTest.class);

    private Cluster newCluster(String name) {
        ConsensusProperties props = new ConsensusProperties();
        props.setDataDir("target/consensus-test/" + name + "-" + System.nanoTime());
        Cluster cluster = new Cluster(java.util.List.of("n1", "n2", "n3"), props.getDataDir(), props);
        cluster.resetAllStores();
        return cluster;
    }

    @Test
    void duplicateSubmissionIsSuppressedAndConflictIsRejected() {
        Cluster cluster = newCluster("idem-basic");
        RaftNode leader = cluster.awaitLeader(5);
        assertNotNull(leader);
        log.info(">>> 领导者={}", leader.id());

        CommitResult first = leader.submit("key-1", "SET account A");
        log.info(">>> 首次提交: {}", first);
        assertTrue(first.success());
        assertEquals(CommitReason.APPLIED, first.reason());
        assertEquals("OK:A", first.value());

        // 相同键 + 相同内容：必须返回首次结果，且不得二次应用。
        CommitResult retry = leader.submit("key-1", "SET account A");
        log.info(">>> 重复提交（相同内容）: {}", retry);
        assertTrue(retry.success());
        assertEquals(CommitReason.DUPLICATE_SUPPRESSED, retry.reason());
        assertEquals(first.value(), retry.value(), "重复提交必须返回与首次相同的结果");

        // 相同键 + 不同内容：必须拒绝且原因可区分。
        CommitResult conflict = leader.submit("key-1", "SET account B");
        log.info(">>> 相同键不同内容: {}", conflict);
        assertFalse(conflict.success());
        assertEquals(CommitReason.IDEMPOTENCY_KEY_CONFLICT, conflict.reason());
        assertNull(conflict.value());

        // 状态必须保持首次值，冲突内容绝不能生效。
        cluster.settle(2);
        for (RaftNode n : cluster.nodes()) {
            assertEquals("A", n.read("account"), "节点 " + n.id() + " 状态必须与首次提交一致");
        }
    }

    @Test
    void submitToFollowerIsRejectedWithNotLeader() {
        Cluster cluster = newCluster("idem-follower");
        RaftNode leader = cluster.awaitLeader(5);
        RaftNode follower = cluster.nodes().stream()
                .filter(n -> !n.id().equals(leader.id())).findFirst().orElseThrow();

        CommitResult result = follower.submit("key-f", "SET f 1");
        log.info(">>> 向跟随者 {} 提交: {}", follower.id(), result);
        assertFalse(result.success());
        assertEquals(CommitReason.NOT_LEADER, result.reason());
        assertEquals(leader.id(), result.leaderId(), "NOT_LEADER 结果应携带领导者提示");
    }

    @Test
    void retryAfterPartitionHealStaysConsistent() {
        Cluster cluster = newCluster("idem-partition");
        RaftNode leader = cluster.awaitLeader(5);
        assertNotNull(leader);

        // 首次提交成功并传播。
        CommitResult first = leader.submit("key-p", "SET order 42");
        assertTrue(first.success());
        cluster.settle(2);

        // 领导者被隔离：其本地未提交日志无法达到多数派。
        cluster.partition(leader.id());

        // 分区期间写入一条全新指令：复制失败，未决记录保留但绝不生效。
        CommitResult blocked = leader.submit("key-new", "SET order 43");
        log.info(">>> 分区期间新指令提交: {}", blocked);
        assertFalse(blocked.success());
        assertEquals(CommitReason.REPLICATION_FAILED, blocked.reason());

        // 用相同键相同内容重试：必须复用同一条未决日志（不重复追加），仍因多数派不可达失败。
        CommitResult blockedRetry = leader.submit("key-new", "SET order 43");
        log.info(">>> 分区期间同键同内容重试: {}", blockedRetry);
        assertFalse(blockedRetry.success());
        assertEquals(CommitReason.REPLICATION_FAILED, blockedRetry.reason());

        // 同键不同内容即便在未决阶段也必须被拒绝且原因可区分。
        CommitResult blockedConflict = leader.submit("key-new", "SET order 99");
        log.info(">>> 分区期间同键不同内容: {}", blockedConflict);
        assertFalse(blockedConflict.success());
        assertEquals(CommitReason.IDEMPOTENCY_KEY_CONFLICT, blockedConflict.reason());

        // 已提交的旧键在分区下重试，幂等语义不变：返回首次结果。
        CommitResult oldStable = leader.submit("key-p", "SET order 42");
        log.info(">>> 分区期间已提交旧键重试: {}", oldStable);
        assertTrue(oldStable.success());
        assertEquals("OK:42", oldStable.value());

        // 恢复后选出领导者，直接由提交动作驱动复制（不预先 settle，以验证“未决重试 -> APPLIED”）。
        cluster.heal();
        RaftNode newLeader = cluster.awaitLeader(5);
        CommitResult retryOld = newLeader.submit("key-p", "SET order 42");
        log.info(">>> 恢复后重试旧键: {}", retryOld);
        assertTrue(retryOld.success());
        assertEquals(CommitReason.DUPLICATE_SUPPRESSED, retryOld.reason());
        assertEquals("OK:42", retryOld.value());

        CommitResult applyNew = newLeader.submit("key-new", "SET order 43");
        log.info(">>> 恢复后重试未决指令: {}", applyNew);
        assertTrue(applyNew.success());
        assertEquals(CommitReason.APPLIED, applyNew.reason());
        assertEquals("OK:43", applyNew.value());

        // 所有副本最终状态一致：未决期间的冲突内容 99 从未生效。
        cluster.settle(2);
        for (RaftNode n : cluster.nodes()) {
            assertEquals("43", n.read("order"), "节点 " + n.id() + " 已提交状态必须一致");
        }
    }
}
