package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景三：故障恢复与日志一致性。
 * 覆盖：崩溃后基于快照+操作历史恢复；未提交记录重启后不生效；
 * 副本间历史分歧被检测并截断修复；任何时刻各副本已提交状态一致。
 */
class RecoveryTest {

    @Test
    void restartReplaysCommittedStateFromSnapshotAndHistory() {
        // 快照阈值设为 3，促使产生快照
        RaftCluster cluster = new RaftCluster(List.of("r1", "r2", "r3"),
                new RaftCluster.Config(8, 8, 3, 3));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        // 提交足够多指令以触发快照（含 NOP，多条）
        for (int i = 1; i <= 6; i++) {
            SubmitResult result = cluster.submit("c", "k" + i, "SET key" + i + " v" + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, result.status(),
                    "第 " + i + " 条应提交: " + result.reason());
        }
        assertTrue(cluster.waitForConvergence(40), "提交后各节点应收敛");
        long commitBefore = cluster.getNode(leader).getCommitIndex();
        System.out.println("==> 崩溃前提交下标=" + commitBefore);

        // 全部节点依次崩溃并重启：状态必须完整恢复（快照 + 快照后日志回放）
        for (String id : cluster.nodeIds()) {
            cluster.crash(id);
            cluster.restart(id);
        }
        // 重启后重新选举
        String leader2 = cluster.runUntilLeaderElected(60);
        assertNotNull(leader2, "全部重启后应重新选出领导者");
        assertTrue(cluster.waitForConvergence(40));

        for (int i = 1; i <= 6; i++) {
            for (RaftNode node : cluster.allNodes().values()) {
                assertEquals("v" + i, node.read("key" + i),
                        "节点 " + node.getId() + " 重启后 key" + i + " 必须被恢复");
            }
        }
        System.out.println("==> 全部节点崩溃重启后状态完整恢复（快照+历史回放）");

        // 重启后仍能继续正常提交，且幂等表也随快照/日志恢复：旧请求重复仍是 DUPLICATE
        SubmitResult dup = cluster.submit("c", "k1", "SET key1 v1", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, dup.status(),
                "重启后幂等记录必须随快照恢复: " + dup.reason());
        SubmitResult fresh = cluster.submit("c", "k7", "SET key7 v7", 40);
        assertEquals(SubmitResult.Status.COMMITTED, fresh.status(), fresh.reason());
        assertTrue(cluster.waitForValue("key7", "v7", 40));
    }

    @Test
    void uncommittedEntryNeverTakesEffectAfterRestart() {
        RaftCluster cluster = new RaftCluster(List.of("s1", "s2", "s3"));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        // 先提交一条确定生效的基线
        SubmitResult base = cluster.submit("c", "base", "SET committed yes", 40);
        assertEquals(SubmitResult.Status.COMMITTED, base.status());
        assertTrue(cluster.waitForConvergence(40));

        // 隔离领导，让其产生一条无法多数派提交的日志
        List<String> followers = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).toList();
        cluster.partition(followers, List.of(leader));

        RaftNode staleLeader = cluster.getNode(leader);
        SubmitResult blocked =
                cluster.submitVia(staleLeader, "c", "ghost", "SET ghost never", 10);
        assertTrue(blocked.status() == SubmitResult.Status.TIMEOUT
                        || blocked.status() == SubmitResult.Status.TERM_CHANGED,
                "隔离期领导不能提交: " + blocked.status());
        System.out.println("==> 隔离期未提交结论=" + blocked.status()
                + "（日志可能已追加但绝不能生效）");

        // 直接崩溃该老领导：其未提交日志随"磁盘"保留
        cluster.crash(leader);
        cluster.heal();
        // 重启老领导
        cluster.restart(leader);
        cluster.runUntilLeaderElected(60);
        assertTrue(cluster.waitForConvergence(60));

        // 所有节点：已提交基线存在，未提交 ghost 一律不存在
        for (RaftNode node : cluster.allNodes().values()) {
            assertEquals("yes", node.read("committed"),
                    "节点 " + node.getId() + " 的已提交基线必须恢复");
            assertEquals(null, node.read("ghost"),
                    "节点 " + node.getId() + " 的未提交记录绝不能生效");
        }
        System.out.println("==> 重启后 ghost 未生效，committed 基线完整恢复");

        // 新集群用同一幂等标识提交：因之前从未提交成功，应作为新指令提交一次
        SubmitResult retry = cluster.submit("c", "ghost", "SET ghost never", 40);
        assertEquals(SubmitResult.Status.COMMITTED, retry.status(),
                "未提交记录占用标识不应阻止之后真正提交: " + retry.reason());
        assertTrue(cluster.waitForValue("ghost", "never", 40));
    }

    @Test
    void divergentHistoryIsDetectedAndRepaired() {
        RaftCluster cluster = new RaftCluster(List.of("d1", "d2", "d3", "d4", "d5"));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        // 共同已提交前缀
        SubmitResult common = cluster.submit("c", "common", "SET shared origin", 40);
        assertEquals(SubmitResult.Status.COMMITTED, common.status());
        assertTrue(cluster.waitForConvergence(40));

        // 隔离老领导（少数派 2 人），多数派独立演进并提交新值
        String buddy = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        List<String> minority = List.of(leader, buddy);
        List<String> majority = cluster.nodeIds().stream()
                .filter(id -> !minority.contains(id)).toList();
        cluster.partition(majority, minority);

        String majLeader = null;
        for (int i = 0; i < 120 && majLeader == null; i++) {
            cluster.tick();
            for (String id : majority) {
                if (cluster.getNode(id).getRole().name().equals("LEADER")) {
                    majLeader = id;
                }
            }
        }
        assertNotNull(majLeader, "多数派分区应选出领导");
        SubmitResult branched = cluster.submitVia(cluster.getNode(majLeader),
                "c", "branch", "SET shared majority", 40);
        assertEquals(SubmitResult.Status.COMMITTED, branched.status(), branched.reason());

        // 少数派老领导也产生一条分歧未提交日志
        SubmitResult lost = cluster.submitVia(cluster.getNode(leader),
                "c", "lost", "SET shared minority", 10);
        System.out.println("==> 少数派分叉提交结论=" + lost.status());

        // 恢复分区：分歧必须被检测、截断并按多数派历史修复
        cluster.heal();
        String convergedLeader = cluster.runUntilLeaderElected(60);
        assertNotNull(convergedLeader);
        assertTrue(cluster.waitForConvergence(80), "分区恢复后日志必须收敛");
        assertTrue(cluster.waitForValue("shared", "majority", 40), "状态必须收敛到多数派值");

        for (RaftNode node : cluster.allNodes().values()) {
            assertEquals("majority", node.read("shared"),
                    "节点 " + node.getId() + " 必须修复分歧，采用多数派历史");
            assertEquals(null, node.read("minority-wins"),
                    "少数派分叉值不得残留");
        }
        System.out.println("==> 历史分歧已检测并修复，所有副本状态一致");
    }
}
