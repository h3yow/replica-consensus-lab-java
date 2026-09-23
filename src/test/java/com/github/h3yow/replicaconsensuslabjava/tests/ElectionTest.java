package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.NodeRole;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景一：领导者选举与任期推进。
 * 覆盖：多数派当选、同一任期唯一领导、少数派无法当选/提交、分区恢复后收敛。
 */
class ElectionTest {

    @Test
    void onlyMajorityCanElect_andConvergesAfterHeal() {
        RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3", "n4", "n5"));

        // 1) 初始应选出唯一领导者
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader, "集群启动后应在多数派支持下选出领导者");
        long firstTerm = cluster.getNode(leader).getCurrentTerm();
        System.out.println("==> 初始领导者=" + leader + " term=" + firstTerm);
        assertUniqueLeaderPerTerm(cluster);

        // 2) 制造 3 | 2 分区，把现任领导放进 2 人组（少数派）
        String other = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        List<String> minority = List.of(leader, other);
        List<String> majority = cluster.nodeIds().stream()
                .filter(id -> !minority.contains(id)).toList();
        cluster.partition(majority, minority);

        // 3) 推进：多数派一侧应选出更高任期的新领导
        String newLeader = null;
        for (int i = 0; i < 60; i++) {
            cluster.tick();
            for (String id : majority) {
                if (cluster.getNode(id).getRole() == NodeRole.LEADER) {
                    newLeader = id;
                }
            }
        }
        assertNotNull(newLeader, "多数派分区应独立选出新领导者");
        long newTerm = cluster.getNode(newLeader).getCurrentTerm();
        assertTrue(newTerm > firstTerm, "新领导者应推进到更高任期: " + newTerm + " > " + firstTerm);
        System.out.println("==> 分区期间多数派新领导者=" + newLeader + " term=" + newTerm);
        assertUniqueLeaderPerTerm(cluster);

        // 4) 分区恢复：收敛到唯一领导者，旧领导退回跟随者
        cluster.heal();
        String converged = cluster.runUntilLeaderElected(60);
        assertNotNull(converged, "分区恢复后应重新收敛到唯一领导者");
        assertUniqueLeaderPerTerm(cluster);
        System.out.println("==> 分区恢复后唯一领导者=" + converged
                + " term=" + cluster.getNode(converged).getCurrentTerm());

        if (!leader.equals(converged)) {
            assertEquals(NodeRole.FOLLOWER, cluster.getNode(leader).getRole(),
                    "被更高任期取代的旧领导必须退回跟随者");
        }
    }

    @Test
    void minoritySideCanNeitherElectNorCommit() {
        RaftCluster cluster = new RaftCluster(List.of("a", "b", "c", "d", "e"));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        // 让领导落在 2 人少数派
        String other = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        List<String> minority = List.of(leader, other);
        List<String> majority = cluster.nodeIds().stream()
                .filter(id -> !minority.contains(id)).toList();
        cluster.partition(majority, minority);

        for (int i = 0; i < 120; i++) {
            cluster.tick();
        }

        // 多数派一定保有可工作的领导者
        boolean majorityHasLeader = majority.stream()
                .anyMatch(id -> cluster.getNode(id).getRole() == NodeRole.LEADER);
        assertTrue(majorityHasLeader, "多数派分区必须保有可工作的领导者");

        // 少数派内任何自认领导的节点都无法完成多数派提交
        for (String id : minority) {
            RaftNode node = cluster.getNode(id);
            if (node.getRole() == NodeRole.LEADER) {
                SubmitResult result =
                        cluster.submitVia(node, "c", "should-fail", "SET x 1", 12);
                assertTrue(result.status() == SubmitResult.Status.TIMEOUT
                                || result.status() == SubmitResult.Status.TERM_CHANGED,
                        "少数派领导提交必须失败，实际=" + result.status() + " 原因=" + result.reason());
                System.out.println("==> 少数派提交被正确拒绝: " + result.status());
            }
        }
    }

    private static void assertUniqueLeaderPerTerm(RaftCluster cluster) {
        for (Map.Entry<Long, Set<String>> e : cluster.leadersByTerm().entrySet()) {
            assertEquals(1, e.getValue().size(),
                    "同一任期 " + e.getKey() + " 出现多个领导者: " + e.getValue());
        }
    }
}
