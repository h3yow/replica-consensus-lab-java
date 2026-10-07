package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftMessage;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：落后副本追赶（InstallSnapshot）。
 * 覆盖：离线很久的副本跨越多次压缩追平；追赶期间集群继续提交、未提交内容不生效、
 * 指令不被重复应用；领导/跟随者在推送中途重启后仍能收敛；过期快照不覆盖更新状态；
 * 多轮压缩下反复掉队又追平仍保持一致。
 */
class CatchUpTest {

    /** 快照阈值调小为 4，让压缩在少量指令后就会真实发生（不再绕开）。 */
    private static RaftCluster newCluster() {
        return new RaftCluster(List.of("a", "b", "c"), new RaftCluster.Config(8, 8, 3, 4));
    }

    private static List<SubmitResult> submitBatch(RaftCluster cluster, String client,
                                                  String reqPrefix, String keyPrefix,
                                                  int start, int count) {
        List<SubmitResult> results = new ArrayList<>();
        for (int i = start; i < start + count; i++) {
            SubmitResult r = cluster.submit(client, reqPrefix + i,
                    "SET " + keyPrefix + i + " v" + i, 60);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(),
                    "第 " + i + " 条应提交: " + r.reason());
            results.add(r);
        }
        return results;
    }

    private static String pickLagging(RaftCluster cluster, String leaderId) {
        return cluster.nodeIds().stream().filter(id -> !id.equals(leaderId)).findFirst().orElseThrow();
    }

    /** 逐 tick 推进直到落后者追平领导，返回花费的 tick 数；超时返回 -1。 */
    private static int ticksUntilCaughtUp(RaftCluster cluster, String laggingId, int maxTicks) {
        for (int i = 0; i < maxTicks; i++) {
            String leaderId = cluster.getLeaderId();
            if (leaderId != null
                    && cluster.getNode(laggingId).getCommitIndex()
                        >= cluster.getNode(leaderId).getCommitIndex()) {
                return i;
            }
            cluster.tick();
        }
        return -1;
    }

    private static void assertAllKeysEqual(RaftCluster cluster, String keyPrefix, int from, int to) {
        for (int i = from; i <= to; i++) {
            String expected = "v" + i;
            for (RaftNode node : cluster.allNodes().values()) {
                if (node.isUp()) {
                    assertEquals(expected, node.read(keyPrefix + i),
                            "节点 " + node.getId() + " 的 " + keyPrefix + i + " 应一致");
                }
            }
        }
    }

    @Test
    void laggingReplicaCatchesUpAcrossCompaction() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        submitBatch(cluster, "c", "r", "key", 1, 8);
        assertTrue(cluster.waitForConvergence(60));

        String lagging = pickLagging(cluster, leader);
        long snapshotBeforeOffline = cluster.getNode(lagging).getSnapshotIndex();

        // 副本离线期间，集群继续接收指令并发生多轮压缩
        cluster.crash(lagging);
        submitBatch(cluster, "c", "r", "key", 9, 12);
        String leaderId = cluster.getLeaderId();
        assertNotNull(leaderId);
        long leaderSnapshot = cluster.getNode(leaderId).getSnapshotIndex();
        long laggingLastLog = cluster.getNode(lagging).getLastLogIndex();
        System.out.println("==> 落后副本离线期间：领导快照推进到 " + leaderSnapshot
                + "，落后副本日志末=" + laggingLastLog + "（缺口已被压缩）");
        assertTrue(leaderSnapshot >= laggingLastLog,
                "构造前提：落后副本缺的历史必须已被领导压缩");

        // 重新加入：必须靠 InstallSnapshot 跨越压缩缺口追平
        cluster.restart(lagging);
        int ticks = ticksUntilCaughtUp(cluster, lagging, 150);
        System.out.println("==> 落后副本 " + lagging + " 重新加入后 " + ticks + " ticks 追平");
        assertTrue(ticks >= 0, "落后副本必须在合理 tick 数内追平");
        assertTrue(cluster.getNode(lagging).getSnapshotIndex() > snapshotBeforeOffline,
                "落后副本应通过安装快照跨越压缩缺口");

        // 追平后：三方已提交进度对齐、每个 key 一致，领导与另一副本未被拖累
        assertTrue(cluster.waitForConvergence(60));
        long commit = cluster.getNode(cluster.getLeaderId()).getCommitIndex();
        for (RaftNode node : cluster.allNodes().values()) {
            assertEquals(commit, node.getCommitIndex(),
                    "节点 " + node.getId() + " 的已提交进度应与领导对齐");
        }
        assertAllKeysEqual(cluster, "key", 1, 20);
        System.out.println("==> 追平后三方 commitIndex=" + commit + "，key1..key20 全部一致");
    }

    @Test
    void clusterKeepsCommittingDuringCatchUp_andRetryConclusionStable() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        List<SubmitResult> first = submitBatch(cluster, "c", "r", "key", 1, 6);
        assertTrue(cluster.waitForConvergence(60));

        String lagging = pickLagging(cluster, leader);
        cluster.crash(lagging);
        submitBatch(cluster, "c", "r", "key", 7, 10); // 触发多轮压缩

        // 追赶期间：领导继续接收并提交新指令，不能被落后副本拖住
        cluster.restart(lagging);
        List<SubmitResult> during = submitBatch(cluster, "c", "r", "key", 17, 4);
        System.out.println("==> 追赶期间领导又连续提交了 4 条新指令（未被拖住）");

        String leaderId = cluster.getLeaderId();
        long newestIndex = cluster.getNode(leaderId).getLastLogIndex();
        RaftNode lag = cluster.getNode(lagging);

        // 逐 tick 追平，期间校验：未提交内容不得在落后副本上生效
        int ticks = 0;
        while (lag.getCommitIndex() < cluster.getNode(leaderId).getCommitIndex() && ticks < 150) {
            cluster.tick();
            ticks++;
            if (lag.getCommitIndex() < newestIndex) {
                assertNull(lag.read("key20"),
                        "key20 在落后副本上尚未提交，不得提前生效");
            }
            assertTrue(lag.getCommitIndex() <= cluster.getNode(leaderId).getCommitIndex(),
                    "落后副本已提交进度不得超过领导");
        }
        assertTrue(lag.getCommitIndex() >= cluster.getNode(leaderId).getCommitIndex(),
                "落后副本应追平");
        assertAllKeysEqual(cluster, "key", 1, 20);

        // 追赶完成后：相同标识重试，结论与首次完全一致（值相同、不二次应用）
        SubmitResult retry = cluster.submit("c", "r1", "SET key1 v1", 60);
        assertEquals(SubmitResult.Status.DUPLICATE, retry.status(), retry.reason());
        assertEquals(first.get(0).value(), retry.value(),
                "重试必须返回与首次相同的结论");
        // 落后副本的幂等表也随快照追平：记录的是首次结论而非二次应用的结果
        assertEquals(first.get(0).value(), lag.idempotentResult("c", "r1"),
                "追赶来的幂等记录必须与首次结论一致（SET 若被重复应用会变成 (updated)）");
        // 相同标识携带不同内容：仍然拒绝
        SubmitResult conflict = cluster.submit("c", "r1", "SET key1 CHANGED", 60);
        assertEquals(SubmitResult.Status.IDEMPOTENCY_CONFLICT, conflict.status(), conflict.reason());
        System.out.println("==> 追赶完成后重试 r1：DUPLICATE 且结论与首次一致；冲突内容被拒绝");
    }

    @Test
    void leaderCrashMidSnapshotPushStillConverges() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        submitBatch(cluster, "c", "r", "key", 1, 6);
        assertTrue(cluster.waitForConvergence(60));

        String lagging = pickLagging(cluster, leader);
        cluster.crash(lagging);
        submitBatch(cluster, "c", "r", "key", 7, 12); // 多轮压缩，缺口进入快照

        // 落后副本回来；同时领导刚接下一条新指令（快照之后还有尾部条目要补）
        cluster.restart(lagging);
        String leaderId = cluster.getLeaderId();
        long preHealCommit = cluster.getNode(lagging).getCommitIndex();
        SubmitResult pending = cluster.getNode(leaderId).submit("c", "r19", "SET key19 v19");
        assertEquals(SubmitResult.Status.PENDING, pending.status(), pending.reason());

        // 观察到"快照已安装、尾部条目未补齐"的中间态时，让领导立刻崩溃
        boolean observedMidPush = false;
        for (int i = 0; i < 200 && !observedMidPush; i++) {
            cluster.tick();
            RaftNode lag = cluster.getNode(lagging);
            RaftNode lead = cluster.getNode(leaderId);
            if (lag.getCommitIndex() > preHealCommit
                    && lag.getLastLogIndex() < lead.getLastLogIndex()) {
                cluster.crash(leaderId);
                observedMidPush = true;
                System.out.println("==> 领导在推送快照后、尾部条目补齐前崩溃（落后副本 commit="
                        + lag.getCommitIndex() + " 日志末=" + lag.getLastLogIndex()
                        + "，领导日志末=" + lead.getLastLogIndex() + "）");
            }
        }
        assertTrue(observedMidPush, "应能观察到快照推送被打断的中间态");

        // 剩余多数派选出新领导，落后副本最终仍要收敛到一致状态
        String newLeader = cluster.runUntilLeaderElected(120);
        assertNotNull(newLeader, "领导崩溃后应重新选主");
        int ticks = ticksUntilCaughtUp(cluster, lagging, 200);
        System.out.println("==> 新领导 " + newLeader + " 接管，落后副本 " + ticks + " ticks 后追平");
        assertTrue(ticks >= 0, "领导中途崩溃后落后副本仍必须追平");
        assertAllKeysEqual(cluster, "key", 1, 18);
        long commit = cluster.getNode(cluster.getLeaderId()).getCommitIndex();
        for (RaftNode node : cluster.allNodes().values()) {
            if (node.isUp()) {
                assertEquals(commit, node.getCommitIndex(),
                        "节点 " + node.getId() + " 的已提交进度应对齐");
            }
        }
    }

    @Test
    void followerCrashMidCatchUpRestartsAndConverges() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        List<SubmitResult> first = submitBatch(cluster, "c", "r", "key", 1, 6);
        assertTrue(cluster.waitForConvergence(60));

        String lagging = pickLagging(cluster, leader);
        cluster.crash(lagging);
        submitBatch(cluster, "c", "r", "key", 7, 12);

        cluster.restart(lagging);
        long preHealCommit = cluster.getNode(lagging).getCommitIndex();
        String leaderId = cluster.getLeaderId();
        cluster.getNode(leaderId).submit("c", "r19", "SET key19 v19"); // 尾部还有在途条目

        // 落后副本刚装上快照（追赶进行到一半）时自己也崩溃
        boolean crashedMidCatchUp = false;
        for (int i = 0; i < 200 && !crashedMidCatchUp; i++) {
            cluster.tick();
            RaftNode lag = cluster.getNode(lagging);
            if (lag.getCommitIndex() > preHealCommit
                    && lag.getLastLogIndex() < cluster.getNode(leaderId).getLastLogIndex()) {
                cluster.crash(lagging);
                crashedMidCatchUp = true;
                System.out.println("==> 落后副本在追赶中途崩溃（已装快照至 commit="
                        + lag.getCommitIndex() + "），随后重启");
            }
        }
        assertTrue(crashedMidCatchUp, "应能让落后副本在追赶中途崩溃");
        cluster.restart(lagging);

        int ticks = ticksUntilCaughtUp(cluster, lagging, 200);
        System.out.println("==> 落后副本重启后 " + ticks + " ticks 追平");
        assertTrue(ticks >= 0, "追赶中途崩溃重启后仍必须追平");
        assertTrue(cluster.waitForConvergence(60));
        assertAllKeysEqual(cluster, "key", 1, 18);
        // 幂等表随快照落盘：崩溃重启后首次结论依然可查且一致
        assertEquals(first.get(0).value(), cluster.getNode(lagging).idempotentResult("c", "r1"),
                "追赶中途崩溃重启后，幂等记录必须与首次结论一致");
        SubmitResult retry = cluster.submit("c", "r1", "SET key1 v1", 60);
        assertEquals(SubmitResult.Status.DUPLICATE, retry.status(), retry.reason());
        assertEquals(first.get(0).value(), retry.value());
    }

    @Test
    void staleSnapshotDoesNotOverwriteNewerState() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        submitBatch(cluster, "c", "r", "key", 1, 8);
        assertTrue(cluster.waitForConvergence(60));

        String target = pickLagging(cluster, leader);
        RaftNode node = cluster.getNode(target);
        long commitBefore = node.getCommitIndex();
        long lastLogBefore = node.getLastLogIndex();
        String valueBefore = node.read("key1");
        assertEquals("v1", valueBefore);

        // 模拟网络中延迟/重复投递的过期快照：内容更旧，绝不能覆盖已提交的更新状态
        RaftMessage.InstallSnapshot stale = new RaftMessage.InstallSnapshot(
                cluster.getNode(leader).getCurrentTerm(), leader,
                2, 1, Map.of("key1", "STALE"), Map.of());
        cluster.inject(leader, target, stale);
        for (int i = 0; i < 5; i++) {
            cluster.tick();
        }

        assertEquals(commitBefore, node.getCommitIndex(), "过期快照不得回退已提交进度");
        assertEquals(lastLogBefore, node.getLastLogIndex(), "过期快照不得截断已有日志");
        assertEquals("v1", node.read("key1"), "过期快照不得覆盖已提交的新值");
        assertEquals(valueBefore, node.read("key1"));
        System.out.println("==> 过期快照（至 index=2）被忽略，节点 " + target
                + " 的 commit=" + commitBefore + "、key1=v1 保持不变");

        // 集群仍然健康：新指令照常提交并收敛
        SubmitResult after = cluster.submit("c", "r9", "SET key9 v9", 60);
        assertEquals(SubmitResult.Status.COMMITTED, after.status(), after.reason());
        assertTrue(cluster.waitForValue("key9", "v9", 60));
        assertAllKeysEqual(cluster, "key", 1, 9);
    }

    @Test
    void repeatedLagAndCatchUpAcrossMultipleCompactions() {
        RaftCluster cluster = newCluster();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        String firstValue = null;
        int keySeq = 0;
        // 连续多轮：副本掉队 -> 集群跨越好几代压缩 -> 副本追平
        for (int round = 1; round <= 3; round++) {
            String leaderId = cluster.getLeaderId();
            assertNotNull(leaderId, "第 " + round + " 轮开始时应已在任领导");
            String lagging = pickLagging(cluster, leaderId);
            long commitBeforeOffline = cluster.getNode(leaderId).getCommitIndex();

            cluster.crash(lagging);
            List<SubmitResult> batch = submitBatch(cluster, "c", "r" + round + "_", "k" + round + "_", 1, 8);
            if (round == 1) {
                firstValue = batch.get(0).value();
            }
            keySeq += 8;
            cluster.restart(lagging);

            int ticks = ticksUntilCaughtUp(cluster, lagging, 200);
            System.out.println("==> 第 " + round + " 轮：副本 " + lagging + " 跨越压缩追平，耗时 "
                    + ticks + " ticks");
            assertTrue(ticks >= 0, "第 " + round + " 轮落后副本必须追平");
            assertTrue(cluster.getNode(leaderId).getCommitIndex() >= commitBeforeOffline,
                    "领导已提交进度不得回退");

            // 每个在线副本的已提交状态满足一致性约束
            assertTrue(cluster.waitForConvergence(60));
            long commit = cluster.getNode(cluster.getLeaderId()).getCommitIndex();
            for (RaftNode node : cluster.allNodes().values()) {
                assertEquals(commit, node.getCommitIndex(),
                        "第 " + round + " 轮后节点 " + node.getId() + " 已提交进度应对齐");
                for (int r = 1; r <= round; r++) {
                    for (int i = 1; i <= 8; i++) {
                        assertEquals("v" + i, node.read("k" + r + "_" + i),
                                "第 " + round + " 轮后节点 " + node.getId()
                                        + " 的 k" + r + "_" + i + " 应一致");
                    }
                }
            }
        }
        System.out.println("==> 连续 3 轮掉队/追平（跨越多代压缩）后，三副本状态完全一致");

        // 跨越多代压缩后，最早的幂等结论依然稳定
        SubmitResult retry = cluster.submit("c", "r1_1", "SET k1_1 v1", 60);
        assertEquals(SubmitResult.Status.DUPLICATE, retry.status(), retry.reason());
        assertEquals(firstValue, retry.value(), "跨多代压缩后重试结论必须与首次一致");
    }
}
