package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.NodeRole;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.Snapshot;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景四：落后副本追赶（InstallSnapshot）。
 *
 * <p>覆盖：离线很久、缺失历史已被多代压缩的副本仍能追上并对齐已提交进度与 KV；
 * 追赶期间领导持续提交不被拖住；追赶完成前后的幂等结论一致、不重复应用；
 * 推送快照途中领导重启 / 落后者重启均可收敛；已更新副本不被旧快照覆盖；
 * 反复掉队、跨越多代压缩后仍满足一致性。</p>
 */
class CatchUpTest {

    /** 断言所有在线节点的已提交下标完全一致，并返回该下标。 */
    private static long assertCommitAligned(RaftCluster cluster) {
        List<RaftNode> up = cluster.allNodes().values().stream()
                .filter(RaftNode::isUp).toList();
        long expected = -1;
        for (RaftNode n : up) {
            if (expected < 0) {
                expected = n.getCommitIndex();
            } else {
                assertEquals(expected, n.getCommitIndex(),
                        "节点 " + n.getId() + " 的提交下标 " + n.getCommitIndex()
                                + " 与 " + expected + " 不对齐");
            }
        }
        assertTrue(expected > 0, "集群应有已提交内容");
        return expected;
    }

    /** 断言所有在线节点对给定 key 集合的取值完全一致。 */
    private static void assertKvAligned(RaftCluster cluster, List<String> keys) {
        List<RaftNode> up = cluster.allNodes().values().stream()
                .filter(RaftNode::isUp).toList();
        for (String key : keys) {
            String expected = up.get(0).read(key);
            for (RaftNode n : up) {
                assertEquals(expected, n.read(key),
                        "节点 " + n.getId() + " 的 key=" + key
                                + " 取值 " + n.read(key) + " 与 " + expected + " 不一致");
            }
        }
    }

    private static RaftCluster clusterWithSmallSnapshot() {
        // 快照阈值 3：每提交约 3 条指令就压缩一代，长时间离线必然跨过快照边界
        return new RaftCluster(List.of("n1", "n2", "n3"),
                new RaftCluster.Config(8, 8, 3, 3));
    }

    @Test
    void longOfflineReplicaCatchesUpAcrossCompactedHistory() {
        RaftCluster cluster = clusterWithSmallSnapshot();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);
        RaftNode leaderNode = cluster.getNode(leader);

        // 离线一个副本（选非领导节点，保证离线期间领导与另一副本构成多数派持续工作）
        String laggardId = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        cluster.crash(laggardId);
        System.out.println("==> [追赶-基础] 副本 " + laggardId + " 离线，集群继续对外服务");

        // 离线期间提交 12 条指令（含 NOP，快照阈值=3 必然发生多代压缩）
        List<String> keys = new java.util.ArrayList<>();
        for (int i = 1; i <= 12; i++) {
            SubmitResult r = cluster.submit("c", "off-" + i, "SET k" + i + " v" + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(),
                    "离线期间两节点多数派应持续提交: " + r.reason());
            keys.add("k" + i);
        }
        // 在线两节点收敛
        assertTrue(cluster.waitForConvergence(30), "在线节点应持续收敛");
        long leaderSnapshot = leaderNode.getSnapshotIndex();
        long laggardCommit = cluster.getNode(laggardId).getCommitIndex();
        System.out.println("==> [追赶-基础] 离线期间领导提交至=" + leaderNode.getCommitIndex()
                + "，领导快照边界=" + leaderSnapshot + "，离线者提交仅=" + laggardCommit);
        assertTrue(leaderSnapshot > laggardCommit,
                "离线者缺失历史必须已被领导压缩掉（快照边界 " + leaderSnapshot
                        + " > 离线者提交 " + laggardCommit + "），否则测试没有真正跨越压缩");

        // ---- 落后者重新加入 ----
        cluster.restart(laggardId);
        long leaderCommitAtRejoin = leaderNode.getCommitIndex();

        // 追赶期间集群不被拖住：先黑掉落后者的入站消息，让它在恢复前保持"追赶中"。
        // 此时领导与另一健康副本仍构成多数派，必须能继续接收并提交全新指令。
        cluster.blockIncoming(laggardId);
        RaftNode laggard = cluster.getNode(laggardId);
        long blockedCommit = laggard.getCommitIndex();
        SubmitResult during = cluster.submit("c", "off-during", "SET kduring alive", 40);
        assertEquals(SubmitResult.Status.COMMITTED, during.status(),
                "落后者追赶中（入站暂不可达），领导仍必须能靠其余多数派提交新指令: "
                        + during.reason());
        assertTrue(leaderNode.getCommitIndex() > leaderCommitAtRejoin,
                "追赶期间领导提交下标必须继续前进");
        assertTrue(laggard.getCommitIndex() == blockedCommit,
                "追赶完成前落后者不得让未追赶内容生效（提交下标不得偷跑）");
        System.out.println("==> [追赶-基础] 落后者保持在 commit=" + blockedCommit
                + " 时，领导已继续提交到=" + leaderNode.getCommitIndex() + "，集群未被拖住");
        keys.add("kduring");

        // 恢复投递：落后者从快照开始追赶并在有限 tick 内追平
        cluster.unblockIncoming(laggardId);
        long catchUpTicks = cluster.ticksToConvergence(80);
        assertTrue(catchUpTicks >= 0, "落后副本应在有限 tick 内追平提交下标");
        // 追赶只需"一轮快照 + 至多一轮增量 + 一两个心跳间隔"量级的推进，
        // 不应随离线期间缺失历史长度（这里已跨越十余条、多代压缩）线性增长。
        assertTrue(catchUpTicks <= 20,
                "落后副本追赶应在有界的少量推进内完成，实际耗费 " + catchUpTicks + " ticks");
        long aligned = assertCommitAligned(cluster);
        assertKvAligned(cluster, keys);
        System.out.println("==> [追赶-基础] 三副本提交下标对齐=" + aligned
                + "，全部 key 取值一致，追赶仅用 " + catchUpTicks + " ticks");

        // 追赶完成后：之前用相同标识提交过的请求重试，结论与第一次一致（不二次应用）
        SubmitResult first = cluster.submit("c", "off-1", "SET k1 v1", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, first.status(),
                "追赶完成后旧请求重试必须命中幂等: " + first.reason());
        SubmitResult second = cluster.submit("c", "off-1", "SET k1 v1", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, second.status());
        assertEquals(first.commitIndex(), second.commitIndex());
        assertEquals(first.value(), second.value());
        System.out.println("==> [追赶-基础] 追赶后旧请求重试结论稳定："
                + second.status() + " index=" + second.commitIndex() + " value=" + second.value());
        // 状态机里 k1 只能是首次应用的值（重复应用会是 updated，结果串会变）
        for (RaftNode n : cluster.allNodes().values()) {
            assertEquals("v1", n.read("k1"));
        }
    }

    @Test
    void leaderRestartMidSnapshotPushStillConverges() {
        RaftCluster cluster = clusterWithSmallSnapshot();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        String laggardId = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        cluster.crash(laggardId);

        // 离线期间提交多条，跨越至少两代压缩
        List<String> keys = new java.util.ArrayList<>();
        for (int i = 1; i <= 9; i++) {
            SubmitResult r = cluster.submit("c", "m-" + i, "SET m" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(), r.reason());
            keys.add("m" + i);
        }
        assertTrue(cluster.waitForConvergence(30));
        long leaderSnap = cluster.getNode(leader).getSnapshotIndex();
        assertTrue(leaderSnap >= 6, "领导应已发生多代压缩，实际快照边界=" + leaderSnap);

        // 落后者回来，但立即黑掉入站消息，使其停在"追赶未完成"
        cluster.restart(laggardId);
        cluster.blockIncoming(laggardId);
        for (int t = 0; t < 6; t++) {
            cluster.tick(); // 领导多轮心跳尝试推快照，全部被丢弃
        }
        assertEquals(0, cluster.getNode(laggardId).getCommitIndex(),
                "入站被黑时落后者不应有任何推进");
        System.out.println("==> [追赶-领导重启] 快照推送在途中（领导心跳 "
                + "已多次尝试但消息被丢弃），此刻重启领导 " + leader);

        // 打断：直接重启正在推送快照的领导（磁盘保留）；之后再恢复落后者投递
        cluster.crash(leader);
        cluster.restart(leader);
        cluster.unblockIncoming(laggardId);

        // 集群重新选出领导（可能是原领导，也可能是另一健康副本），落后者最终必须追平
        String leader2 = cluster.runUntilLeaderElected(80);
        assertNotNull(leader2, "领导重启打断后应重新选出领导者");
        assertTrue(cluster.waitForConvergence(100),
                "领导在推送快照途中重启，集群与落后者最终仍须收敛");
        long aligned = assertCommitAligned(cluster);
        assertKvAligned(cluster, keys);
        System.out.println("==> [追赶-领导重启] 打断后重新收敛，提交下标对齐=" + aligned
                + "，新领导=" + leader2);
    }

    @Test
    void laggardRestartMidInstallResumesAndConverges() {
        RaftCluster cluster = clusterWithSmallSnapshot();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        String laggardId = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        cluster.crash(laggardId);
        List<String> keys = new java.util.ArrayList<>();
        for (int i = 1; i <= 9; i++) {
            SubmitResult r = cluster.submit("c", "r-" + i, "SET r" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(), r.reason());
            keys.add("r" + i);
        }
        assertTrue(cluster.waitForConvergence(30));

        // 落后者重新加入，允许一轮快照投递到达（安装到一半由我们再次打断）
        cluster.restart(laggardId);
        cluster.blockIncoming(laggardId);
        for (int t = 0; t < 4; t++) {
            cluster.tick(); // 领导在推、消息被丢
        }
        // 放行并推进若干 tick：快照应当到达并被安装（这就是"追赶进行到一半"的磁盘状态）
        cluster.unblockIncoming(laggardId);
        long afterFirstInstall = 0;
        for (int t = 0; t < 10; t++) {
            cluster.tick();
            afterFirstInstall = cluster.getNode(laggardId).getCommitIndex();
            if (afterFirstInstall > 0) {
                break;
            }
        }
        assertTrue(afterFirstInstall > 0,
                "放行后落后者应在几轮内安装快照，实际提交=" + afterFirstInstall);
        // 立刻再次打断，不让它走完整轮增量追赶
        System.out.println("==> [追赶-落后者重启] 落后者已安装快照到 commit="
                + afterFirstInstall + "（领导=" + cluster.getNode(leader).getCommitIndex()
                + "），此刻在追赶途中重启落后者");

        // 打断：落后者自己在追赶中途崩溃并重启（已安装的快照在磁盘上，必须可续传）
        cluster.crash(laggardId);
        cluster.restart(laggardId);

        // 领导追赶期间又提交了新的一代压缩
        for (int i = 10; i <= 14; i++) {
            SubmitResult r = cluster.submit("c", "r-" + i, "SET r" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(), r.reason());
            keys.add("r" + i);
        }

        assertTrue(cluster.waitForConvergence(100), "落后者中途重启后仍须追平");
        long aligned = assertCommitAligned(cluster);
        assertKvAligned(cluster, keys);
        System.out.println("==> [追赶-落后者重启] 落后者中途重启后续传完成，提交对齐=" + aligned);
    }

    @Test
    void newerReplicaIsNotOverwrittenByStaleSnapshot() {
        RaftCluster cluster = new RaftCluster(List.of("s1", "s2", "s3", "s4", "s5"),
                new RaftCluster.Config(8, 8, 3, 3));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        // 共同基线提交两条
        SubmitResult base = cluster.submit("c", "base", "SET shared old", 40);
        assertEquals(SubmitResult.Status.COMMITTED, base.status());
        assertTrue(cluster.waitForConvergence(40));

        // 隔离现任领导（少数派 2 人），多数派独立演进并发生压缩
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
                if (cluster.getNode(id).getRole() == NodeRole.LEADER) {
                    majLeader = id;
                }
            }
        }
        assertNotNull(majLeader, "多数派应选出新领导");
        // 多数派提交足够多指令，跨越多代压缩
        for (int i = 1; i <= 9; i++) {
            SubmitResult r = cluster.submitVia(cluster.getNode(majLeader),
                    "c", "new-" + i, "SET m" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(), r.reason());
        }
        assertTrue(cluster.getNode(majLeader).getSnapshotIndex() >= 6,
                "多数派领导应已压缩多代");
        // 老领导（少数派）手上还是旧内容、旧快照
        long oldLeaderCommit = cluster.getNode(leader).getCommitIndex();
        Snapshot staleSnapshot = cluster.getNode(leader).snapshotCopy();
        System.out.println("==> [追赶-防旧覆盖] 老领导 " + leader + " 停留在 commit="
                + oldLeaderCommit + "，多数派领导 " + majLeader + " 已提交到 "
                + cluster.getNode(majLeader).getCommitIndex());

        // 恢复分区前，先模拟"老领导迟到的 InstallSnapshot"直接送到多数派领导：
        // 这是追赶被打断/网络乱序下可能出现的旧消息，目标内容比它新，必须拒绝。
        String targetValueBefore = cluster.getNode(majLeader).read("shared");
        long targetCommitBefore = cluster.getNode(majLeader).getCommitIndex();
        cluster.injectInstallSnapshot(leader, majLeader, staleSnapshot, oldLeaderCommit);
        cluster.tick();
        assertEquals(targetCommitBefore, cluster.getNode(majLeader).getCommitIndex(),
                "旧快照不得把更新副本的提交下标往后拉");
        assertEquals(targetValueBefore, cluster.getNode(majLeader).read("shared"),
                "旧快照不得覆盖更新副本的 KV 状态");
        System.out.println("==> [追赶-防旧覆盖] 旧领导迟到快照被拒绝，目标 commit="
                + targetCommitBefore + " shared=" + targetValueBefore + " 保持不变");

        // 再走真实的分区恢复：老领导反而是"落后者"，它必须接受多数派的新快照追上
        cluster.heal();
        String converged = cluster.runUntilLeaderElected(80);
        assertNotNull(converged);
        assertTrue(cluster.waitForConvergence(120), "恢复后老领导必须靠新快照追平");
        assertCommitAligned(cluster);
        List<String> keys = new java.util.ArrayList<>();
        keys.add("shared");
        for (int i = 1; i <= 9; i++) {
            keys.add("m" + i);
        }
        assertKvAligned(cluster, keys);
        // shared 仍是旧值 old（多数派没有改它），但新 key 必须全部同步到老领导
        for (int i = 1; i <= 9; i++) {
            assertEquals(String.valueOf(i), cluster.getNode(leader).read("m" + i),
                    "老领导必须通过新快照/增量追平多数派新增状态 m" + i);
        }
        System.out.println("==> [追赶-防旧覆盖] 分区恢复后老领导已追平多数派，集群一致");
    }

    @Test
    void repeatedLagAndCatchUpAcrossManyCompactionGenerations() {
        RaftCluster cluster = clusterWithSmallSnapshot();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);
        List<String> keys = new java.util.ArrayList<>();

        int requestSeq = 0;
        // 连续 4 轮：让一个（可能不同的）副本离线，集群提交并压缩若干代，再让它回来追平
        for (int round = 1; round <= 4; round++) {
            String laggardId = cluster.nodeIds().get((round - 1) % cluster.size());
            boolean laggardIsLeader = laggardId.equals(cluster.getLeaderId());
            cluster.crash(laggardId);
            System.out.println("==> [追赶-多代] 第 " + round + " 轮：副本 " + laggardId
                    + (laggardIsLeader ? "（现任领导，离线触发重新选举）" : "（跟随者）") + " 离线");
            if (laggardIsLeader) {
                assertNotNull(cluster.runUntilLeaderElected(120),
                        "第 " + round + " 轮领导离线后多数派必须重新选出领导");
            }

            // 离线期间提交 6 条（阈值 3，至少两代压缩）
            for (int i = 0; i < 6; i++) {
                requestSeq++;
                String key = "g" + requestSeq;
                SubmitResult r = cluster.submit("c", "gen-" + requestSeq,
                        "SET " + key + " round" + round, 40);
                assertEquals(SubmitResult.Status.COMMITTED, r.status(),
                        "第 " + round + " 轮离线期间提交应成功: " + r.reason());
                keys.add(key);
            }
            // 在线副本之间保持收敛
            assertTrue(cluster.waitForConvergence(40),
                    "第 " + round + " 轮在线副本应持续收敛");

            cluster.restart(laggardId);
            assertTrue(cluster.runUntilLeaderElected(80) != null, "第 " + round + " 轮应有领导");
            assertTrue(cluster.waitForConvergence(100),
                    "第 " + round + " 轮离线副本必须在有限 tick 内追平");

            // 每轮追平后都做强一致性校验：提交下标对齐 + 所有 key 一致
            long aligned = assertCommitAligned(cluster);
            assertKvAligned(cluster, keys);
            System.out.println("==> [追赶-多代] 第 " + round + " 轮追平完成，提交下标对齐="
                    + aligned + "，累计 key=" + keys.size()
                    + "，领导快照边界=" + cluster.getNode(cluster.getLeaderId()).getSnapshotIndex());

            // 追平后幂等结论稳定：抽查本轮最早提交的一条
            String checkKey = keys.get(keys.size() - 6);
            long firstRequestOfRound = requestSeq - 5;
            SubmitResult dup = cluster.submit("c", "gen-" + firstRequestOfRound,
                    "SET " + checkKey + " round" + round, 30);
            assertEquals(SubmitResult.Status.DUPLICATE, dup.status(),
                    "第 " + round + " 轮追平后历史请求重试必须仍为 DUPLICATE: " + dup.reason());
            // 全部副本该 key 取值一致且未被二次应用
            assertKvAligned(cluster, List.of(checkKey));
        }

        // 最终再整体校验一次全部在线副本
        long finalAligned = assertCommitAligned(cluster);
        assertKvAligned(cluster, keys);
        System.out.println("==> [追赶-多代] 连续 4 轮掉队/追平、跨越多个压缩代后，"
                + "提交下标对齐=" + finalAligned + "，共 " + keys.size() + " 个 key 全部一致");
    }

    @Test
    void caughtUpReplicaDoesNotReapplyCommandsAndIdempotencySurvives() {
        RaftCluster cluster = clusterWithSmallSnapshot();
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        String laggardId = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leader)).findFirst().orElseThrow();
        // 先提交一条确定首次应用为 (new) 的基线，再让落后者离线
        SubmitResult base = cluster.submit("c", "once", "SET once only", 40);
        assertEquals(SubmitResult.Status.COMMITTED, base.status());
        assertEquals("OK:once=only(new)", base.value(), "首次应用结果必须是 new");
        assertTrue(cluster.waitForConvergence(30));

        cluster.crash(laggardId);
        List<String> keys = new java.util.ArrayList<>();
        keys.add("once");
        for (int i = 1; i <= 9; i++) {
            SubmitResult r = cluster.submit("c", "d-" + i, "SET d" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.COMMITTED, r.status(), r.reason());
            keys.add("d" + i);
        }
        assertTrue(cluster.waitForConvergence(30));

        // 落后者跨快照追平
        cluster.restart(laggardId);
        assertTrue(cluster.waitForConvergence(100), "落后者必须追平");
        assertCommitAligned(cluster);
        assertKvAligned(cluster, keys);

        // 追平后，落后者对"追赶区间内"已提交请求做重试：必须返回 DUPLICATE，
        // 且结果字符串保持首次应用的结论（若追赶时二次应用，结果会从 new 变 updated）
        for (int i = 1; i <= 9; i++) {
            SubmitResult dup = cluster.submit("c", "d-" + i, "SET d" + i + " " + i, 40);
            assertEquals(SubmitResult.Status.DUPLICATE, dup.status(),
                    "d-" + i + " 追赶完成后重试必须幂等: " + dup.reason());
            assertEquals("OK:d" + i + "=" + i + "(new)", dup.value(),
                    "d" + i + " 追赶过程中只能被应用一次，实际=" + dup.value());
        }
        // 离线前的那条也一样
        SubmitResult baseDup = cluster.submit("c", "once", "SET once only", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, baseDup.status());
        assertEquals("OK:once=only(new)", baseDup.value());
        System.out.println("==> [追赶-不重复应用] 落后者追赶后 10 条历史请求重试均为 DUPLICATE，"
                + "结果串保留首次 (new) 结论，证明快照安装+增量回放未二次应用");
    }
}
