package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 场景二：带幂等标识的指令提交。
 * 覆盖：重复提交返回相同结论且不二次应用；同标识不同内容被拒绝且原因可区分；
 * 提交结论在重试与分区场景下保持一致；非领导入口可转发。
 */
class IdempotencyTest {

    private static final String CLIENT = "client-A";

    @Test
    void duplicateSubmissionReturnsSameResultWithoutReapply() {
        RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3"));
        String leader = cluster.runUntilLeaderElected(60);
        assertNotNull(leader);

        SubmitResult first = cluster.submit(CLIENT, "req-1", "SET color blue", 30);
        assertEquals(SubmitResult.Status.COMMITTED, first.status(), first.reason());
        long firstIndex = first.commitIndex();
        String firstValue = first.value();
        System.out.println("==> 首次提交: status=" + first.status()
                + " index=" + firstIndex + " value=" + firstValue);

        // 完全相同的请求重复提交多次
        for (int i = 0; i < 3; i++) {
            SubmitResult dup = cluster.submit(CLIENT, "req-1", "SET color blue", 30);
            assertEquals(SubmitResult.Status.DUPLICATE, dup.status(),
                    "重复提交必须标记为 DUPLICATE，实际=" + dup.status());
            assertEquals(firstIndex, dup.commitIndex(), "重复请求必须返回首次的提交下标");
            assertEquals(firstValue, dup.value(), "重复请求必须返回与首次完全一致的结果");
            System.out.println("==> 第 " + (i + 2) + " 次提交: " + dup.status()
                    + " 复用 index=" + dup.commitIndex() + " value=" + dup.value()
                    + " reason=" + dup.reason());
        }

        // 状态机只有一次 SET（结果字符串带 (new)，若二次应用会变成 (updated)）
        assertTrue(cluster.waitForValue("color", "blue", 30), "重复提交后各节点应收敛");
        for (RaftNode node : cluster.allNodes().values()) {
            assertEquals("blue", node.read("color"));
        }
        assertEquals("OK:color=blue(new)", firstValue, "首次应用必须是 new 而非 updated");
    }

    @Test
    void sameIdempotencyKeyWithDifferentContentIsRejectedWithDistinctReason() {
        RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3"));
        cluster.runUntilLeaderElected(60);

        SubmitResult first = cluster.submit(CLIENT, "req-9", "SET k v1", 30);
        assertEquals(SubmitResult.Status.COMMITTED, first.status());

        // 同一 (clientId, requestId) 携带不同内容
        SubmitResult conflict = cluster.submit(CLIENT, "req-9", "SET k v2-DIFFERENT", 30);
        assertEquals(SubmitResult.Status.IDEMPOTENCY_CONFLICT, conflict.status(),
                "同标识异内容必须被拒绝");
        assertNotEquals(SubmitResult.Status.DUPLICATE, conflict.status(),
                "冲突拒绝必须与重复返回可区分");
        assertTrue(conflict.reason().contains("不同内容"),
                "拒绝原因需明确说明内容冲突，实际=" + conflict.reason());
        System.out.println("==> 冲突提交被拒绝: status=" + conflict.status()
                + " reason=" + conflict.reason());

        // 原内容状态不受影响
        assertEquals("v1", cluster.getNode(leaderOf(cluster)).read("k"));

        // 换一个 requestId 的相同内容是全新合法提交
        SubmitResult other = cluster.submit(CLIENT, "req-10", "SET k v2-DIFFERENT", 30);
        assertEquals(SubmitResult.Status.COMMITTED, other.status(), other.reason());
        assertEquals("v2-DIFFERENT", cluster.getNode(leaderOf(cluster)).read("k"));
    }

    @Test
    void submitViaFollowerEntryIsForwardedToLeader() {
        RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3"));
        String leaderId = cluster.runUntilLeaderElected(60);
        RaftNode follower = cluster.allNodes().values().stream()
                .filter(n -> !n.getId().equals(leaderId)).findFirst().orElseThrow();

        SubmitResult result =
                cluster.submitVia(follower, CLIENT, "req-f", "SET viafollower yes", 40);
        assertEquals(SubmitResult.Status.COMMITTED, result.status(),
                "非领导入口应按 leaderHint 转发并提交: " + result.reason());
        assertTrue(cluster.waitForValue("viafollower", "yes", 30));
        assertEquals("yes", cluster.getNode(leaderId).read("viafollower"));

        // 经由跟随者重复提交同样命中幂等
        SubmitResult dup =
                cluster.submitVia(follower, CLIENT, "req-f", "SET viafollower yes", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, dup.status());
        System.out.println("==> 经跟随者入口提交与重试结论一致: " + dup.status());
    }

    @Test
    void retryAcrossPartitionKeepsConsistentConclusion() {
        RaftCluster cluster = new RaftCluster(List.of("n1", "n2", "n3"));
        String leaderId = cluster.runUntilLeaderElected(60);

        // 让领导与另外两台断开（领导进入 1 节点少数派）
        List<String> followers = cluster.nodeIds().stream()
                .filter(id -> !id.equals(leaderId)).toList();
        cluster.partition(followers, List.of(leaderId));

        // 老领导上的新提交无法获得多数派 -> 超时
        RaftNode staleLeader = cluster.getNode(leaderId);
        SubmitResult blocked = cluster.submitVia(staleLeader, CLIENT, "req-p", "SET p 1", 12);
        assertTrue(blocked.status() == SubmitResult.Status.TIMEOUT
                        || blocked.status() == SubmitResult.Status.TERM_CHANGED,
                "分区中的少数派领导不能提交，实际=" + blocked.status());
        System.out.println("==> 分区期间提交未提交成功: " + blocked.status()
                + " reason=" + blocked.reason());

        // 多数派选出新领导
        String newLeader = null;
        for (int i = 0; i < 40 && newLeader == null; i++) {
            cluster.tick();
            for (String id : followers) {
                if (cluster.getNode(id).getRole().name().equals("LEADER")) {
                    newLeader = id;
                }
            }
        }
        assertNotNull(newLeader, "多数派应选出新领导");
        cluster.heal();
        cluster.runUntilLeaderElected(60);

        // 相同幂等标识重试：之前从未提交成功，因此应正常提交一次，结论确定
        SubmitResult retry = cluster.submit(CLIENT, "req-p", "SET p 1", 40);
        assertEquals(SubmitResult.Status.COMMITTED, retry.status(),
                "分区恢复后重试应成功: " + retry.reason());
        SubmitResult retryAgain = cluster.submit(CLIENT, "req-p", "SET p 1", 40);
        assertEquals(SubmitResult.Status.DUPLICATE, retryAgain.status());
        assertEquals(retry.commitIndex(), retryAgain.commitIndex());
        assertEquals(retry.value(), retryAgain.value());
        System.out.println("==> 恢复后重试结论=" + retry.status()
                + "，再次重试=" + retryAgain.status() + "，下标与结果一致");

        // 所有节点 key=p 状态一致，且老领导少数派期间的未提交记录未生效
        assertTrue(cluster.waitForValue("p", "1", 40), "分区恢复修复分歧后各节点应收敛");
        for (RaftNode node : cluster.allNodes().values()) {
            assertEquals("1", node.read("p"),
                    "节点 " + node.getId() + " 的已提交状态必须一致");
        }
    }

    private static String leaderOf(RaftCluster cluster) {
        String id = cluster.getLeaderId();
        assertNotNull(id, "测试期间应存在唯一领导");
        return id;
    }
}
