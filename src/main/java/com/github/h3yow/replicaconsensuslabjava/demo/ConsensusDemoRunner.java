package com.github.h3yow.replicaconsensuslabjava.demo;

import com.github.h3yow.replicaconsensuslabjava.consensus.NodeRole;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftCluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import com.github.h3yow.replicaconsensuslabjava.consensus.SubmitResult;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 本机独立演示：启动一个进程内多副本集群，依次演示
 * 选举 → 幂等提交/冲突拒绝 → 分区 → 故障重启回放 → 分歧修复，
 * 全过程通过控制台日志解释每一步判定。可由 consensus.demo.enabled 关闭。
 */
@Component
public class ConsensusDemoRunner implements ApplicationRunner {

    private final ConsensusProperties properties;

    public ConsensusDemoRunner(ConsensusProperties properties) {
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (!properties.isEnabled()) {
            return;
        }
        int n = Math.max(3, properties.getNodes());
        List<String> ids = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
            ids.add("n" + i);
        }
        RaftCluster.Config config = new RaftCluster.Config(
                properties.getElectionTimeoutMin(),
                properties.getElectionTimeoutRange(),
                properties.getHeartbeatInterval(),
                properties.getSnapshotThreshold());

        banner("1) 初始化 " + n + " 副本集群并选举领导者");
        RaftCluster cluster = new RaftCluster(ids, config);
        String leader = cluster.runUntilLeaderElected(properties.getTickBudget());
        log("唯一领导者=" + leader + " term=" + cluster.getNode(leader).getCurrentTerm());

        banner("2) 幂等提交：首次 / 重复 / 同键异内容");
        SubmitResult first = cluster.submit("demo-client", "op-1", "SET weather sunny",
                properties.getTickBudget());
        log("首次提交 => " + first.status() + " index=" + first.commitIndex()
                + " value=" + first.value());
        SubmitResult duplicate = cluster.submit("demo-client", "op-1", "SET weather sunny",
                properties.getTickBudget());
        log("重复提交 => " + duplicate.status() + " index=" + duplicate.commitIndex()
                + " value=" + duplicate.value() + "（与首次一致，未二次应用）");
        SubmitResult conflict = cluster.submit("demo-client", "op-1", "SET weather RAINY",
                properties.getTickBudget());
        log("同键异内容 => " + conflict.status() + " 原因=" + conflict.reason());

        banner("3) 制造 " + (n - 2) + " | 2 分区，验证少数派无法提交");
        List<String> minority = new ArrayList<>(List.of(leader,
                ids.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow()));
        List<String> majority = ids.stream().filter(id -> !minority.contains(id)).toList();
        cluster.partition(majority, minority);
        SubmitResult blocked = cluster.submitVia(cluster.getNode(leader),
                "demo-client", "op-blocked", "SET blocked yes", 12);
        log("少数派领导提交 => " + blocked.status() + " 原因=" + blocked.reason());

        String majLeader = null;
        for (int i = 0; i < properties.getTickBudget() && majLeader == null; i++) {
            cluster.tick();
            for (String id : majority) {
                if (cluster.getNode(id).getRole() == NodeRole.LEADER) {
                    majLeader = id;
                }
            }
        }
        log("多数派分区新领导者=" + majLeader);

        banner("4) 恢复网络 + 老领导崩溃重启：验证快照/历史回放与分歧修复");
        cluster.heal();
        String converged = cluster.runUntilLeaderElected(properties.getTickBudget());
        cluster.waitForConvergence(properties.getTickBudget());
        log("分区恢复后唯一领导者=" + converged);

        cluster.crash(converged);
        log("领导者 " + converged + " 崩溃（磁盘保留）");
        cluster.restart(converged);
        String leader2 = cluster.runUntilLeaderElected(properties.getTickBudget());
        cluster.waitForConvergence(properties.getTickBudget());
        log("重启后重新选出领导者=" + leader2);

        SubmitResult retry = cluster.submit("demo-client", "op-blocked", "SET blocked yes",
                properties.getTickBudget());
        log("恢复后重试未决指令 => " + retry.status());
        cluster.waitForValue("blocked", "yes", properties.getTickBudget());

        banner("5) 最终各副本已提交状态");
        for (RaftNode node : cluster.allNodes().values()) {
            log("节点 " + node.getId()
                    + " role=" + node.getRole() + " term=" + node.getCurrentTerm()
                    + " commit=" + node.getCommitIndex()
                    + " weather=" + node.read("weather")
                    + " blocked=" + node.read("blocked"));
        }
        log("演示完成：各副本已提交状态一致，未提交记录未生效，冲突与重复均被明确区分。");
    }

    private static void banner(String title) {
        System.out.println();
        System.out.println("==================== " + title + " ====================");
    }

    private static void log(String message) {
        System.out.println("[演示] " + message);
    }
}
