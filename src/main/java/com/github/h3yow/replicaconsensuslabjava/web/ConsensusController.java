package com.github.h3yow.replicaconsensuslabjava.web;

import com.github.h3yow.replicaconsensuslabjava.consensus.Cluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.CommitResult;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 本地 HTTP 入口：无需外部中间件，启动一个 Spring Boot 进程即可手动驱动并观察
 * 选举、提交、分区、故障与恢复的完整过程。
 */
@RestController
@RequestMapping("/api/consensus")
public class ConsensusController {

    private final Cluster cluster;

    public ConsensusController(Cluster cluster) {
        this.cluster = cluster;
    }

    /** 集群与各副本状态。 */
    @org.springframework.web.bind.annotation.GetMapping("/status")
    public Map<String, Object> status() {
        return cluster.status();
    }

    /** 驱动一轮选举。 */
    @PostMapping("/elect")
    public Map<String, Object> elect() {
        cluster.tickElection();
        return cluster.status();
    }

    /** 驱动一轮心跳复制。 */
    @PostMapping("/replicate")
    public Map<String, Object> replicate() {
        cluster.tickReplication();
        return cluster.status();
    }

    /**
     * 提交指令，请求体：{"node":"n1","idempotencyKey":"k1","command":"SET a 1"}。
     * node 省略时自动发往当前领导者。
     */
    @PostMapping("/submit")
    public CommitResult submit(@RequestBody Map<String, String> body) {
        String target = body.getOrDefault("node", "");
        RaftNode node = target.isBlank() ? cluster.leader() : cluster.node(target);
        if (node == null) {
            return new CommitResult(false,
                    com.github.h3yow.replicaconsensuslabjava.consensus.CommitReason.NOT_LEADER,
                    null, 0, body.get("idempotencyKey"), null, "无可用领导者");
        }
        return node.submit(body.get("idempotencyKey"), body.get("command"));
    }

    /** 分区：请求体 {"isolate":["n1"]} 或 {"sideA":["n1"],"sideB":["n2","n3"]}。 */
    @PostMapping("/partition")
    @SuppressWarnings("unchecked")
    public Map<String, Object> partition(@RequestBody Map<String, Object> body) {
        if (body.containsKey("isolate")) {
            List<String> isolated = (List<String>) body.get("isolate");
            cluster.partition(isolated.toArray(String[]::new));
        } else {
            cluster.partitionBetween((List<String>) body.get("sideA"), (List<String>) body.get("sideB"));
        }
        return cluster.status();
    }

    /** 修复全部网络分区。 */
    @PostMapping("/heal")
    public Map<String, Object> heal() {
        cluster.heal();
        return cluster.status();
    }

    @PostMapping("/nodes/{id}/crash")
    public Map<String, Object> crash(@PathVariable String id) {
        cluster.node(id).crash();
        return cluster.status();
    }

    @PostMapping("/nodes/{id}/restart")
    public Map<String, Object> restart(@PathVariable String id) {
        cluster.node(id).restart();
        return cluster.status();
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        cluster.resetAllStores();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("reset", true);
        m.put("status", cluster.status());
        return m;
    }
}
