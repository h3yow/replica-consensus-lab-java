package com.github.h3yow.replicaconsensuslabjava.tests;

import com.github.h3yow.replicaconsensuslabjava.consensus.Cluster;
import com.github.h3yow.replicaconsensuslabjava.consensus.ConsensusProperties;
import com.github.h3yow.replicaconsensuslabjava.consensus.NodeRole;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftMessages;
import com.github.h3yow.replicaconsensuslabjava.consensus.RaftNode;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 显式验证选举安全（Election Safety）与领导者唯一性：
 * 同一任期内第二个“领导者”的 AppendEntries 必须因任期过期被拒绝；
 * 只有更高任期的合法请求能让节点更新领导者认知。
 */
class SplitBrainRejectTest {

    private static final Logger log = LoggerFactory.getLogger(SplitBrainRejectTest.class);

    @Test
    void staleLeaderSameTermIsRejectedAndHigherTermConverges() {
        ConsensusProperties props = new ConsensusProperties();
        props.setDataDir("target/consensus-test/splitbrain-" + System.nanoTime());
        Cluster cluster = new Cluster(List.of("n1", "n2", "n3"), props.getDataDir(), props);
        cluster.resetAllStores();

        RaftNode leader = cluster.awaitLeader(5);
        assertNotNull(leader);
        long term = leader.currentTerm();
        RaftNode follower = cluster.nodes().stream()
                .filter(n -> !n.id().equals(leader.id())).findFirst().orElseThrow();
        log.info(">>> 合法领导者={} term={}，跟随者={}", leader.id(), term, follower.id());

        // 伪造一个“同任期的第二个领导者”（旧分区领导者视角），其心跳必须被拒绝。
        RaftMessages.AppendRequest stale = new RaftMessages.AppendRequest(
                "impostor", term, null, follower.lastLogIndex(),
                follower.lastLogTerm(), List.of(), follower.commitIndex());
        RaftMessages.AppendResponse denied = follower.handleAppend(stale);
        log.info(">>> 同任期冒充领导者 AppendEntries 被拒: success={} term={}",
                denied.success(), denied.term());
        assertFalse(denied.success(), "同任期第二个领导者必须被拒绝");
        assertEquals(term, denied.term());
        // 跟随者不得因此改变对合法领导者的认知。
        assertEquals(leader.id(), follower.leaderHint());

        // 更高任期的合法请求应被接受，节点退回跟随者并认可新领导者。
        RaftMessages.AppendRequest legitimate = new RaftMessages.AppendRequest(
                "n3", term + 1, null, follower.lastLogIndex(),
                follower.lastLogTerm(), List.of(), follower.commitIndex());
        RaftMessages.AppendResponse accepted = follower.handleAppend(legitimate);
        log.info(">>> 更高任期 term={} 的合法 AppendEntries: success={}", term + 1, accepted.success());
        assertTrue(accepted.success());
        assertEquals(term + 1, follower.currentTerm());
        assertEquals(NodeRole.FOLLOWER, follower.role());

        // 集群视图任一时刻最多一个存活领导者。
        long leaders = cluster.nodes().stream()
                .filter(n -> n.alive() && n.role() == NodeRole.LEADER).count();
        assertTrue(leaders <= 1, "存活领导者数量必须 <= 1");
    }
}
