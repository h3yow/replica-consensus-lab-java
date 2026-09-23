package com.github.h3yow.replicaconsensuslabjava.consensus;

/** 副本节点在 Raft 协议中的角色。 */
public enum NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
