package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * 幂等记录：指令应用后为 (clientId, requestId) 保存结论，用于重复提交去重。
 */
public class IdempotencyRecord implements java.io.Serializable {
    private final String clientId;
    private final String requestId;
    private final String commandHash;
    private final long commitIndex;
    private final String result;

    public IdempotencyRecord(String clientId, String requestId, String commandHash,
                             long commitIndex, String result) {
        this.clientId = clientId;
        this.requestId = requestId;
        this.commandHash = commandHash;
        this.commitIndex = commitIndex;
        this.result = result;
    }

    public String getClientId() { return clientId; }
    public String getRequestId() { return requestId; }
    public String getCommandHash() { return commandHash; }
    public long getCommitIndex() { return commitIndex; }
    public String getResult() { return result; }

    @Override
    public String toString() {
        return "IdempotencyRecord{id=" + clientId + ":" + requestId
                + ", hash='" + commandHash + "', commitIndex=" + commitIndex + "}";
    }
}
