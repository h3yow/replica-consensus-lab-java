package com.github.h3yow.replicaconsensuslabjava.consensus;

/**
 * 日志条目：一条由领导者复制、多数派提交后才会应用的指令记录。
 */
public class LogEntry implements java.io.Serializable {
    private final long term;
    private final long index;
    private final String clientId;
    private final String requestId;
    private final String command;

    public LogEntry(long term, long index, String clientId, String requestId, String command) {
        this.term = term;
        this.index = index;
        this.clientId = clientId;
        this.requestId = requestId;
        this.command = command;
    }

    public long getTerm() { return term; }
    public long getIndex() { return index; }
    public String getClientId() { return clientId; }
    public String getRequestId() { return requestId; }
    public String getCommand() { return command; }

    @Override
    public String toString() {
        return "LogEntry{index=" + index + ", term=" + term
                + ", id=" + clientId + ":" + requestId + ", cmd='" + command + "'}";
    }
}
