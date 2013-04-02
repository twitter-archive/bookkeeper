package org.apache.bookkeeper.stats;

/**
 * The logger interface for the PerChannelBookieClient
 */
public interface PCBookieClientStatsLogger extends StatsLogger {
    public static enum PCBookieClientOp {
        ADD_ENTRY, READ_ENTRY, TIMEOUT_ADD_ENTRY, TIMEOUT_READ_ENTRY, NETTY_TIMEOUT_ADD_ENTRY, NETTY_TIMEOUT_READ_ENTRY
    }

    public static enum PCBookieSimpleStatType {
        CONNECTED_STATE
    }
}
