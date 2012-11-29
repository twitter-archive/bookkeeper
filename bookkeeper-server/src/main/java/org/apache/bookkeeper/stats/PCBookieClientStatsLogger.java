package org.apache.bookkeeper.stats;

/**
 * The logger interface for the PerChannelBookieClient
 */
public interface PCBookieClientStatsLogger extends StatsLogger {
    public static enum PCBookieClientOp {
        ADD_ENTRY, READ_ENTRY, RANGE_READ_ENTRY
    }

    public static enum PCBookieSimpleStatType {
        CONNECTED_STATE
    }
}
