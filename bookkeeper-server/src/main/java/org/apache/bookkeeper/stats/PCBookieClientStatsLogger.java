package org.apache.bookkeeper.stats;

/**
 * The logger interface for the PerChannelBookieClient
 */
public class PCBookieClientStatsLogger extends BookkeeperStatsLogger {
    public static enum PCBookieClientOp {
        ADD_ENTRY, READ_ENTRY, READ_ENTRY_AND_FENCE, READ_ENTRY_LONG_POLL, READ_LONG_POLL_RESPONSE,
        TIMEOUT_ADD_ENTRY, TIMEOUT_READ_ENTRY, NETTY_TIMEOUT_ADD_ENTRY, NETTY_TIMEOUT_READ_ENTRY,
        CHANNEL_CONNECT, CHANNEL_WRITE
    }

    public PCBookieClientStatsLogger(StatsLogger underlying) {
        super(underlying, PCBookieClientOp.values(), null, null);
    }
}
