package org.apache.bookkeeper.stats;

/**
 * Any backend that logs bookkeeper client stats should implement this interface.
 */
public interface BookkeeperClientStatsLogger extends StatsLogger {

    /**
     * An enum for the operations that can be logged by this logger.
     */
    public static enum BookkeeperClientOp {
        ADD_ENTRY, READ_ENTRY
    }

    public static enum BookkeeperClientSimpleStatType {
        NUM_ENSEMBLE_CHANGE, NUM_OPEN_LEDGERS, NUM_PENDING_ADD, NUM_PERMITS_TAKEN
    }
}

