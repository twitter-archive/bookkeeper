package org.apache.bookkeeper.stats;

/**
 * Any backend that logs bookkeeper client stats should implement this interface.
 */
public interface BookkeeperClientStatsLogger extends StatsLogger {

    /**
     * An enum for the operations that can be logged by this logger.
     */
    public static enum BookkeeperClientOp {
        ADD_ENTRY, READ_ENTRY, ENSEMBLE_CHANGE, LEDGER_CREATE, LEDGER_OPEN, LEDGER_DELETE
    }

    public static enum BookkeeperClientSimpleStatType {
        NUM_ENSEMBLE_CHANGE, NUM_OPEN_LEDGERS, NUM_PENDING_ADD
    }
}

