package org.apache.bookkeeper.stats;

/**
 * Any backend that logs bookkeeper client stats should implement this interface.
 */
public class BookkeeperClientStatsLogger extends BookkeeperStatsLogger {

    /**
     * An enum for the operations that can be logged by this logger.
     */
    public static enum BookkeeperClientOp {
        ADD_ENTRY, ADD_COMPLETE, NUM_ADDS_SUBMITTED_PER_CALLBACK,
        READ_ENTRY, READ_LAST_CONFIRMED,
        TRY_READ_LAST_CONFIRMED, READ_LAST_CONFIRMED_LONG_POLL,
        READ_LAST_CONFIRMED_AND_ENTRY, READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE,
        READ_LAST_CONFIRMED_AND_ENTRY_HIT, READ_LAST_CONFIRMED_AND_ENTRY_MISS,
        SPECULATIVES_PER_READ, SPECULATIVES_PER_READ_LAC,
        ENSEMBLE_CHANGE, LEDGER_CREATE, LEDGER_OPEN, LEDGER_OPEN_RECOVERY, LEDGER_DELETE, LEDGER_CLOSE,
        LEDGER_RECOVER, LEDGER_RECOVER_READ_ENTRIES, LEDGER_RECOVER_ADD_ENTRIES,
        TIMEOUT_ADD_ENTRY
    }

    public static enum BookkeeperClientCounter {
        NUM_ENSEMBLE_CHANGE, NUM_OPEN_LEDGERS, NUM_PENDING_ADD, LAC_UPDATE_HITS, LAC_UPDATE_MISSES
    }

    public BookkeeperClientStatsLogger(StatsLogger underlying) {
        super(underlying, BookkeeperClientOp.values(), BookkeeperClientCounter.values(), null);
    }
}

