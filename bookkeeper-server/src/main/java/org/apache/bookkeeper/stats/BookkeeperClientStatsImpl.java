package org.apache.bookkeeper.stats;

/**
 * This class implements the BookkeeperClientStatsLogger interface using stats from twitter-common.
 * An instance of this class is responsible for stats from one bookkeeper client.
 */
public class BookkeeperClientStatsImpl extends BaseStatsImpl implements BookkeeperClientStatsLogger {
    public BookkeeperClientStatsImpl(String name) {
        super(name, BookkeeperClientOp.values(), BookkeeperClientSimpleStatType.values());
    }
}
