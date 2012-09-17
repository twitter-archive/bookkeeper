package org.apache.bookkeeper.stats;

/**
 * A simple interface that exposes just 2 useful methods. One to get the logger for an Op stat
 * and another to get the logger for a simple stat
 */
public interface StatsLogger {
    /**
     * @param type
     * @return Get the logger for an OpStat described by the enum type.
     */
    public OpStatsLogger getOpStatsLogger(Enum type);

    /**
     * @param type
     * @return Get the logger for a simple stat described by the enum type.
     */
    public SimpleStat getSimpleStatLogger(Enum type);

    /**
     * Clear state
     */
    public void clear();
}
