package org.apache.hedwig.server.stats;

/**
 * This interface handles logging of statistics related to each operation (PUBLISH,
 * CONSUME etc.)
 */
public interface OpStatsLogger {

    /**
     * Increment the failed op counter.
     */
    public void registerFailedEvent();

    /**
     * An operation succeeded with the given latency. Update
     * stats to reflect the same
     * @param latency
     */
    public void registerSuccessfulEvent(long latency);

    /**
     * @return Returns an OpStatsData object with necessary values. We need this function
     * to support JMX exports. This should be deprecated sometime in the near future.
     * populated.
     */
    public OpStatsData toOpStatsData();

    /**
     * Clear stats for this operation.
     */
    public void clear();
}
