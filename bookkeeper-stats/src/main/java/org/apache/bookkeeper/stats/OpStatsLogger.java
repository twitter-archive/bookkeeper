package org.apache.bookkeeper.stats;

/**
 * This interface handles logging of statistics related to each operation (PUBLISH,
 * CONSUME etc.)
 */
public interface OpStatsLogger {

    /**
     * Increment the failed op counter with the given eventLatencyMillis.
     * @param eventLatencyMicros The event latency in microseconds.
     */
    public void registerFailedEvent(long eventLatencyMicros);

    /**
     * An operation succeeded with the given eventLatencyMillis. Update
     * stats to reflect the same
     * @param eventLatencyMicros The event latency in microseconds.
     */
    public void registerSuccessfulEvent(long eventLatencyMicros);

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
