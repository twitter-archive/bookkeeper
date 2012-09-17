package org.apache.bookkeeper.stats;

/**
 * This class provides a read view of operation specific stats.
 * We expose this to JMX.
 * We use primitives because the class has to conform to CompositeViewData.
 */
public class OpStatsData {
    private final long numSuccessfulEvents, numFailedEvents;
    // All latency values are in Milliseconds.
    private final double avgLatencyMillis;
    // 10.0 50.0, 90.0, 99.0, 99.9, 99.99 in that order.
    // TODO(Aniruddha): Figure out if we can use a Map
    long[] percentileLatenciesMillis;
    public OpStatsData (long numSuccessfulEvents, long numFailedEvents,
                        double avgLatencyMillis, long[] percentileLatenciesMillis) {
        this.numSuccessfulEvents = numSuccessfulEvents;
        this.numFailedEvents = numFailedEvents;
        this.avgLatencyMillis = avgLatencyMillis;
        this.percentileLatenciesMillis = percentileLatenciesMillis;
    }

    public long getP10Latency() {
        return this.percentileLatenciesMillis[0];
    }
    public long getP50Latency() {
        return this.percentileLatenciesMillis[1];
    }

    public long getP90Latency() {
        return this.percentileLatenciesMillis[2];
    }

    public long getP99Latency() {
        return this.percentileLatenciesMillis[3];
    }

    public long getP999Latency() {
        return this.percentileLatenciesMillis[4];
    }

    public long getP9999Latency() {
        return this.percentileLatenciesMillis[5];
    }

    public long getNumSuccessfulEvents() {
        return this.numSuccessfulEvents;
    }

    public long getNumFailedEvents() {
        return this.numFailedEvents;
    }

    public double getAvgLatencyMillis() {
        return this.avgLatencyMillis;
    }
}
