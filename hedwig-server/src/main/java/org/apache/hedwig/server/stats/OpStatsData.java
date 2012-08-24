package org.apache.hedwig.server.stats;

/**
 * This class provides a read view of operation specific stats.
 * We expose this to JMX.
 * We use primitives because the class has to conform to CompositeViewData.
 */
public class OpStatsData {
    private final long numSuccessfulEvents, numFailedEvents;
    private final double avgLatency;
    // 10.0 50.0, 90.0, 99.0, 99.9, 99.99 in that order.
    // TODO(Aniruddha): Figure out if we can use a Map
    long[] percentileLatencies;
    public OpStatsData (long numSuccessfulEvents, long numFailedEvents,
                        double avgLatency, long[] percentileLatencies) {
        this.numSuccessfulEvents = numSuccessfulEvents;
        this.numFailedEvents = numFailedEvents;
        this.avgLatency = avgLatency;
        this.percentileLatencies = percentileLatencies;
    }

    public long getP10Latency() {
        return this.percentileLatencies[0];
    }
    public long getP50Latency() {
        return this.percentileLatencies[1];
    }

    public long getP90Latency() {
        return this.percentileLatencies[2];
    }

    public long getP99Latency() {
        return this.percentileLatencies[3];
    }

    public long getP999Latency() {
        return this.percentileLatencies[4];
    }

    public long getP9999Latency() {
        return this.percentileLatencies[5];
    }

    public long getNumSuccessfulEvents() {
        return this.numSuccessfulEvents;
    }

    public long getNumFailedEvents() {
        return this.numFailedEvents;
    }

    public double getAvgLatency() {
        return this.avgLatency;
    }
}
