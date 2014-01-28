package org.apache.bookkeeper.stats.twitter.ostrich;

import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;

class OpStatsLoggerImpl implements OpStatsLogger {

    static final double[] PERCENTILES = new double[] {
            0.1, 0.5, 0.9, 0.99, 0.999, 0.9999
    };

    private final String scope;
    private final com.twitter.ostrich.stats.Counter successCounter;
    private final com.twitter.ostrich.stats.Counter failureCounter;
    private final com.twitter.ostrich.stats.Metric successMetric;
    private final com.twitter.ostrich.stats.Metric failureMetric;

    OpStatsLoggerImpl(String scope, com.twitter.ostrich.stats.StatsProvider statsProvider) {
        this.scope = scope;
        successCounter = statsProvider.getCounter(statName("requests/success"));
        failureCounter = statsProvider.getCounter(statName("requests/failure"));
        successMetric = statsProvider.getMetric(statName("latency/success"));
        failureMetric = statsProvider.getMetric(statName("latency/failure"));
    }

    private String statName(String statName) {
        return String.format("%s/%s", scope, statName);
    }

    @Override
    public void registerFailedEvent(long eventLatencyMicros) {
        failureMetric.add((int) eventLatencyMicros);
        failureCounter.incr();
    }

    @Override
    public void registerSuccessfulEvent(long eventLatencyMicros) {
        successMetric.add((int)eventLatencyMicros);
        successCounter.incr();
    }

    @Override
    public OpStatsData toOpStatsData() {
        long numSuccess = successCounter.apply();
        long numFailures = failureCounter.apply();
        com.twitter.ostrich.stats.Distribution distribution = successMetric.apply();
        com.twitter.ostrich.stats.Histogram histogram = distribution.histogram();
        double avgLatency = distribution.average();
        long[] percentiles = new long[PERCENTILES.length];
        int i = 0;
        for (double percentile : PERCENTILES) {
            percentiles[i] = histogram.getPercentile(percentile);
            ++i;
        }
        return new OpStatsData(numSuccess, numFailures, avgLatency, percentiles);
    }

    @Override
    public void clear() {
        successCounter.reset();
        failureCounter.reset();
        successMetric.clear();
        failureMetric.clear();
    }
}
