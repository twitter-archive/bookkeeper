package org.apache.bookkeeper.stats;

import com.twitter.common.stats.*;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of the OpStatsLogger interface that handles per operation type stats.
 * Internals use twitter.common.stats for exporting metrics.
 */
public class OpStatsLoggerImpl implements OpStatsLogger {
    private final String name;
    private final RequestStats events;

    public OpStatsLoggerImpl(String name) {
        this.name = name;
        this.events = new RequestStats(name);
    }

    // OpStatsLogger functions
    public void registerFailedEvent(long eventLatencyMillis) {
        this.events.incErrors(TimeUnit.MILLISECONDS.toMicros(eventLatencyMillis));
    }

    public void registerSuccessfulEvent(long eventLatencyMillis) {
        this.events.requestComplete(TimeUnit.MILLISECONDS.toMicros(eventLatencyMillis));
    }

    public synchronized void clear() {
        //TODO(Aniruddha): Figure out how to clear RequestStats. Till then this is a no-op
    }

    /**
     * This function should go away soon (hopefully).
     */
    public synchronized OpStatsData toOpStatsData() {
        long numFailed = this.events.getErrorCount();
        long numSuccess = this.events.getSlidingStats().getEventCounter().get() - numFailed;
        double avgLatencyMillis = this.events.getSlidingStats().getPerEventLatency().read() / 1000.0;
        double[] default_percentiles = {10, 50, 90, 99, 99.9, 99.99};
        long[] latenciesMillis = new long[default_percentiles.length];
        Arrays.fill(latenciesMillis, Long.MAX_VALUE);
        Map<Double, ? extends Stat> realPercentileLatencies =
                this.events.getPercentile().getPercentiles();
        for (int i = 0; i < default_percentiles.length; i++) {
            if (realPercentileLatencies.containsKey(default_percentiles[i])) {
                @SuppressWarnings("unchecked")
                Stat<Double> latency = realPercentileLatencies.get(default_percentiles[i]);
                latenciesMillis[i] = TimeUnit.MICROSECONDS.toMillis(latency.read().longValue());
            }
        }
        return new OpStatsData(numSuccess, numFailed, avgLatencyMillis, latenciesMillis);
    }
}
