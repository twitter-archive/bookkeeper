package org.apache.hedwig.server.stats;

import com.twitter.common.stats.*;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An implementation of the OpStatsLogger interface that handles per operation type stats.
 * Internals use twitter.common.stats for exporting metrics.
 */
public class OpStatsLoggerImpl implements OpStatsLogger {
    private final String name;

    // Use RequestStats to only log successful operations. Percentiles might get skewed
    // if we don't do this because RequestStats considers errors having occurred with a
    // latency of 0.
    private final RequestStats successfulEvents;

    // Handle failed operations ourselves
    private final AtomicLong numFailedEvents = new AtomicLong(0);
    private final AtomicLong numTotalEvents = new AtomicLong(0);


    OpStatsLoggerImpl(String name) {
        this.name = name;
        this.successfulEvents = new RequestStats(name + "_success");
        setUpStatsExport();
    }

    // Set up stat exports
    private void setUpStatsExport() {
        // Total events per sec
        Stats.export(Rate.of(name + "_total_events_per_sec", numTotalEvents).build());
        Stats.export(name + "_total_events", numTotalEvents);
        // Total failed events per second
        Stats.export(Rate.of(name + "_failed_events_per_sec", numFailedEvents).build());
        Stats.export(name + "_total_failed_events", numFailedEvents);
    }

    // OpStatsLogger functions
    public void registerFailedEvent() {
        this.numFailedEvents.incrementAndGet();
    }

    public void registerSuccessfulEvent(long eventLatency) {
        this.numTotalEvents.incrementAndGet();
        // We store our latency in milliseconds for now.
        // TODO(Aniruddha): Change it to microseconds and update viz
        this.successfulEvents.requestComplete(eventLatency);
    }

    public synchronized void clear() {
        //TODO(Aniruddha): Figure out how to clear RequestStats. Till then this is a no-op
    }

    /**
     * This function should go away soon (hopefully).
     */
    public synchronized OpStatsData toOpStatsData() {
        long numSuccess = this.successfulEvents.getSlidingStats().getEventCounter().get();
        long numFailed = this.numFailedEvents.get();
        double avgLatency = this.successfulEvents.getSlidingStats().getPerEventLatency().read();
        double[] default_percentiles = {10, 50, 90, 99, 99.9, 99.99};
        long[] latencies = new long[default_percentiles.length];
        Arrays.fill(latencies, Long.MAX_VALUE);
        Map<Double, ? extends Stat> realPercentileLatencies =
                this.successfulEvents.getPercentile().getPercentiles();
        for (int i = 0; i < default_percentiles.length; i++) {
            if (realPercentileLatencies.containsKey(default_percentiles[i])) {
                @SuppressWarnings("unchecked")
                Stat<Double> latency = realPercentileLatencies.get(default_percentiles[i]);
                latencies[i] = (latency.read()).longValue();
            }
        }
        return new OpStatsData(numSuccess, numFailed, avgLatency, latencies);
    }
}
