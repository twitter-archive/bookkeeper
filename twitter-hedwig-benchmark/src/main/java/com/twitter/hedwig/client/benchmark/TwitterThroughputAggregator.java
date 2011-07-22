package com.twitter.hedwig.client.benchmark;

import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stats aggregator for non-callback operations. Measures just throughput.
 */
public class TwitterThroughputAggregator {
    private static final Logger logger = Logger.getLogger(TwitterThroughputAggregator.class);

    private final String label;
    private AtomicInteger done = new AtomicInteger();
    private AtomicInteger numFailed = new AtomicInteger();

    public TwitterThroughputAggregator(final String label,
        final long statsReportingIntervalInMilliSecs) {
        this.label = label;
        if (Boolean.getBoolean("progress")) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    long lastReportingTime = System.currentTimeMillis();
                    int prevSnapDone = done.get();
                    while(true) {
                        TwitterBenchmarkUtils.sleep(statsReportingIntervalInMilliSecs);
                        long tmpReportingTime = System.currentTimeMillis();
                        int tmpDone = done.get();
                        summarize(tmpReportingTime - lastReportingTime, tmpDone - prevSnapDone);
                        prevSnapDone = tmpDone;
                        lastReportingTime = tmpReportingTime;
                    }
                }
            }).start();
        }
    }

    public void ding(boolean failed) {
        int snapDone = done.incrementAndGet();
        if (failed)
            numFailed.incrementAndGet();
        if (logger.isDebugEnabled())
            logger.debug(label + " " + (failed ? "failed" : "succeeded") + ", done so far = " + snapDone);
    }

    public void summarize(long intervalInMilliSecs, int numDone) {
        logger.info("Finished " + label + ": count = " + numDone + ", time interval = " +
                    intervalInMilliSecs + "ms, tp = " + 1000.0*numDone/intervalInMilliSecs +
                    "ops/sec,  numFailed = " + numFailed);
    }
}