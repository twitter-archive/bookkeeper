package org.apache.bookkeeper.stats.twitter.ostrich;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import java.util.HashSet;

import static org.junit.Assert.assertTrue;

public class TestOstrichProviderShutdownServiceTracker {

    @Test(timeout = 60000)
    public void testOstrichProviderWhenShutdownServiceTracker() throws Exception {
        OstrichProvider op = new OstrichProvider("");
        Configuration conf = new CompositeConfiguration();
        conf.setProperty(OstrichProvider.STATS_EXPORT, true);
        conf.setProperty(OstrichProvider.STATS_HTTP_PORT, 0);
        conf.setProperty(OstrichProvider.SHOULD_SHUTDOWN_SERVICE_TRACKER, true);
        op.start(conf);
        op.stop();

        HashSet<Thread> threadset = new HashSet<Thread>();
        int threadCount = Thread.activeCount();
        Thread threads[] = new Thread[threadCount*2];
        threadCount = Thread.enumerate(threads);
        for(int i = 0; i < threadCount; i++) {
            if (threads[i].getName().contains("LatchedStatsListener")) {
                threadset.add(threads[i]);
            }
        }

        assertTrue("Should not find LatchedStatsListener.", threadset.isEmpty());
    }
}
