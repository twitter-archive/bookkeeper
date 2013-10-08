package org.apache.bookkeeper.stats;

import org.apache.commons.configuration.Configuration;

public class NullStatsProvider implements StatsProvider {

    final StatsLogger nullStatsLogger = new NullStatsLogger();

    @Override
    public void start(Configuration conf) {
        // nop
    }

    @Override
    public void stop() {
        // nop
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return nullStatsLogger;
    }

}
