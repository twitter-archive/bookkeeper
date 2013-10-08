package org.apache.bookkeeper.stats;

import org.apache.commons.configuration.Configuration;

/**
 * An abstraction that provides us with the singleton instance on which to operate.
 */
public class ServerStatsProvider {
    private static String SCOPE_BOOKKEEPER_SERVER = "bookkeeper_server";

    private static BookkeeperServerStatsLogger instance
            = new BookkeeperServerStatsLogger(Stats.get().getStatsLogger(SCOPE_BOOKKEEPER_SERVER));

    public static StatsProvider initialize(Configuration conf) {
        Stats.loadStatsProvider(conf);
        StatsProvider statsProvider = Stats.get();
        instance = new BookkeeperServerStatsLogger(statsProvider.getStatsLogger(SCOPE_BOOKKEEPER_SERVER));
        return Stats.get();
    }

    public static BookkeeperServerStatsLogger getStatsLoggerInstance() {
        return instance;
    }
}
