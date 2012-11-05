package org.apache.bookkeeper.stats;

/**
 * An abstraction that provides us with the singleton instance on which to operate.
 */
public class ServerStatsProvider {
    private static BookkeeperServerStatsImpl instance
            = new BookkeeperServerStatsImpl("bookkeeper_server");
    public static BookkeeperServerStatsLogger getStatsLoggerInstance() {
        return instance;
    }
}
