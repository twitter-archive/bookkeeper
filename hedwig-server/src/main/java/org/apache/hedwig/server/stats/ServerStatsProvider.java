package org.apache.hedwig.server.stats;

/**
 * An abstraction that provides us with the singleton instance on which to operate.
 */
public class ServerStatsProvider {
    private static HedwigServerStatsImpl instance = new HedwigServerStatsImpl("hedwig_server");
    public static HedwigServerStatsLogger getStatsLoggerInstance() {
        return instance;
    }

    public static HedwigServerStatsGetter getStatsGetterInstance() {
        return instance;
    }
}
