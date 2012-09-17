package org.apache.hedwig.server.stats;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.SimpleStat;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.hedwig.protocol.PubSubProtocol;

/**
 * Any backend that logs hedwig stats should implement this interface.
 * The Getter and Logger interfaces are different because some backends might export
 * stats internally and the getters would have to be no-ops.
 */
public interface HedwigServerStatsLogger extends StatsLogger {

    /**
     * An enum representing the simple stats logged by this logger
     */
    public static enum HedwigServerSimpleStatType {
        TOTAL_REQUESTS_RECEIVED, TOTAL_REQUESTS_REDIRECT, TOTAL_MESSAGES_DELIVERED,
        NUM_TOPICS, PERSIST_QUEUE, NUM_SUBSCRIPTIONS
    }
}

