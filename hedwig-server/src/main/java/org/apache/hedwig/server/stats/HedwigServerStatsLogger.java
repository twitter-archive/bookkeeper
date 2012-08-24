package org.apache.hedwig.server.stats;

import org.apache.hedwig.protocol.PubSubProtocol;

/**
 * Any backend that logs hedwig stats should implement this interface.
 * The Getter and Logger interfaces are different because some backends might export
 * stats internally and the getters would have to be no-ops.
 */
public interface HedwigServerStatsLogger {

    /**
     * @return Get the OpStatsLogger for a particular operation type
     */
    public OpStatsLogger getOpStatsLogger(PubSubProtocol.OperationType type);

    /**
     * @return Get the {@link SimpleStatImpl} representing the total requests received
     */
    public SimpleStat getRequestsReceivedLogger();

    /**
     * @return Get the {@link SimpleStatImpl} representing the total redirected requests
     */
    public SimpleStat getRequestsRedirectLogger();

    /**
     * @return Get the {@link SimpleStatImpl} representing the delivered messages
     */
    public SimpleStat getMessagesDeliveredLogger();

    /**
     * @return Get the {@link SimpleStatImpl} representing the number of topics
     */
    public SimpleStat getNumTopicsLogger();

    /**
     * @return Get the {@link SimpleStatImpl} representing the persist queue size
     */
    public SimpleStat getPersistQueueSizeLogger();

    /**
     * @return Get the {@link SimpleStatImpl} representing the number of subscriptions
     */
    public SimpleStat getNumSubscriptionsLogger();

    /**
     * Clear state.
     */
    public void clear();
}
