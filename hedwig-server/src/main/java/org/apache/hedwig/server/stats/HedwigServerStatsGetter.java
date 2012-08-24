package org.apache.hedwig.server.stats;

import org.apache.hedwig.protocol.PubSubProtocol;

/**
 * This interface lets us implement multiple stat collection backends without affecting stat collection
 * through JMX. We should remove this if/when we decide to move away from exporting stats through JMX.
 */
public interface HedwigServerStatsGetter {

    /**
     * Get the OpStatsData for "type".
     */
    public OpStatsData getOpStatsData(PubSubProtocol.OperationType type);
    /**
     * @return Get the total number of requests that we have received till now
     */
    public long getNumRequestsReceived();

    /**
     * @return Get the total number of requests that have been redirected
     */
    public long getNumRequestsRedirect();

    /**
     * @return Get the total number of messages we have delivered till now
     */
    public long getNumMessagesDelivered();

    /**
     * @return Get the number of topics that this hub is responsible for
     */
    public long getNumTopics();

    /**
     * @return Get the bookkeeper persist queue size. This gives us an idea if requests to
     * Bookkeeper are backed up
     */
    public long getPersistQueueSize();
}
