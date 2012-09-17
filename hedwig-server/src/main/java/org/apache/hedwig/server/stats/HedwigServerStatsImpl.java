package org.apache.hedwig.server.stats;

import org.apache.bookkeeper.stats.BaseStatsImpl;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

/**
 * This class implements the HedwigServerStatsLogger and HedwigServerStatsGetter interfaces.
 * It's a singleton responsible for stats for the entire server. Internals are implemented
 * using twitter.common.stats
 * TODO(Aniruddha): Add support for exporting ReadAheadCache stats.
 */
public class HedwigServerStatsImpl extends BaseStatsImpl implements HedwigServerStatsGetter, HedwigServerStatsLogger {

    public HedwigServerStatsImpl(String name) {
        super(name, OperationType.values(), HedwigServerSimpleStatType.values());
    }

    // The HedwigServerStatsGetter functions
    @Override
    public OpStatsData getOpStatsData(OperationType type) {
        return getOpStatsLogger(type).toOpStatsData();
    }

    @Override
    public long getNumRequestsReceived() {
        return getSimpleStatLogger(HedwigServerSimpleStatType.TOTAL_REQUESTS_RECEIVED).get();
    }

    @Override
    public long getNumRequestsRedirect() {
        return getSimpleStatLogger(HedwigServerSimpleStatType.TOTAL_REQUESTS_REDIRECT).get();
    }

    @Override
    public long getNumMessagesDelivered() {
        return getSimpleStatLogger(HedwigServerSimpleStatType.TOTAL_MESSAGES_DELIVERED).get();
    }

    @Override
    public long getNumTopics() {
        return getSimpleStatLogger(HedwigServerSimpleStatType.NUM_TOPICS).get();
    }

    @Override
    public long getPersistQueueSize() {
        return getSimpleStatLogger(HedwigServerSimpleStatType.PERSIST_QUEUE).get();
    }
}
