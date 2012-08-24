package org.apache.hedwig.server.stats;

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Stats;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * This class implements the HedwigServerStatsLogger and HedwigServerStatsGetter interfaces.
 * It's a singleton responsible for stats for the entire server. Internals are implemented
 * using twitter.common.stats
 * TODO(Aniruddha): Add support for exporting ReadAheadCache stats.
 */
public class HedwigServerStatsImpl implements HedwigServerStatsGetter, HedwigServerStatsLogger {

    // A singleton. This instance is responsible for storing stats for the entire server.
    private static final Logger LOG = LoggerFactory.getLogger(HedwigServerStatsImpl.class.getName());
    private final String name;
    private final SimpleStat totalRequestsReceived;
    private final SimpleStat totalRequestsRedirect;
    private final SimpleStat totalMessagesDelivered;
    private final SimpleStat numTopics;
    private final SimpleStat persistQueueSize;
    private final SimpleStat numSubscriptions;
    private final Map<OperationType, OpStatsLogger> opStatsLoggerMap = new HashMap<OperationType, OpStatsLogger>();

    public HedwigServerStatsImpl(String name) {
        this.name = name;
        this.totalRequestsReceived = new SimpleStatImpl(this.name + "_total_requests_received");
        this.totalRequestsRedirect = new SimpleStatImpl(this.name + "_total_requests_redirect");
        this.totalMessagesDelivered = new SimpleStatImpl(this.name + "_total_messages_delivered");
        this.numTopics = new SimpleStatImpl(this.name + "_num_topics");
        this.persistQueueSize = new SimpleStatImpl(this.name + "_persist_queue");
        this.numSubscriptions = new SimpleStatImpl(this.name + "_num_subscriptions");
        for (OperationType type : OperationType.values()) {
            this.opStatsLoggerMap.put(type, new OpStatsLoggerImpl(this.name + "_" + type.name().toLowerCase()));
        }
    }

    // The HedwigServerStatsLogger interface functions
    @Override
    public OpStatsLogger getOpStatsLogger(OperationType type) {
        return this.opStatsLoggerMap.get(type);
    }

    @Override
    public SimpleStat getRequestsReceivedLogger() {
        return this.totalRequestsReceived;
    }

    @Override
    public SimpleStat getRequestsRedirectLogger() {
        return this.totalRequestsRedirect;
    }

    @Override
    public SimpleStat getMessagesDeliveredLogger() {
        return this.totalMessagesDelivered;
    }

    @Override
    public SimpleStat getNumTopicsLogger() {
        return this.numTopics;
    }

    @Override
    public SimpleStat getPersistQueueSizeLogger() {
        return this.persistQueueSize;
    }

    @Override
    public SimpleStat getNumSubscriptionsLogger() {
        return this.numSubscriptions;
    }

    @Override
    public synchronized void clear() {
        this.totalRequestsReceived.clear();
        this.totalRequestsRedirect.clear();
        this.totalMessagesDelivered.clear();
        this.numTopics.clear();
        this.persistQueueSize.clear();
        for (OpStatsLogger statLogger : this.opStatsLoggerMap.values()) {
            statLogger.clear();
        }
    }

    // The HedwigServerStatsGetter functions
    @Override
    public OpStatsData getOpStatsData(OperationType type) {
        return this.opStatsLoggerMap.get(type).toOpStatsData();
    }

    @Override
    public long getNumRequestsReceived() {
        return this.totalRequestsReceived.get();
    }

    @Override
    public long getNumRequestsRedirect() {
        return this.totalRequestsRedirect.get();
    }

    @Override
    public long getNumMessagesDelivered() {
        return this.totalMessagesDelivered.get();
    }

    @Override
    public long getNumTopics() {
        return this.numTopics.get();
    }

    @Override
    public long getPersistQueueSize() {
        return this.persistQueueSize.get();
    }
}
