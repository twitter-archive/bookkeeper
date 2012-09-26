package org.apache.hedwig.server.stats;

import com.google.protobuf.ByteString;
import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.stats.BaseStatsImpl;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.util.Pair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class implements the HedwigServerStatsLogger and HedwigServerStatsGetter interfaces.
 * It's a singleton responsible for stats for the entire server. Internals are implemented
 * using twitter.common.stats
 * TODO(Aniruddha): Add support for exporting ReadAheadCache stats.
 */
public class HedwigServerStatsImpl extends BaseStatsImpl implements HedwigServerStatsGetter, HedwigServerStatsLogger {

    // Maps a topic to the number of pending messages to be delivered. Maps to -1 if we don't have ownership
    // for a topic that we had earlier acquired.
    ConcurrentMap<PerTopicStatType, ConcurrentMap<ByteString, PerTopicStat>> perTopicLoggerMap
            = new ConcurrentHashMap<PerTopicStatType, ConcurrentMap<ByteString, PerTopicStat>>();

    public HedwigServerStatsImpl(String name) {
        super(name, OperationType.values(), HedwigServerSimpleStatType.values());
        for (PerTopicStatType type : PerTopicStatType.values()) {
            perTopicLoggerMap.put(type, new ConcurrentHashMap<ByteString, PerTopicStat>());
        }
        setUpStats();
    }

    /**
     * Set up any stats that are not already exported by our stat types.
     */
    private void setUpStats() {
        // Exports the maximum value for messages pending delivery across all topics.
        SampledStat<Long> localPendingStat = new SampledStat<Long>(name + "_max_pending_delivery", 0L) {
            @Override
            public Long doSample() {
                ConcurrentMap<ByteString, PerTopicStat> topicMap = ServerStatsProvider
                        .getStatsLoggerInstance().getPerTopicLogger(PerTopicStatType.LOCAL_PENDING);
                long maxPending = 0L;
                for (PerTopicStat _value : topicMap.values()) {
                    PerTopicPendingMessageStat value = (PerTopicPendingMessageStat)_value;
                    AtomicLong pending;
                    if (null == (pending = value.getPending())) {
                        continue;
                    }
                    maxPending = Math.max(maxPending, pending.get());
                }
                return maxPending;
            }
        };
        Stats.export(localPendingStat);
        // Export the max age of the last seen message across any region. Takes the maximum across all topics.
        //TODO(Aniruddha): Export this stat per region.
        SampledStat<Long> maxAgeCrossRegion = new SampledStat<Long>(name + "_max_age_cross_region", 0L) {
            @Override
            public Long doSample() {
                ConcurrentMap<ByteString, PerTopicStat> topicMap = ServerStatsProvider
                        .getStatsLoggerInstance().getPerTopicLogger(PerTopicStatType.CROSS_REGION);
                long minLastSeenMillis = Long.MAX_VALUE;
                for (PerTopicStat _value : topicMap.values()) {
                    PerTopicCrossRegionStat value = (PerTopicCrossRegionStat)_value;
                    ConcurrentMap<ByteString, Pair<PubSubProtocol.Message, Long>> regionMap =
                            value.getRegionMap();
                    for (Pair<PubSubProtocol.Message, Long> regionValue : regionMap.values()) {
                        Long lastSeenTimestamp = regionValue.second();
                        if (null == lastSeenTimestamp) {
                            continue;
                        }
                        minLastSeenMillis = Math.min(minLastSeenMillis, lastSeenTimestamp);
                    }
                }
                return Math.max(0L, MathUtils.now() - minLastSeenMillis);
            }
        };
        Stats.export(maxAgeCrossRegion);
    }

    // The HedwigServerStatsGetter functions
    @Override
    public OpStatsData getOpStatsData(OperationType type) {
        return getOpStatsLogger(type).toOpStatsData();
    }

    private PerTopicStat getPerTopicStat(PerTopicStatType type, ByteString topic, boolean create) {
        ConcurrentMap<ByteString, PerTopicStat> topicMap = perTopicLoggerMap.get(type);
        PerTopicStat curValue = null, statToPut = null;
        if (create) {
            if (type == PerTopicStatType.CROSS_REGION) {
                statToPut = new PerTopicCrossRegionStat(topic);
            } else if (type == PerTopicStatType.LOCAL_PENDING) {
                statToPut = new PerTopicPendingMessageStat(topic);
            }
            curValue = topicMap.putIfAbsent(topic, statToPut);
        }
        if (null == curValue) {
            curValue = topicMap.get(topic);
        }
        return curValue;
    }

    @Override
    public void setPerTopicSeqId(PerTopicStatType type, ByteString topic, long seqId, boolean create) {
        PerTopicStat curValue = getPerTopicStat(type, topic, create);
        if (null == curValue) {
            // We don't have ownership of the topic.
            return;
        }
        curValue.setSeqId(seqId);
    }

    @Override
    public void setPerTopicLastSeenMessage(PerTopicStatType type, ByteString topic, PubSubProtocol.Message message, boolean create) {
        PerTopicStat curValue = getPerTopicStat(type, topic, create);
        if (null == curValue) {
            // We don't have ownership of the topic.
            return;
        }
        curValue.setLastSeenMessage(message);
    }

    @Override
    public ConcurrentMap<ByteString, PerTopicStat> getPerTopicLogger(PerTopicStatType type) {
        return perTopicLoggerMap.get(type);
    }

    @Override
    public void removePerTopicLogger(PerTopicStatType type, ByteString topic) {
        ConcurrentMap<ByteString, PerTopicStat> topicMap = perTopicLoggerMap.get(type);
        topicMap.remove(topic);
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
