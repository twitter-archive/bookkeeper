package org.apache.hedwig.server.stats;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.SimpleStat;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.hedwig.protocol.PubSubProtocol;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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
        NUM_TOPICS, PERSIST_QUEUE, NUM_SUBSCRIPTIONS, NUM_REMOTE_SUBSCRIPTIONS,
        NUM_CACHED_ENTRIES, NUM_CACHE_STUBS, NUM_CACHE_HITS, CACHE_ENTRY_SIZE
    }

    public static enum PerTopicStatType {
        CROSS_REGION, LOCAL_PENDING
    }

    /**
     * Set the value in the per topic map for operation type. We use this to set the locally pending messages.
     * @param type The type of operation whose map we update.
     * @param topic
     * @param value The sequence id to be set
     * @param create True if we should put a value in the map if one doesn't exist.
     * @return
     */
    public void setPerTopicSeqId(PerTopicStatType type, ByteString topic, long seqId, boolean create);

    /**
     * Set the value in the per topic map for operation type. We use this to set cross region delivered sequence ids.
     * @param type
     * @param topic
     * @param message
     * @param create
     */
    public void setPerTopicLastSeenMessage(PerTopicStatType type, ByteString topic, PubSubProtocol.Message message, boolean create);

    /**
     * Get the logger map for this type. Each entry in the returned map is of the form <topic, PerTopicStat>
     * @param type
     */
    public ConcurrentMap<ByteString, PerTopicStat> getPerTopicLogger(PerTopicStatType type);

    /**
     * Remove the logger from the maps for this topic.
     * @param type
     * @param topic
     */
    public void removePerTopicLogger(PerTopicStatType type, ByteString topic);
}

