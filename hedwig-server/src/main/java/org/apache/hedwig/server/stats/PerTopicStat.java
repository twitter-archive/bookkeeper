package org.apache.hedwig.server.stats;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.PerTopicStatType;
import org.apache.hedwig.util.Pair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An abstract class representing a per topic stat.
 */
public abstract class PerTopicStat {

    /**
     * The topic this stat represents.
     */
    // TODO: seems no one use it, comment it to avoid findbug warning
    // ByteString topic;

    public PerTopicStat(ByteString topic) {
        // this.topic = topic;
    }

    /**
     * Sets the pending messages to be delivered stat value to seqId. Should be called only on
     * a PerTopicPendingMessageStat instance.
     * @param seqId
     */
    public abstract void setSeqId(long seqId);

    /**
     * Sets the last seen message from other regions for this particular topic. Should only be called
     * on an instance of PerTopicCrossRegionStat.
     * @param message
     */
    public abstract void setLastSeenMessage(PubSubProtocol.Message message);
}
