package org.apache.hedwig.server.stats;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.PerTopicStatType;
import org.apache.hedwig.util.Pair;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Stores per topic information for cross region delivery stats.
 */
public class PerTopicCrossRegionStat extends PerTopicStat {

    /**
     * This represents the most recent cross region message for a topic.
     * Note: This could be null.
     */
    PubSubProtocol.Message message;

    /**
     * Stores the last seen message and the time(in milliseconds) we saw that message. The key is the
     * region name.
     */
    ConcurrentMap<ByteString, Pair<PubSubProtocol.Message, Long>> regionMap;

    public PerTopicCrossRegionStat(ByteString topic) {
        super(topic);
        this.message = null;
        this.regionMap = new ConcurrentHashMap<ByteString, Pair<PubSubProtocol.Message, Long>>();
    }

    @Override
    public void setSeqId(long seqId) {
        throw new RuntimeException("Method not implemented for per topic stat type : " + PerTopicStatType.CROSS_REGION);
    }

    @Override
    public void setLastSeenMessage(PubSubProtocol.Message message) {
        this.message = message;
        if (null == message) {
            return;
        }
        regionMap.put(message.getSrcRegion(), Pair.of(message, MathUtils.now()));
    }

    public PubSubProtocol.Message getMessage() {
        return message;
    }

    public ConcurrentMap<ByteString, Pair<PubSubProtocol.Message, Long>> getRegionMap() {
        return this.regionMap;
    }
}
