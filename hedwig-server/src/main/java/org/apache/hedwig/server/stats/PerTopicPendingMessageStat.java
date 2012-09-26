package org.apache.hedwig.server.stats;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.PerTopicStatType;

import java.util.concurrent.atomic.AtomicLong;

public class PerTopicPendingMessageStat extends PerTopicStat {

    /**
     * This value represents the number of messages pending delivery for this topic locally.
     * Note: This could be null.
     */
    AtomicLong pending;

    public PerTopicPendingMessageStat(ByteString topic) {
        super(topic);
        this.pending = new AtomicLong(0);
    }

    @Override
    public void setSeqId(long seqId) {
        this.pending.set(seqId);
    }

    public AtomicLong getPending() {
        return pending;
    }

    @Override
    public void setLastSeenMessage(PubSubProtocol.Message message) {
        throw new RuntimeException("Method not implemented for per topic stat type : " + PerTopicStatType.LOCAL_PENDING);
    }
}
