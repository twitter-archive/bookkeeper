package com.twitter.hedwig.client.benchmark;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.RegionSpecificSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

public class TwitterBenchmarkSubscriber implements Callable<Void>{
    private static final Logger logger = Logger.getLogger(TwitterBenchmarkSubscriber.class);
    private Subscriber subscriber;
    private String subscriberPrefix;
    private ByteString topic;
    private int numSubscribers;
    private double delayFrequency;
    private int delayInSecs;
    private int statsReportingIntervalInSecs;

    private static Random RNG = new Random(System.currentTimeMillis());

    public TwitterBenchmarkSubscriber(Subscriber subscriber, ByteString topic,
        String subscriberPrefix, int numSubscribers, double delayFrequency, int delayInSecs,
        int statsReportingIntervalInSecs) {
        this.subscriber = subscriber;
        this.subscriberPrefix = subscriberPrefix;
        this.topic = topic;
        this.numSubscribers = numSubscribers;
        this.delayFrequency = delayFrequency;
        this.delayInSecs = delayInSecs;
        this.statsReportingIntervalInSecs = statsReportingIntervalInSecs;
    }

    public Void call() throws Exception {
        final TwitterThroughputAggregator agg =
            new TwitterThroughputAggregator(subscriberPrefix + "_1_" + numSubscribers,
                this.statsReportingIntervalInSecs*1000L);

        final Map<String, Long> lastSeqIdSeenMap = new HashMap<String, Long>();

        for (int i = 0; i < this.numSubscribers; i++) {
            ByteString subId =  ByteString.copyFromUtf8(subscriberPrefix + i);
            logger.info("Subscribe to " + topic.toString("utf-8") + ", subId=" +
                        subId.toString("utf-8"));
            subscriber.subscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH);
            subscriber.startDelivery(topic, subId, new MessageHandler() {
                @Override
                public void consume(ByteString thisTopic, ByteString subscriberId, Message msg,
                        Callback<Void> callback, Object context) {
                    if(RNG.nextDouble() <= delayFrequency) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("delay message handling by " + delayInSecs + " seconds");
                        }
                        TwitterBenchmarkUtils.sleep(delayInSecs*1000L);
                    }
                    if (logger.isDebugEnabled())
                        logger.debug("Got message from src-region: " + msg.getSrcRegion() + " with seq-id: "
                                + msg.getMsgId());

                    String mapKey = topic + msg.getSrcRegion().toStringUtf8();
                    Long lastSeqIdSeen = lastSeqIdSeenMap.get(mapKey);
                    if (lastSeqIdSeen == null) {
                        lastSeqIdSeen = (long) 0;
                    }

                    if (getSrcSeqId(msg) <= lastSeqIdSeen) {
                        logger.info("Redelivery of message, src-region: " + msg.getSrcRegion() + "seq-id: "
                                + msg.getMsgId());
                    } else {
                        agg.ding(false);
                    }

                    callback.operationFinished(context, null);
                }
            });
        }
        logger.info("Finished subscribing to topics and now waiting for messages to come in...");
        // don't exit until being killed
        while(true) {
            try {
                Thread.sleep(10000L);
            } catch (InterruptedException ignored) {
            }
        }
    }

    long getSrcSeqId(Message msg) {
        if (msg.getMsgId().getRemoteComponentsCount() == 0) {
            return msg.getMsgId().getLocalComponent();
        }

        for (RegionSpecificSeqId rseqId : msg.getMsgId().getRemoteComponentsList()) {
            if (rseqId.getRegion().equals(msg.getSrcRegion()))
                return rseqId.getSeqId();
        }

        return msg.getMsgId().getLocalComponent();
    }

}
