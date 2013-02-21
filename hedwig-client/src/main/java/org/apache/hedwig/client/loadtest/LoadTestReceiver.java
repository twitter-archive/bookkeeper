package org.apache.hedwig.client.loadtest;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.LoadTest;
import org.apache.hedwig.protocol.LoadTest.LoadTestMessage;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

public class LoadTestReceiver extends LoadTestBase {
    private static Logger logger = LoggerFactory.getLogger(LoadTestReceiver.class);
    private int numSubscribers;
    private int subscriberStartIndex;
    private LinkedList<SingleSubscriber> subList = new LinkedList<SingleSubscriber>();
    public LoadTestReceiver(TopicProvider topicProvider, LoadTestUtils ltUtil,
                            ClientConfiguration conf,
                            int numSubscribers, int subscriberStartIndex) {
        super(topicProvider, ltUtil, conf, "receive");
        this.numSubscribers = numSubscribers;
        this.subscriberStartIndex = subscriberStartIndex;
    }

    private class SingleSubscriber implements Runnable {
        final HedwigClient client;
        final ByteString subId;
        public SingleSubscriber(ByteString subId) {
            this.subId = subId;
            this.client = new HedwigClient(conf);
        }

        private void startDelivery(ByteString topic) {
            try {
                client.getSubscriber().startDelivery(topic, subId, new MessageHandler() {
                    @Override
                    public void deliver(ByteString topic, ByteString subscriberId, Message msg,
                                        Callback<Void> callback, Object context) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Received message:" + msg + " for topic:" + topic.toString() + " for" +
                                    " subscriber:" + subscriberId.toStringUtf8());
                        }
                        try {
                            LoadTestMessage ltm = LoadTestMessage.parseFrom(msg.getBody().toByteArray());
                            long latencyMillis = System.currentTimeMillis() - ltm.getTimestamp();
                            stat.requestComplete(TimeUnit.MILLISECONDS.toMicros(latencyMillis));
                        } catch (InvalidProtocolBufferException e) {
                            logger.error("Error while parsing message:" + msg, e);
                        }
                        callback.operationFinished(context, null);
                    }
                });
            } catch (Exception e) {
                logger.error("Exception while starting delivery for topic:" + topic.toStringUtf8() +
                        " for subscriber:" + subId.toStringUtf8(), e);
            }
        }
        public void stop() {
            for (ByteString topic : topicProvider.getTopicList()) {
                try {
                    client.getSubscriber().closeSubscription(topic, subId);
                } catch (Exception e) {
                    logger.error("Exception while unsubscribing from topic:" + topic.toStringUtf8() +
                            " for subscriber:" + subId.toStringUtf8());
                }
            }
        }

        public void run() {
            Subscriber subscriber = client.getSubscriber();
            for (final ByteString topic : topicProvider.getTopicList()) {
                subscriber.asyncSubscribe(topic, subId, CreateOrAttach.CREATE_OR_ATTACH, new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Subscription for topic:" + topic.toStringUtf8() + " for " +
                                    "subscriber:" + subId.toStringUtf8() + " succeeded");
                        }
                        startDelivery(topic);
                    }
                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        logger.error("Subscription to topic:" + topic.toStringUtf8() + " for " +
                                "subscriber:" + subId.toStringUtf8() + " failed", exception);
                    }
                }, null);
            }
        }
    }

    public void start() {
        for (int i = subscriberStartIndex; i < subscriberStartIndex+numSubscribers; i++) {
            ByteString subId = ltUtil.getSubscriberFromNumber(i);
            SingleSubscriber singleSub = new SingleSubscriber(subId);
            subList.add(singleSub);
            singleSub.run();
        }
    }

    public void stop() {
        for (SingleSubscriber sub : subList) {
            sub.stop();
        }
    }
}
