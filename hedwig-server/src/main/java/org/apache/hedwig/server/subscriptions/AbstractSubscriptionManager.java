/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.subscriptions;

import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionData;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionEvent;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionState;
import org.apache.hedwig.protoextensions.MessageIdUtils;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.common.TopicOpQueuer;
import org.apache.hedwig.server.delivery.ChannelEndPoint;
import org.apache.hedwig.server.delivery.DeliveryEndPoint;
import org.apache.hedwig.server.delivery.DeliveryManager;
import org.apache.hedwig.server.handlers.SubscriptionChannelManager;
import org.apache.hedwig.server.persistence.PersistenceManager;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.HedwigServerInternalOpStatType;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TopicOwnershipChangeListener;
import org.apache.hedwig.util.Callback;
import org.apache.hedwig.util.CallbackUtils;
import org.apache.hedwig.util.ConcurrencyUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public abstract class AbstractSubscriptionManager implements SubscriptionManager, TopicOwnershipChangeListener {

    static Logger logger = LoggerFactory.getLogger(AbstractSubscriptionManager.class);

    private final ServerConfiguration cfg;
    final ConcurrentHashMap<ByteString, Map<ByteString, InMemorySubscriptionState>> top2sub2seq =
		new ConcurrentHashMap<ByteString, Map<ByteString, InMemorySubscriptionState>>();
    // We use different queues for remote and local subscriptions
    protected final TopicOpQueuer localQueuer;
    private final TopicOpQueuer remoteQueuer;

    private final ArrayList<SubscriptionEventListener> listeners = new ArrayList<SubscriptionEventListener>();

    // Handle to the DeliveryManager for the server so we can stop serving subscribers
    // when losing topics
    private final DeliveryManager dm;
    // Handle to the PersistenceManager for the server so we can pass along the
    // message consume pointers for each topic.
    private final PersistenceManager pm;
    // The subscription channel manager that handles the actual endpoints of our subscriptions.
    private final SubscriptionChannelManager subChannelMgr;
    // Timer for running a recurring thread task to get the minimum message
    // sequence ID for each topic that all subscribers for it have consumed
    // already. With that information, we can call the PersistenceManager to
    // update it on the messages that are safe to be garbage collected.
    private final Timer timer = new Timer(true);
    // In memory mapping of topics to the minimum consumed message sequence ID
    // for all subscribers to the topic.
    private final ConcurrentHashMap<ByteString, Long> topic2MinConsumedMessagesMap = new ConcurrentHashMap<ByteString, Long>();

    protected final Callback<Void> noopCallback = new NoopCallback<Void>();

    static class NoopCallback<T> implements Callback<T> {
        @Override
        public void operationFailed(Object ctx, PubSubException exception) {
            logger.warn("Exception found in AbstractSubscriptionManager : ", exception);
        }

        @Override
        public void operationFinished(Object ctx, T resultOfOperation) {
        };
    }

    public AbstractSubscriptionManager(ServerConfiguration cfg, TopicManager tm,
                                       PersistenceManager pm, DeliveryManager dm,
                                       SubscriptionChannelManager subChannelMgr,
                                       OrderedSafeExecutor scheduler) {
        this.cfg = cfg;
        localQueuer = new TopicOpQueuer(scheduler);
        remoteQueuer = new TopicOpQueuer(scheduler);
        tm.addTopicOwnershipChangeListener(this);
        this.pm = pm;
        this.dm = dm;
        this.subChannelMgr = subChannelMgr;
        // Schedule the recurring MessagesConsumedTask only if a
        // PersistenceManager is passed.
        if (pm != null) {
            timer.schedule(new MessagesConsumedTask(), 0, cfg.getMessagesConsumedThreadRunInterval());
        }
    }

    /**
     * This is the Timer Task for finding out for each topic, what the minimum
     * consumed message by the subscribers are. This information is used to pass
     * along to the server's PersistenceManager so it can garbage collect older
     * topic messages that are no longer needed by the subscribers.
     */
    class MessagesConsumedTask extends TimerTask {
        /**
         * Implement the TimerTask's abstract run method.
         */
        @Override
        public void run() {
            // We are looping through relatively small in memory data structures
            // so it should be safe to run this fairly often.
            for (ByteString topic : top2sub2seq.keySet()) {
                final Map<ByteString, InMemorySubscriptionState> topicSubscriptions = top2sub2seq.get(topic);
                if (topicSubscriptions == null) {
                    continue;
                }

                long minConsumedMessage = Long.MAX_VALUE;
                boolean hasBound = true;
                // Loop through all subscribers on the current topic to find the
                // minimum persisted message id. The reason not using in-memory
                // consumed message id is LedgerRanges and InMemorySubscriptionState
                // may be inconsistent in case of a server crash.
                for (InMemorySubscriptionState curSubscription : topicSubscriptions.values()) {
                    if (curSubscription.getLastPersistedSeqId() < minConsumedMessage) {
                        minConsumedMessage = curSubscription.getLastPersistedSeqId();
                    }
                    hasBound = hasBound && curSubscription.getSubscriptionPreferences().hasMessageBound();
                }
                // Call the PersistenceManager if nobody subscribes to the topic
                // yet, or the consume pointer has moved ahead since the last
                // time, or if this is the initial subscription.
                Long minConsumedFromMap = topic2MinConsumedMessagesMap.get(topic);
                if (topicSubscriptions.isEmpty()
                    || (minConsumedFromMap != null && minConsumedFromMap < minConsumedMessage)
                    || (minConsumedFromMap == null && minConsumedMessage != 0)) {
                    topic2MinConsumedMessagesMap.put(topic, minConsumedMessage);
                    pm.consumedUntil(topic, minConsumedMessage);
                } else if (hasBound) {
                    pm.consumeToBound(topic);
                }
            }
        }
    }

    private class AcquireOp extends TopicOpQueuer.TimedAsynchronousOp<Void> {

        public AcquireOp(TopicOpQueuer enclosingInstance, ByteString topic, Callback<Void> callback, Object ctx) {
            enclosingInstance.super(topic, callback, ctx, HedwigServerInternalOpStatType.SUBSCRIPTION_MANAGER_ACQUIRE);
        }

        @Override
        public void run() {
            if (top2sub2seq.containsKey(topic)) {
                cb.operationFinished(ctx, null);
                return;
            }

            readSubscriptions(topic, new Callback<Map<ByteString, InMemorySubscriptionState>>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, exception);
                }

                @Override
                public void operationFinished(final Object ctx,
                final Map<ByteString, InMemorySubscriptionState> resultOfOperation) {
                    // We've just inherited a bunch of subscriber for this
                    // topic, some of which may be local. If they are, then we
                    // need to (1) notify listeners of this and (2) record the
                    // number for bookkeeping so that future
                    // subscribes/unsubscribes can efficiently notify listeners.

                    // The final "commit" (and "abort") operations.
                    final Callback<Void> cb2 = new Callback<Void>() {

                        @Override
                        public void operationFailed(Object ctx, PubSubException exception) {
                            logger.error("Subscription manager failed to acquired topic " + topic.toStringUtf8(),
                                         exception);
                            cb.operationFailed(ctx, null);
                        }

                        @Override
                        public void operationFinished(Object ctx, Void voidObj) {
                            top2sub2seq.put(topic, resultOfOperation);
                            logger.info("Subscription manager successfully acquired topic: " + topic.toStringUtf8());
                            cb.operationFinished(ctx, null);
                        }

                    };

                    // Notify listeners if necessary.
                    if (hasLocalSubscriptions(resultOfOperation)) {
                        notifySubscribe(topic, false, cb2, ctx);
                    } else {
                        cb2.operationFinished(ctx, null);
                    }

                    updateMessageBound(topic);
                }

            }, ctx);

        }

    }

    private void notifySubscribe(ByteString topic, boolean synchronous, final Callback<Void> cb, final Object ctx) {
        Callback<Void> mcb = CallbackUtils.multiCallback(listeners.size(), cb, ctx);
        for (SubscriptionEventListener listener : listeners) {
            listener.onFirstLocalSubscribe(topic, synchronous, mcb);
        }
    }

    private TopicOpQueuer getOpQueuer(ByteString subscriberId) {
        if (SubscriptionStateUtils.isHubSubscriber(subscriberId)) {
            return remoteQueuer;
        } else {
            return localQueuer;
        }
    }

    private void subSpecificPushAndMaybeRun(ByteString topic, ByteString subId, TopicOpQueuer.Op op) {
        getOpQueuer(subId).pushAndMaybeRun(topic, op);
    }

    /**
     * Figure out who is subscribed. Do nothing if already acquired. If there's
     * an error reading the subscribers' sequence IDs, then the topic is not
     * acquired.
     *
     * @param topic
     * @param callback
     * @param ctx
     */
    @Override
    public void acquiredTopic(final ByteString topic, final Callback<Void> callback, Object ctx) {
        localQueuer.pushAndMaybeRun(topic, new AcquireOp(localQueuer, topic, callback, ctx));
    }

    class ReleaseOp extends TopicOpQueuer.TimedAsynchronousOp<Void> {

        public ReleaseOp(TopicOpQueuer enclosingInstance, final ByteString topic, final Callback<Void> cb, Object ctx) {
            enclosingInstance.super(topic, cb, ctx, HedwigServerInternalOpStatType.SUBSCRIPTION_MANAGER_RELEASE);
        }

        @Override
        public void run() {
            // Make sure we don't serve this subscription request later.
            final Map<ByteString, InMemorySubscriptionState> states = top2sub2seq.remove(topic);
            if (logger.isDebugEnabled()) {
                logger.debug("Try to update subscription states when losing topic " + topic.toStringUtf8());
            }
            updateSubscriptionStates(states, topic, new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    logger.info("Finished update subscription states when losting topic "
                              + topic.toStringUtf8());
                    finish();
                }

                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    logger.warn("Error when releasing topic : " + topic.toStringUtf8(), exception);
                    finish();
                }

                private void finish() {
                    if (states != null) {
                        notifyUnsubcribe(topic, !hasLocalSubscriptions(states));
                    }
                    // no subscriptions now, it may be removed by other release ops
                    if (null != states) {
                        for (final ByteString subId : states.keySet()) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("Stop serving subscriber (" + topic.toStringUtf8() + ", "
                                           + subId.toStringUtf8() + ") when losing topic");
                            }
                            if (null != dm) {
                                final DeliveryEndPoint endPoint = dm.getDeliveryEndPoint(new TopicSubscriber(topic, subId));
                                dm.stopServingSubscriber(topic, subId, SubscriptionEvent.TOPIC_MOVED, endPoint,
                                     new Callback<Void>() {
                                         private void finish() {
                                             if (null != endPoint && null != AbstractSubscriptionManager.this.subChannelMgr) {
                                                 // Only if this is a ChannelEndPoint, we should let the subscription channel
                                                 // manager know that it needs to remove the channel this endpoint encapsulates.
                                                 Channel channel = ((ChannelEndPoint)endPoint).getChannel();
                                                 if (null != channel) {
                                                     AbstractSubscriptionManager.this.subChannelMgr
                                                             .remove(new TopicSubscriber(topic, subId), channel);
                                                 }
                                             }
                                         }
                                         @Override
                                         public void operationFinished(Object o, Void aVoid) {
                                             finish();
                                         }

                                         @Override
                                         public void operationFailed(Object o, PubSubException e) {
                                             finish();
                                         }
                                     }, null);
                            }
                        }
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("Stop serving topic " + topic.toStringUtf8());
                    }
                    cb.operationFinished(ctx, null);
                }
            }, ctx);
        }
    }

    void updateSubscriptionStates(ByteString topic, Callback<Void> finalCb, Object ctx) {
        final Map<ByteString, InMemorySubscriptionState> states = top2sub2seq.get(topic);
        updateSubscriptionStates(states, topic, finalCb, ctx);
    }

    void updateSubscriptionStates(final Map<ByteString, InMemorySubscriptionState> states,
                                  ByteString topic, Callback<Void> finalCb, Object ctx) {
        // Try to update subscription states of a specified topic
        if (null == states) {
            finalCb.operationFinished(ctx, null);
        } else {
            Callback<Void> mcb = CallbackUtils.multiCallback(states.size(), finalCb, ctx);
            for (Entry<ByteString, InMemorySubscriptionState> entry : states.entrySet()) {
                InMemorySubscriptionState memState = entry.getValue();
                if (memState.setLastConsumeSeqIdImmediately()) {
                    updateSubscriptionState(topic, entry.getKey(), memState, mcb, ctx);
                } else {
                    mcb.operationFinished(ctx, null);
                }
            }
        }
    }

    /**
     * Remove the local mapping.
     */
    @Override
    public void lostTopic(ByteString topic) {
        localQueuer.pushAndMaybeRun(topic, new ReleaseOp(localQueuer, topic, noopCallback, null));
    }

    private void notifyUnsubcribe(ByteString topic, boolean lastSubscriber) {
        for (SubscriptionEventListener listener : listeners)
            listener.onLastLocalUnsubscribe(topic, lastSubscriber);
    }

    protected abstract void readSubscriptions(final ByteString topic,
            final Callback<Map<ByteString, InMemorySubscriptionState>> cb, final Object ctx);

    protected abstract void readSubscriptionData(final ByteString topic, final ByteString subscriberId,
            final Callback<InMemorySubscriptionState> cb, Object ctx);

    private class SubscribeOp extends TopicOpQueuer.AsynchronousOp<SubscriptionData> {
        SubscribeRequest subRequest;
        MessageSeqId consumeSeqId;

        public SubscribeOp(TopicOpQueuer enclosingInstance, ByteString topic, SubscribeRequest subRequest,
                           MessageSeqId consumeSeqId, Callback<SubscriptionData> callback, Object ctx) {
            enclosingInstance.super(topic, callback, ctx);
            this.subRequest = subRequest;
            this.consumeSeqId = consumeSeqId;
        }

        @Override
        public void run() {

            logger.info("Executing a subscription request for topic:" + topic.toStringUtf8() + " from subscriber:" + subRequest.getSubscriberId()
                    .toStringUtf8());
            final Map<ByteString, InMemorySubscriptionState> topicSubscriptions = top2sub2seq.get(topic);
            if (topicSubscriptions == null) {
                cb.operationFailed(ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }

            final ByteString subscriberId = subRequest.getSubscriberId();
            final InMemorySubscriptionState subscriptionState = topicSubscriptions.get(subscriberId);
            CreateOrAttach createOrAttach = subRequest.getCreateOrAttach();

            if (subscriptionState != null) {

                if (createOrAttach.equals(CreateOrAttach.CREATE)) {
                    String msg = "Topic: " + topic.toStringUtf8() + " subscriberId: " + subscriberId.toStringUtf8()
                                 + " requested creating a subscription but it is already subscribed with state: "
                                 + SubscriptionStateUtils.toString(subscriptionState.getSubscriptionState());
                    logger.error(msg);
                    cb.operationFailed(ctx, new PubSubException.ClientAlreadySubscribedException(msg));
                    return;
                }

                // Subscription existed before, check whether new preferences provided
                // if new preferences provided, merged the subscription data and updated them
                // TODO: needs ACL mechanism when changing preferences
                if (subRequest.hasPreferences() &&
                    subscriptionState.updatePreferences(subRequest.getPreferences())) {
                    updateSubscriptionPreferences(topic, subscriberId, subscriptionState, new Callback<Void>() {
                        @Override
                        public void operationFailed(Object ctx, PubSubException exception) {
                            cb.operationFailed(ctx, exception);
                        }

                        @Override
                        public void operationFinished(Object ctx, Void resultOfOperation) {
                            logger.info("Topic: " + topic.toStringUtf8() + " subscriberId: " + subscriberId.toStringUtf8()
                                             + " attaching to subscription with state: "
                                             + SubscriptionStateUtils.toString(subscriptionState.getSubscriptionState())
                                             + ", with preferences: "
                                             + SubscriptionStateUtils.toString(subscriptionState.getSubscriptionPreferences()));
                            // update message bound if necessary
                            updateMessageBound(topic);
                            cb.operationFinished(ctx, subscriptionState.toSubscriptionData());
                        }
                    }, ctx);
                    return;
                }

                // otherwise just attach
                logger.info("Topic: " + topic.toStringUtf8() + " subscriberId: " + subscriberId.toStringUtf8()
                                 + " attaching to subscription with state: "
                                 + SubscriptionStateUtils.toString(subscriptionState.getSubscriptionState())
                                 + ", with preferences: "
                                 + SubscriptionStateUtils.toString(subscriptionState.getSubscriptionPreferences()));

                cb.operationFinished(ctx, subscriptionState.toSubscriptionData());
                return;
            }

            // we don't have a mapping for this subscriber
            if (createOrAttach.equals(CreateOrAttach.ATTACH)) {
                String msg = "Topic: " + topic.toStringUtf8() + " subscriberId: " + subscriberId.toStringUtf8()
                             + " requested attaching to an existing subscription but it is not subscribed";
                logger.error(msg);
                cb.operationFailed(ctx, new PubSubException.ClientNotSubscribedException(msg));
                return;
            }

            // now the hard case, this is a brand new subscription, must record
            SubscriptionState.Builder stateBuilder = SubscriptionState.newBuilder().setMsgId(consumeSeqId);

            SubscriptionPreferences.Builder preferencesBuilder;
            if (subRequest.hasPreferences()) {
                preferencesBuilder = SubscriptionPreferences.newBuilder(subRequest.getPreferences());
            } else {
                preferencesBuilder = SubscriptionPreferences.newBuilder();
            }

            // backward compatibility
            if (subRequest.hasMessageBound()) {
                preferencesBuilder = preferencesBuilder.setMessageBound(subRequest.getMessageBound());
            }

            SubscriptionData.Builder subDataBuilder =
                SubscriptionData.newBuilder().setState(stateBuilder).setPreferences(preferencesBuilder);
            final SubscriptionData subData = subDataBuilder.build();

            createSubscriptionData(topic, subscriberId, subData, new Callback<Version>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, exception);
                }

                @Override
                public void operationFinished(Object ctx, final Version version) {
                    Callback<Void> cb2 = new Callback<Void>() {
                        @Override
                        public void operationFailed(final Object ctx, final PubSubException exception) {
                            logger.error("subscription for subscriber " + subscriberId.toStringUtf8() + " to topic "
                                         + topic.toStringUtf8() + " failed due to failed listener callback", exception);
                            // should remove subscription when synchronized cross-region subscription failed
                            deleteSubscriptionData(topic, subscriberId, version, new Callback<Void>() {
                                @Override
                                public void operationFinished(Object context,
                                        Void resultOfOperation) {
                                    finish();
                                }
                                @Override
                                public void operationFailed(Object context,
                                        PubSubException ex) {
                                    logger.error("Remove subscription for subscriber " + subscriberId.toStringUtf8() + " to topic "
                                                 + topic.toStringUtf8() + " failed : ", ex);
                                    finish();
                                }
                                private void finish() {
                                    cb.operationFailed(ctx, exception);
                                }
                            }, ctx);
                        }

                        @Override
                        public void operationFinished(Object ctx, Void resultOfOperation) {
                            topicSubscriptions.put(subscriberId, new InMemorySubscriptionState(subData, version));

                            updateMessageBound(topic);
                            cb.operationFinished(ctx, subData);
                        }

                    };

                    // if this will be the first local subscription, notifySubscribe
                    if (!SubscriptionStateUtils.isHubSubscriber(subRequest.getSubscriberId())
                        && !hasLocalSubscriptions(topicSubscriptions)) {
                        logger.info("This is the first subscription for topic:" + topic.toStringUtf8() +
                                " so we are notifying listeners.");
                        notifySubscribe(topic, subRequest.getSynchronous(), cb2, ctx);
                    }
                    else
                        cb2.operationFinished(ctx, null);
                }
            }, ctx);
        }
    }

    /**
     * @return True if the given subscriberId-to-subscriberState map contains a local subscription:
     * the vast majority of subscriptions are local, so we will quickly encounter one if it exists.
     */
    private static boolean hasLocalSubscriptions(Map<ByteString, InMemorySubscriptionState> topicSubscriptions) {
      for (ByteString subId : topicSubscriptions.keySet())
        if (!SubscriptionStateUtils.isHubSubscriber(subId))
          return true;
      return false;
    }

    public void updateMessageBound(ByteString topic) {
        final Map<ByteString, InMemorySubscriptionState> topicSubscriptions = top2sub2seq.get(topic);
        if (topicSubscriptions == null) {
            return;
        }
        int maxBound = Integer.MIN_VALUE;
        for (Map.Entry<ByteString, InMemorySubscriptionState> e : topicSubscriptions.entrySet()) {
            if (!e.getValue().getSubscriptionPreferences().hasMessageBound()) {
                maxBound = Integer.MIN_VALUE;
                break;
            } else {
                maxBound = Math.max(maxBound, e.getValue().getSubscriptionPreferences().getMessageBound());
            }
        }
        if (maxBound == Integer.MIN_VALUE) {
            pm.clearMessageBound(topic);
        } else {
            pm.setMessageBound(topic, maxBound);
        }
    }

    @Override
    public void serveSubscribeRequest(ByteString topic, SubscribeRequest subRequest, MessageSeqId consumeSeqId,
                                      Callback<SubscriptionData> callback, Object ctx) {
        SubscribeOp op = new SubscribeOp(getOpQueuer(subRequest.getSubscriberId()), topic, subRequest,
                consumeSeqId, callback, ctx);
        subSpecificPushAndMaybeRun(topic, subRequest.getSubscriberId(), op);
    }

    private class ConsumeOp extends TopicOpQueuer.AsynchronousOp<Void> {
        ByteString subscriberId;
        MessageSeqId consumeSeqId;

        public ConsumeOp(TopicOpQueuer enclosingInstance, ByteString topic, ByteString subscriberId,
                         MessageSeqId consumeSeqId, Callback<Void> callback, Object ctx) {
            enclosingInstance.super(topic, callback, ctx);
            this.subscriberId = subscriberId;
            this.consumeSeqId = consumeSeqId;
        }

        @Override
        public void run() {
            Map<ByteString, InMemorySubscriptionState> topicSubs = top2sub2seq.get(topic);
            if (topicSubs == null) {
                cb.operationFinished(ctx, null);
                return;
            }

            final InMemorySubscriptionState subState = topicSubs.get(subscriberId);
            if (subState == null) {
                cb.operationFinished(ctx, null);
                return;
            }

            if (subState.setLastConsumeSeqId(consumeSeqId, cfg.getConsumeInterval())) {
                updateSubscriptionState(topic, subscriberId, subState, new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        subState.setLastPersistedSeqId(consumeSeqId.getLocalComponent());
                        cb.operationFinished(ctx, resultOfOperation);
                    }

                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        cb.operationFailed(ctx, exception);
                    }
                }, ctx);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Only advanced consume pointer in memory, will persist later, topic: "
                                 + topic.toStringUtf8() + " subscriberId: " + subscriberId.toStringUtf8()
                                 + " persistentState: " + SubscriptionStateUtils.toString(subState.getSubscriptionState())
                                 + " in-memory consume-id: "
                                 + MessageIdUtils.msgIdToReadableString(subState.getLastConsumeSeqId()));
                }
                cb.operationFinished(ctx, null);
            }

            // tell delivery manage about the consume event
            if (null != dm) {
                dm.messageConsumed(topic, subscriberId, consumeSeqId);
            }
        }
    }

    @Override
    public void setConsumeSeqIdForSubscriber(ByteString topic, ByteString subscriberId, MessageSeqId consumeSeqId,
            Callback<Void> callback, Object ctx) {
        ConsumeOp op = new ConsumeOp(getOpQueuer(subscriberId), topic, subscriberId, consumeSeqId, callback, ctx);
        subSpecificPushAndMaybeRun(topic, subscriberId, op);
    }

    private class CloseSubscriptionOp extends TopicOpQueuer.AsynchronousOp<Void> {

        public CloseSubscriptionOp(TopicOpQueuer enclosingInstance,
                                   ByteString topic, ByteString subscriberId,
                                   Callback<Void> callback, Object ctx) {
            enclosingInstance.super(topic, callback, ctx);
        }

        @Override
        public void run() {
            // TODO: BOOKKEEPER-412: we might need to move the loaded subscription
            //                       to reclaim memory
            // But for now we do nothing
            cb.operationFinished(ctx, null);
        }
    }

    @Override
    public void closeSubscription(ByteString topic, ByteString subscriberId,
                                  Callback<Void> callback, Object ctx) {
        CloseSubscriptionOp op = new CloseSubscriptionOp(getOpQueuer(subscriberId), topic, subscriberId,
                                                         callback, ctx);
        subSpecificPushAndMaybeRun(topic, subscriberId, op);
    }

    private class UnsubscribeOp extends TopicOpQueuer.AsynchronousOp<Void> {
        ByteString subscriberId;

        public UnsubscribeOp(TopicOpQueuer enclosingInstance, ByteString topic, ByteString subscriberId,
                             Callback<Void> callback, Object ctx) {
            enclosingInstance.super(topic, callback, ctx);
            this.subscriberId = subscriberId;
        }

        @Override
        public void run() {
            final Map<ByteString, InMemorySubscriptionState> topicSubscriptions = top2sub2seq.get(topic);
            if (topicSubscriptions == null) {
                cb.operationFailed(ctx, new PubSubException.ServerNotResponsibleForTopicException(""));
                return;
            }

            if (!topicSubscriptions.containsKey(subscriberId)) {
                cb.operationFailed(ctx, new PubSubException.ClientNotSubscribedException(""));
                return;
            }

            deleteSubscriptionData(topic, subscriberId, topicSubscriptions.get(subscriberId).getVersion(),
                    new Callback<Void>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    cb.operationFailed(ctx, exception);
                }

                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    topicSubscriptions.remove(subscriberId);
                    // Notify listeners if necessary.
                    if (!SubscriptionStateUtils.isHubSubscriber(subscriberId)
                        && !hasLocalSubscriptions(topicSubscriptions))
                        notifyUnsubcribe(topic, true);

                    updateMessageBound(topic);
                    cb.operationFinished(ctx, null);
                }
            }, ctx);

        }

    }

    @Override
    public void unsubscribe(ByteString topic, ByteString subscriberId, Callback<Void> callback, Object ctx) {
        UnsubscribeOp op = new UnsubscribeOp(getOpQueuer(subscriberId), topic, subscriberId, callback, ctx);
        subSpecificPushAndMaybeRun(topic, subscriberId, op);
    }

    /**
     * Not thread-safe.
     */
    @Override
    public void addListener(SubscriptionEventListener listener) {
        listeners.add(listener);
    }

    /**
     * Method to stop this class gracefully including releasing any resources
     * used and stopping all threads spawned.
     */
    @Override
    public void stop() {
        timer.cancel();
        try {
            final LinkedBlockingQueue<Boolean> queue = new LinkedBlockingQueue<Boolean>();
            // update dirty subscriptions
            for (ByteString topic : top2sub2seq.keySet()) {
                Callback<Void> finalCb = new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        ConcurrencyUtils.put(queue, true);
                    }
                    @Override
                    public void operationFailed(Object ctx,
                            PubSubException exception) {
                        ConcurrencyUtils.put(queue, false);
                    }
                };
                updateSubscriptionStates(topic, finalCb, null);
                queue.take();
            }
        } catch (InterruptedException ie) {
            logger.warn("Error during updating subscription states : ", ie);
        }
    }

    private void updateSubscriptionState(final ByteString topic, final ByteString subscriberId,
                                         final InMemorySubscriptionState state,
                                         final Callback<Void> callback, Object ctx) {
        SubscriptionData subData;
        Callback<Version> cb = new Callback<Version>() {
            @Override
            public void operationFinished(Object ctx, Version version) {
                state.setVersion(version);
                callback.operationFinished(ctx, null);
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                if (exception instanceof PubSubException.BadVersionException) {
                    readSubscriptionData(topic, subscriberId, new Callback<InMemorySubscriptionState>() {
                        @Override
                        public void operationFinished(Object ctx,
                                InMemorySubscriptionState resultOfOperation) {
                            state.setVersion(resultOfOperation.getVersion());
                            updateSubscriptionState(topic, subscriberId, state, callback, ctx);
                        }
                        @Override
                        public void operationFailed(Object ctx,
                                PubSubException exception) {
                            callback.operationFailed(ctx, exception);
                        }
                    }, ctx);

                    return;
                }
                callback.operationFailed(ctx, exception);
            }
        };
        if (isPartialUpdateSupported()) {
            subData = SubscriptionData.newBuilder().setState(state.getSubscriptionState()).build();
            updateSubscriptionData(topic, subscriberId, subData, state.getVersion(), cb, ctx);
        } else {
            subData = state.toSubscriptionData();
            replaceSubscriptionData(topic, subscriberId, subData, state.getVersion(), cb, ctx);
        }
    }

    private void updateSubscriptionPreferences(final ByteString topic, final ByteString subscriberId,
                                               final InMemorySubscriptionState state,
                                               final Callback<Void> callback, Object ctx) {
        SubscriptionData subData;
        Callback<Version> cb = new Callback<Version>() {
            @Override
            public void operationFinished(Object ctx, Version version) {
                state.setVersion(version);
                callback.operationFinished(ctx, null);
            }
            @Override
            public void operationFailed(Object ctx, PubSubException exception) {
                if (exception instanceof PubSubException.BadVersionException) {
                    readSubscriptionData(topic, subscriberId, new Callback<InMemorySubscriptionState>() {
                        @Override
                        public void operationFinished(Object ctx,
                                InMemorySubscriptionState resultOfOperation) {
                            state.setVersion(resultOfOperation.getVersion());
                            updateSubscriptionPreferences(topic, subscriberId, state, callback, ctx);
                        }
                        @Override
                        public void operationFailed(Object ctx,
                                PubSubException exception) {
                            callback.operationFailed(ctx, exception);
                        }
                    }, ctx);

                    return;
                }
                callback.operationFailed(ctx, exception);
            }
        };
        if (isPartialUpdateSupported()) {
            subData = SubscriptionData.newBuilder().setPreferences(state.getSubscriptionPreferences()).build();
            updateSubscriptionData(topic, subscriberId, subData, state.getVersion(), cb, ctx);
        } else {
            subData = state.toSubscriptionData();
            replaceSubscriptionData(topic, subscriberId, subData, state.getVersion(), cb, ctx);
        }
    }

    protected abstract boolean isPartialUpdateSupported();

    protected abstract void createSubscriptionData(final ByteString topic, ByteString subscriberId,
            SubscriptionData data, Callback<Version> callback, Object ctx);

    protected abstract void updateSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx);

    protected abstract void replaceSubscriptionData(ByteString topic, ByteString subscriberId, SubscriptionData data,
            Version version, Callback<Version> callback, Object ctx);

    protected abstract void deleteSubscriptionData(ByteString topic, ByteString subscriberId, Version version, Callback<Void> callback,
            Object ctx);

}
