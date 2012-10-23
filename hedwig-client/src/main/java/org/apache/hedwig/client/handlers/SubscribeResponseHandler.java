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
package org.apache.hedwig.client.handlers;

import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.data.MessageConsumeData;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.netty.HedwigClientImpl;
import org.apache.hedwig.client.netty.ResponseHandler;
import org.apache.hedwig.exceptions.PubSubException.ClientAlreadySubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubResponse;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protocol.PubSubProtocol.StatusCode;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeResponse;
import org.apache.hedwig.protocol.PubSubProtocol.SubscriptionPreferences;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;

public class SubscribeResponseHandler {

    private static Logger logger = LoggerFactory.getLogger(SubscribeResponseHandler.class);

    private final ResponseHandler responseHandler;

    // Member variables used when this ResponseHandler is for a Subscribe
    // channel. We need to be able to consume messages sent back to us from
    // the server, and to also recreate the Channel connection if it ever goes
    // down. For that, we need to store the original PubSubData for the
    // subscribe request, and also the MessageHandler that was registered when
    // delivery of messages started for the subscription.
    private volatile PubSubData origSubData;
    private volatile Channel subscribeChannel;
    // Counter for the number of consumed messages so far to buffer up before we
    // send the Consume message back to the server along with the last/largest
    // message seq ID seen so far in that batch.
    private AtomicInteger numConsumedMessagesInBuffer = new AtomicInteger(0);
    private MessageSeqId lastMessageSeqId;

    // The Message delivery handler that is used to deliver messages without locking.
    final private MessageDeliveryHandler deliveryHandler = new MessageDeliveryHandler();

    // Set to store all of the outstanding subscribed messages that are pending
    // to be consumed by the client app's MessageHandler. If this ever grows too
    // big (e.g. problem at the client end for message consumption), we can
    // throttle things by temporarily setting the Subscribe Netty Channel
    // to not be readable. When the Set has shrunk sufficiently, we can turn the
    // channel back on to read new messages.
    final private Set<Message> outstandingMsgSet;

    public SubscribeResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        // Create the Set (from a concurrent hashmap) to keep track
        // of outstanding Messages to be consumed by the client app. At this
        // stage, delivery for that topic hasn't started yet so creation of
        // this Set should be thread safe. We'll create the Set with an initial
        // capacity equal to the configured parameter for the maximum number of
        // outstanding messages to allow. The load factor will be set to
        // 1.0f which means we'll only rehash and allocate more space if
        // we ever exceed the initial capacity. That should be okay
        // because when that happens, things are slow already and piling
        // up on the client app side to consume messages.
        outstandingMsgSet = Collections.newSetFromMap(
                new ConcurrentHashMap<Message,Boolean>(
                        responseHandler.getConfiguration().getMaximumOutstandingMessages(), 1.0f));
    }

    // Public getter to retrieve the original PubSubData used for the Subscribe
    // request.
    public PubSubData getOrigSubData() {
        return origSubData;
    }

    // Main method to handle Subscribe responses from the server that we sent
    // a Subscribe Request to.
    public void handleSubscribeResponse(PubSubResponse response, PubSubData pubSubData, Channel channel)
            throws Exception {
        // If this was not a successful response to the Subscribe request, we
        // won't be using the Netty Channel created so just close it.
        if (!response.getStatusCode().equals(StatusCode.SUCCESS)) {
            try {
                HedwigClientImpl.getResponseHandlerFromChannel(channel).handleChannelClosedExplicitly();
            } catch (NoResponseHandlerException e) {
                // Log an error. But should we also return and not process anything further?
                logger.error("No response handler found while trying to close channel explicitly while handling a " +
                        "failed subscription response.", e);
                // Continue closing the channel because this is an unexpected event and state should be reset.
            }
            channel.close();
        }

        logger.info("Handling a Subscribe response: " + response + ", pubSubData: " + pubSubData + ", host: "
                         + HedwigClientImpl.getHostFromChannel(channel));

        switch (response.getStatusCode()) {
        case SUCCESS:
            synchronized(this) {
                // For successful Subscribe requests, store this Channel locally
                // and set it to not be readable initially.
                // This way we won't be delivering messages for this topic
                // subscription until the client explicitly says so.
                subscribeChannel = channel;
                subscribeChannel.setReadable(false);
                // Store the original PubSubData used to create this successful
                // Subscribe request.
                origSubData = pubSubData;
                deliveryHandler.setOrigSubData(pubSubData);

                SubscriptionPreferences preferences = null;
                if (response.hasResponseBody()) {
                    ResponseBody respBody = response.getResponseBody();
                    if (respBody.hasSubscribeResponse()) {
                        SubscribeResponse resp = respBody.getSubscribeResponse();
                        if (resp.hasPreferences()) {
                            preferences = resp.getPreferences();
                            logger.info("Receive subscription preferences for (topic:" + pubSubData.topic.toStringUtf8()
                                    + ", subscriber:" + pubSubData.subscriberId.toStringUtf8() + ") :"
                                    + SubscriptionStateUtils.toString(preferences));
                        }
                    }
                }
                // Store the mapping for the TopicSubscriber to the Channel.
                // This is so we can control the starting and stopping of
                // message deliveries from the server on that Channel. Store
                // this only on a successful ack response from the server.
                TopicSubscriber topicSubscriber = new TopicSubscriber(pubSubData.topic, pubSubData.subscriberId);
                responseHandler.getSubscriber().setChannelAndPreferencesForTopic(topicSubscriber, channel, preferences);
            }
            // Response was success so invoke the callback's operationFinished
            // method.
            pubSubData.getCallback().operationFinished(pubSubData.context, null);
            break;
        case CLIENT_ALREADY_SUBSCRIBED:
            // For Subscribe requests, the server says that the client is
            // already subscribed to it.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ClientAlreadySubscribedException(
                                                    "Client is already subscribed for topic: " +
                                                        pubSubData.topic.toStringUtf8() + ", subscriberId: " +
                                                        pubSubData.subscriberId.toStringUtf8()));
            break;
        case SERVICE_DOWN:
            // Response was service down failure so just invoke the callback's
            // operationFailed method.
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                    "Server responded with a SERVICE_DOWN status"));
            break;
        case NOT_RESPONSIBLE_FOR_TOPIC:
            // Redirect response so we'll need to repost the original Subscribe
            // Request
            responseHandler.handleRedirectResponse(response, pubSubData, channel);
            break;
        default:
            // Consider all other status codes as errors, operation failed
            // cases.
            logger.error("Unexpected error response from server for PubSubResponse: " + response);
            pubSubData.getCallback().operationFailed(pubSubData.context, new ServiceDownException(
                                                    "Server responded with a status code of: "
                                                    + response.getStatusCode() +
                                                    " and message: " + response.getStatusMsg()));
            break;
        }
    }

    // Main method to handle consuming a message for a topic that the client is
    // subscribed to.
    public void handleSubscribeMessage(PubSubResponse response) {
        if (logger.isDebugEnabled()) {
            logger.debug("Handling a Subscribe message in response: {}, topic: {}, subscriberId: {}",
                    new Object[] { response, getOrigSubData().topic.toStringUtf8(),
                                   getOrigSubData().subscriberId.toStringUtf8() });
        }
        // Simply call asyncMessageConsume on this message. asyncMessageConsume will take care of queuing the message
        // if no message handler is available.
        asyncMessageConsume(response.getMessage());
    }

    /**
     * Method called when a message arrives for a subscribe Channel and we want
     * to consume it asynchronously via the registered MessageDeliveryHandler
     *
     * @param message
     *            Message from Subscribe Channel we want to consume.
     */
    protected void asyncMessageConsume(Message message) {
        if (logger.isDebugEnabled())
            logger.debug("Call the client app's MessageHandler asynchronously to consume the message: " + message
                         + ", topic: " + origSubData.topic.toStringUtf8() + ", subscriberId: "
                         + origSubData.subscriberId.toStringUtf8());
        // Add this "pending to be consumed" message to the outstandingMsgSet.
        outstandingMsgSet.add(message);
        // Check if we've exceeded the max size for the outstanding message set.
        if (outstandingMsgSet.size() >= responseHandler.getConfiguration().getMaximumOutstandingMessages()
                && subscribeChannel.isReadable()) {
            // Too many outstanding messages so throttle it by setting the Netty
            // Channel to not be readable.
            logger.warn("Too many outstanding messages (" + outstandingMsgSet.size()
                         + ") for topic:" + origSubData.topic.toStringUtf8() + " and subscriber:" + origSubData.subscriberId.toStringUtf8()
                         + " so throttling the subscribe netty Channel");
            subscribeChannel.setReadable(false);
        }
        // The message set in the MessageConsumeData class below is used by the MessageDeliveryHandler to
        // invoke deliver.
        MessageConsumeData messageConsumeData = new MessageConsumeData(origSubData.topic, origSubData.subscriberId,
                message, responseHandler.getClient().getConsumeCallback());
        try {
            deliveryHandler.offerAndOptionallyDeliver(messageConsumeData);
        } catch (MessageDeliveryHandler.MessageDeliveryException e) {
            logger.error("Caught exception while trying to deliver messages to the message handler." + e);
            // We close the channel so that this subscription is closed and retried later resulting in a new response handler.
            // We want the state to be cleared so we don't close the channel explicitly.
            if (null != subscribeChannel) {
                subscribeChannel.close();
            }
        }
    }

    /**
     * Method called when the client app's MessageHandler has asynchronously
     * completed consuming a subscribed message sent from the server. The
     * contract with the client app is that messages sent to the handler to be
     * consumed will have the callback response done in the same order. So if we
     * asynchronously call the MessageHandler to consume messages #1-5, that
     * should call the messageConsumed method here via the VoidCallback in the
     * same order. As the contract with the client app states that this message will be
     * called in the same order, there is no need to synchronize on this.
     *
     * @param message
     *            Message sent from server for topic subscription that has been
     *            consumed by the client.
     */
    protected void messageConsumed(Message message) {
        if (logger.isDebugEnabled())
            logger.debug("Message has been successfully consumed by the client app for message: " + message
                         + ", topic: " + origSubData.topic.toStringUtf8() + ", subscriberId: "
                         + origSubData.subscriberId.toStringUtf8());

        ClientConfiguration cfg = responseHandler.getConfiguration();

        // Remove this consumed message from the outstanding Message Set.
        outstandingMsgSet.remove(message);

        // If we had throttled delivery and the outstanding message number is below the configured threshold,
        // set the channel to readable.
        if (!subscribeChannel.isReadable() && outstandingMsgSet.size() <= cfg.getConsumedMessagesBufferSize()
                * cfg.getReceiveRestartPercentage() / 100.0) {
            logger.info("Message consumption has caught up so okay to turn off throttling of messages on the subscribe channel for topic:"
                   + origSubData.topic.toStringUtf8()
                   + ", subscriber:"
                   + origSubData.subscriberId.toStringUtf8());
            subscribeChannel.setReadable(true);
        }

        // Update these variables only if we are auto-sending consume
        // messages to the server. Otherwise the onus is on the client app
        // to call the Subscriber consume API to let the server know which
        // messages it has successfully consumed.
        if (cfg.isAutoSendConsumeMessageEnabled()) {
            lastMessageSeqId = message.getMsgId();
            // We just need to keep a count of the number of messages consumed
            // and the largest/latest msg ID seen so far in this batch. Messages
            // should be delivered in order and without gaps.
            if (numConsumedMessagesInBuffer.incrementAndGet() == cfg.getConsumedMessagesBufferSize()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending consume message to hedwig server for topic:" + origSubData.topic.toStringUtf8()
                    + ", subscriber:" + origSubData.subscriberId.toStringUtf8() + " with message sequence id:"
                    + lastMessageSeqId);
                }
                responseHandler.getSubscriber().doConsume(origSubData, subscribeChannel, lastMessageSeqId);
                numConsumedMessagesInBuffer.addAndGet(-(cfg.getConsumedMessagesBufferSize()));
            }
        }
    }

    /**
     * Setter used for Subscribe flows when delivery for the subscription is
     * started. This is used to register the MessageHandler needed to consumer
     * the subscribed messages for the topic.
     *
     * @param messageHandler
     *            MessageHandler to register for this ResponseHandler instance.
     */
    public void setMessageHandler(MessageHandler messageHandler) {
        logger.info("Setting the messageHandler for topic: {}, subscriberId: {}",
                getOrigSubData().topic.toStringUtf8(),
                getOrigSubData().subscriberId.toStringUtf8());
        try {
            deliveryHandler.setMessageHandlerOptionallyDeliver(messageHandler);
        } catch (MessageDeliveryHandler.MessageDeliveryException e) {
            logger.error("Error while setting message handler and delivering outstanding messages." + e);
            if (null != subscribeChannel) {
                subscribeChannel.close();
            }
        }
    }

    /**
     * Getter for the MessageHandler that is set for this subscribe channel.
     *
     * @return The MessageHandler for consuming messages
     */
    public MessageHandler getMessageHandler() {
        return deliveryHandler.getMessageHandler();
    }
}
