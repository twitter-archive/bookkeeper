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
package org.apache.hedwig.client.netty.impl.multiplex;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.exceptions.AlreadyStartDeliveryException;
import org.apache.hedwig.client.exceptions.NoResponseHandlerException;
import org.apache.hedwig.client.handlers.SubscribeResponseHandler;
import org.apache.hedwig.client.netty.CleanupChannelMap;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.NetUtils;
import org.apache.hedwig.client.netty.impl.AbstractHChannelManager;
import org.apache.hedwig.client.netty.impl.ClientChannelPipelineFactory;
import org.apache.hedwig.client.netty.impl.HChannelHandler;
import org.apache.hedwig.client.netty.impl.HChannelImpl;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;


/**
 * Multiplex HChannel Manager which establish a connection for multi subscriptions.
 */
public class MultiplexHChannelManager extends AbstractHChannelManager {

    static final Logger logger = LoggerFactory.getLogger(MultiplexHChannelManager.class);

    // Find which HChannel that a given TopicSubscriber used.
    protected final CleanupChannelMap<InetSocketAddress> subscriptionChannels;

    // PipelineFactory to create subscription netty channels to the appropriate server
    private final ClientChannelPipelineFactory subscriptionChannelPipelineFactory;

    public MultiplexHChannelManager(ClientConfiguration cfg,
                                    ChannelFactory socketFactory) {
        super(cfg, socketFactory);
        subscriptionChannels = new CleanupChannelMap<InetSocketAddress>();
        subscriptionChannelPipelineFactory =
            new MultiplexSubscriptionChannelPipelineFactory(cfg, this);
    }

    @Override
    protected ClientChannelPipelineFactory getSubscriptionChannelPipelineFactory() {
        return subscriptionChannelPipelineFactory;
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(Channel channel) {
        // store the channel connected to target host for future usage
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        HChannel newHChannel = new HChannelImpl(host, channel, this,
                                                getSubscriptionChannelPipelineFactory());
        return storeSubscriptionChannel(host, newHChannel);
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(InetSocketAddress host) {
        HChannel newHChannel = new HChannelImpl(host, this,
                                                getSubscriptionChannelPipelineFactory());
        return storeSubscriptionChannel(host, newHChannel);
    }

    private HChannel storeSubscriptionChannel(InetSocketAddress host,
                                              HChannel newHChannel) {
        // here, we guarantee there is only one channel used to communicate with target
        // host.
        return subscriptionChannels.addChannel(host, newHChannel);
    }

    @Override
    protected HChannel getSubscriptionChannel(InetSocketAddress host) {
        return subscriptionChannels.getChannel(host);
    }

    protected HChannel getSubscriptionChannel(TopicSubscriber subscriber) {
        InetSocketAddress host = topic2Host.get(subscriber.getTopic());
        if (null == host) {
            // we don't know where is the owner of the topic
            return null;
        } else {
            return getSubscriptionChannel(host);
        }
    }

    @Override
    protected HChannel getSubscriptionChannelByTopicSubscriber(TopicSubscriber subscriber) {
        InetSocketAddress host = topic2Host.get(subscriber.getTopic());
        if (null == host) {
            // we don't know where is the topic
            return null;
        } else {
            // we had know which server owned the topic
            HChannel channel = getSubscriptionChannel(host);
            if (null == channel) {
                // create a channel to connect to sepcified host
                channel = createAndStoreSubscriptionChannel(host);
            }
            return channel;
        }
    }

    @Override
    protected void onSubscriptionChannelDisconnected(InetSocketAddress host,
                                                     Channel channel) {
        HChannel hChannel = subscriptionChannels.getChannel(host);
        if (null == hChannel) {
            return;
        }
        Channel underlyingChannel = hChannel.getChannel();
        if (null == underlyingChannel ||
            !underlyingChannel.equals(channel)) {
            return;
        }
        logger.info("Subscription Channel {} disconnected from {}.",
                    va(channel, host));
        // remove existed channel
        if (subscriptionChannels.removeChannel(host, hChannel)) {
            try {
                HChannelHandler channelHandler =
                    HChannelImpl.getHChannelHandlerFromChannel(channel);
                channelHandler.getSubscribeResponseHandler()
                              .onChannelDisconnected(host, channel);
            } catch (NoResponseHandlerException nrhe) {
                logger.warn("No Channel Handler found for channel {} when it disconnected.",
                            channel);
            }
        }
    }

    @Override
    public SubscribeResponseHandler getSubscribeResponseHandler(TopicSubscriber topicSubscriber) {
        HChannel hChannel = getSubscriptionChannel(topicSubscriber);
        if (null == hChannel) {
            return null;
        }
        Channel channel = hChannel.getChannel();
        if (null == channel) {
            return null;
        }
        try {
            HChannelHandler channelHandler =
                HChannelImpl.getHChannelHandlerFromChannel(channel);
            return channelHandler.getSubscribeResponseHandler();
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found for channel {}, topic subscriber {}.",
                        channel, topicSubscriber);
            return null;
        }
    }

    protected void restartDelivery(TopicSubscriber topicSubscriber)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        startDelivery(topicSubscriber, null, true);
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        SubscribeResponseHandler subscribeResponseHandler =
            getSubscribeResponseHandler(topicSubscriber);
        if (null == subscribeResponseHandler ||
            !subscribeResponseHandler.hasSubscription(topicSubscriber)) {
            logger.warn("Trying to close a subscription when we don't have a subscription channel cached for {}",
                        topicSubscriber);
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }
        subscribeResponseHandler.asyncCloseSubscription(topicSubscriber, callback, context);
    }

    @Override
    protected void checkTimeoutRequestsOnSubscriptionChannels() {
        // timeout task may be started before constructing subscriptionChannels
        if (null == subscriptionChannels) {
            return;
        }
        for (HChannel channel : subscriptionChannels.getChannels()) {
            try {
                HChannelHandler channelHandler =
                    HChannelImpl.getHChannelHandlerFromChannel(channel.getChannel());
                channelHandler.checkTimeoutRequests();
            } catch (NoResponseHandlerException nrhe) {
                continue;
            }
        }
    }

    @Override
    protected void closeSubscriptionChannels() {
        subscriptionChannels.close();
    }

}
