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
package org.apache.hedwig.client.netty.impl.simple;

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
import org.apache.hedwig.exceptions.PubSubException.ClientNotSubscribedException;
import org.apache.hedwig.exceptions.PubSubException.ServiceDownException;
import org.apache.hedwig.filter.ClientMessageFilter;
import org.apache.hedwig.protocol.PubSubProtocol.ResponseBody;
import org.apache.hedwig.protoextensions.SubscriptionStateUtils;
import org.apache.hedwig.util.Callback;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * Simple HChannel Manager which establish a connection for each subscription.
 */
public class SimpleHChannelManager extends AbstractHChannelManager {

    private static Logger logger = LoggerFactory.getLogger(SimpleHChannelManager.class);

    // Concurrent Map to store the cached Channel connections on the client side
    // to a server host for a given Topic + SubscriberId combination. For each
    // TopicSubscriber, we want a unique Channel connection to the server for
    // it. We can also get the ResponseHandler tied to the Channel via the
    // Channel Pipeline.
    protected final CleanupChannelMap<TopicSubscriber> topicSubscriber2Channel;

    // PipelineFactory to create subscription netty channels to the appropriate server
    private final ClientChannelPipelineFactory subscriptionChannelPipelineFactory;

    public SimpleHChannelManager(ClientConfiguration cfg,
                                 ChannelFactory socketFactory) {
        super(cfg, socketFactory);
        topicSubscriber2Channel = new CleanupChannelMap<TopicSubscriber>();
        this.subscriptionChannelPipelineFactory =
            new SimpleSubscriptionChannelPipelineFactory(cfg, this);
    }

    @Override
    protected ClientChannelPipelineFactory getSubscriptionChannelPipelineFactory() {
        return subscriptionChannelPipelineFactory;
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(Channel channel) {
        // for simple channel, we don't store subscription channel now
        // we store it until we received success response
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        return new HChannelImpl(host, channel, this,
                                getSubscriptionChannelPipelineFactory());
    }

    @Override
    protected HChannel createAndStoreSubscriptionChannel(InetSocketAddress host) {
        // for simple channel, we don't store subscription channel now
        // we store it until we received success response
        return new HChannelImpl(host, this,
                                getSubscriptionChannelPipelineFactory());
    }

    protected HChannel storeSubscriptionChannel(TopicSubscriber topicSubscriber,
                                                Channel channel) {
        InetSocketAddress host = NetUtils.getHostFromChannel(channel);
        HChannel newHChannel = new HChannelImpl(host, channel, this,
                                                getSubscriptionChannelPipelineFactory());
        return topicSubscriber2Channel.addChannel(topicSubscriber, newHChannel);
    }

    @Override
    protected HChannel getSubscriptionChannel(InetSocketAddress host) {
        return null;
    }

    @Override
    protected HChannel getSubscriptionChannelByTopicSubscriber(TopicSubscriber subscriber) {
        HChannel channel = topicSubscriber2Channel.getChannel(subscriber);
        if (null != channel) {
            // there is no channel established for this subscription
            return channel;
        } else {
            InetSocketAddress host = topic2Host.get(subscriber.getTopic());
            if (null == host) {
                return null;
            } else {
                channel = getSubscriptionChannel(host);
                if (null == channel) {
                    channel = createAndStoreSubscriptionChannel(host);
                }
                return channel;
            }
        }
    }

    @Override
    protected void onSubscriptionChannelDisconnected(InetSocketAddress host,
                                                     Channel channel) {
        logger.info("Subscription Channel {} disconnected from {}.",
                    va(channel, host));
        try {
            // get hchannel handler
            HChannelHandler channelHandler =
                HChannelImpl.getHChannelHandlerFromChannel(channel);
            channelHandler.getSubscribeResponseHandler()
                          .onChannelDisconnected(host, channel);
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found for channel {} when it disconnected.",
                        channel);
        }
    }

    @Override
    public SubscribeResponseHandler getSubscribeResponseHandler(TopicSubscriber topicSubscriber) {
        HChannel hChannel = topicSubscriber2Channel.getChannel(topicSubscriber);
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

    @Override
    public void startDelivery(TopicSubscriber topicSubscriber,
                              MessageHandler messageHandler)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        startDelivery(topicSubscriber, messageHandler, false);
    }

    protected void restartDelivery(TopicSubscriber topicSubscriber)
        throws ClientNotSubscribedException, AlreadyStartDeliveryException {
        startDelivery(topicSubscriber, null, true);
    }

    @Override
    public void asyncCloseSubscription(final TopicSubscriber topicSubscriber,
                                       final Callback<ResponseBody> callback,
                                       final Object context) {
        HChannel hChannel = topicSubscriber2Channel.removeChannel(topicSubscriber);
        if (null == hChannel) {
            logger.warn("Trying to close a subscription when we don't have a subscribe channel cached for {}",
                        topicSubscriber);
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }

        Channel channel = hChannel.getChannel();
        if (null == channel) {
            callback.operationFinished(context, (ResponseBody)null);
            return;
        }

        try {
            HChannelImpl.getHChannelHandlerFromChannel(channel).closeExplicitly();
        } catch (NoResponseHandlerException nrhe) {
            logger.warn("No Channel Handler found when closing {}'s channel {}.",
                        channel, topicSubscriber);
        }
        ChannelFuture future = channel.close();
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    logger.error("Failed to close the subscription channel for {}",
                                 topicSubscriber);
                    callback.operationFailed(context, new ServiceDownException(
                        "Failed to close the subscription channel for " + topicSubscriber));
                } else {
                    callback.operationFinished(context, (ResponseBody)null);
                }
            }
        });
    }

    @Override
    protected void checkTimeoutRequestsOnSubscriptionChannels() {
        // timeout task may be started before constructing topicSubscriber2Channel
        if (null == topicSubscriber2Channel) {
            return;
        }
        for (HChannel channel : topicSubscriber2Channel.getChannels()) {
            try {
                HChannelHandler channelHandler =
                    HChannelImpl.getHChannelHandlerFromChannel(channel.getChannel());
                channelHandler.checkTimeoutRequests();
            } catch (NoResponseHandlerException nrhe) {
                logger.debug("No Response Handler found for channel {}, Ignore and Continue", channel);
                continue;
            }
        }
    }

    @Override
    protected void closeSubscriptionChannels() {
        topicSubscriber2Channel.close();
    }
}
