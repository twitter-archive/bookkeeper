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
package org.apache.hedwig.server.handlers;

import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.server.stats.ServerStatsProvider;
import org.jboss.netty.channel.Channel;

import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.ConsumeRequest;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.protocol.PubSubProtocol.PubSubRequest;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.netty.UmbrellaHandler;
import org.apache.hedwig.server.subscriptions.SubscriptionManager;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.util.Callback;

public class ConsumeHandler extends BaseHandler {

    SubscriptionManager sm;
    final OpStatsLogger consumeStatsLogger = ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(OperationType.CONSUME);

    @Override
    public void handleRequestAtOwner(PubSubRequest request, Channel channel) {
        final long requestTimeMillis = MathUtils.now();
        if (!request.hasConsumeRequest()) {
            UmbrellaHandler.sendErrorResponseToMalformedRequest(channel, request.getTxnId(),
                    "Missing consume request data");
            // We don't collect consume process time.
            consumeStatsLogger.registerFailedEvent(MathUtils.now() - requestTimeMillis);
            return;
        }

        ConsumeRequest consumeRequest = request.getConsumeRequest();

        sm.setConsumeSeqIdForSubscriber(request.getTopic(), consumeRequest.getSubscriberId(),
                                        consumeRequest.getMsgId(), new Callback<Void>() {
            @Override
            public void operationFinished(Object ctx, Void ignoreVal) {
                consumeStatsLogger.registerSuccessfulEvent(MathUtils.now() - requestTimeMillis);
            }

            @Override
            public void operationFailed(Object ctx, PubSubException e) {
                consumeStatsLogger.registerFailedEvent(MathUtils.now() - requestTimeMillis);
            }
        }, null);

    }

    public ConsumeHandler(TopicManager tm, SubscriptionManager sm, ServerConfiguration cfg) {
        super(tm, cfg);
        this.sm = sm;
    }
}
