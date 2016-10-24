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
package org.apache.bookkeeper.proto;

import java.util.concurrent.TimeUnit;
import com.google.common.base.Stopwatch;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

abstract class PacketProcessorBase extends SafeRunnable {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBase.class);
    final Request request;
    final Channel channel;
    final Bookie bookie;
    final OpStatsLogger channelWriteOpStatsLogger;
    protected final Stopwatch enqueueStopwatch;
    protected final StatsLogger statsLogger;

    PacketProcessorBase(Request request,
                        Channel channel,
                        Bookie bookie,
                        StatsLogger statsLogger) {
        this.request = request;
        this.channel = channel;
        this.bookie = bookie;
        this.enqueueStopwatch = Stopwatch.createStarted();
        this.statsLogger = statsLogger;
        this.channelWriteOpStatsLogger = statsLogger.getOpStatsLogger(CHANNEL_WRITE);
    }

    protected void sendResponse(final int rc, final OpStatsLogger statsLogger, Object response) {
        final Stopwatch writeStopwatch = Stopwatch.createStarted();
        channel.write(response).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {

                long writeMicros = writeStopwatch.elapsed(TimeUnit.MICROSECONDS);
                if (!channelFuture.isSuccess()) {
                    channelWriteOpStatsLogger.registerFailedEvent(writeMicros);
                } else {
                    channelWriteOpStatsLogger.registerSuccessfulEvent(writeMicros);
                }

                long requestMicros = enqueueStopwatch.elapsed(TimeUnit.MICROSECONDS);
                if (BookieProtocol.EOK == rc) {
                    statsLogger.registerSuccessfulEvent(requestMicros);
                } else {
                    statsLogger.registerFailedEvent(requestMicros);
                }
            }
        });
    }

    public boolean isVersionCompatible(Request request) {
        byte version = request.getProtocolVersion();
        if (version < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                || version > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            logger.error("Invalid protocol version. Expected something between " +
                    BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION + " and " +
                    BookieProtocol.CURRENT_PROTOCOL_VERSION + ". Got " + version + ".");
            return false;
        }
        return true;
    }
}
