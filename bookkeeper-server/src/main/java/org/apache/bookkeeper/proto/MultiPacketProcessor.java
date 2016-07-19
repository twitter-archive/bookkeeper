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

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.DigestManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MonitoredThreadPoolExecutor;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.util.HashedWheelTimer;

import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

/**
 * This class is a packet processor implementation that processes multiple packets at
 * a time. It starts a configurable number of threads that handle read and write requests.
 * Each thread dequeues a packet to be processed from the end of the queue, processes it and
 * sends a response to the NIOServerFactory#Cnxn. Internally this is implemented using
 * an ExecutorService
 */
public class MultiPacketProcessor implements RequestProcessor {

    /**
     * The server configuration. We use this for getting the number of add and read
     * worker threads.
     */
    private final ServerConfiguration serverCfg;

    /**
     * This is the Bookie instance that is used to handle all read and write requests.
     */
    private final Bookie bookie;

    /**
     * The threadpool used to execute all read entry requests issued to this server.
     */
    private final OrderedSafeExecutor readThreadPool;

    /**
     * The threadpool used to execute all add entry requests issued to this server.
     */
    private final OrderedSafeExecutor writeThreadPool;

    /**
     * The threadpool used to execute all long poll requests issued to this server
     * after they are done waiting
     */
    private final ExecutorService longPollThreadPool;

    /**
     * The Timer used to time out requests for long polling
     */
    private final HashedWheelTimer requestTimer;

    // Stats
    protected final StatsLogger statsLogger;

    /**
     * Digest manager used for validating ledger digest before persisting.
     */
    final Optional<DigestManager> macManager;

    public MultiPacketProcessor(ServerConfiguration serverCfg,
                                Bookie bookie,
                                StatsLogger statsLogger)
            throws GeneralSecurityException {
        this.serverCfg = serverCfg;
        this.bookie = bookie;
        this.statsLogger = statsLogger;
        this.readThreadPool =
            createOrderedSafeExecutor(this.serverCfg.getNumReadWorkerThreads(),
                            "BookieReadThread-" + serverCfg.getBookiePort());
        this.writeThreadPool =
            createOrderedSafeExecutor(this.serverCfg.getNumAddWorkerThreads(),
                            "BookieWriteThread-" + serverCfg.getBookiePort());
        this.longPollThreadPool =
            createExecutor(
                    this.serverCfg.getNumLongPollWorkerThreads(),
                    "BookieLongPollThread-" + serverCfg.getBookiePort() + "-%d",
                    OP_LONG_POLL);
        this.requestTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("BookieRequestTimer-%d").build(),
                this.serverCfg.getRequestTimerTickDurationMs(),
                TimeUnit.MILLISECONDS, this.serverCfg.getRequestTimerNumTicks());

        if (serverCfg.isCRC32VerifyEnabled()) {
            this.macManager = Optional.of(DigestManager.instantiate(
                    -1, // ledger id - not used by verify
                    new byte[0], // password not used by crc32
                    BookKeeper.DigestType.CRC32));
        } else {
            this.macManager = Optional.absent();
        }
    }

    private ExecutorService createExecutor(int numThreads, String nameFormat, String scope) {
        if (numThreads <= 0) {
            return null;
        } else {
            return new MonitoredThreadPoolExecutor(numThreads, nameFormat, statsLogger, scope);
        }
    }

    private OrderedSafeExecutor createOrderedSafeExecutor(int numThreads, String name) {
        if (numThreads <= 0) {
            return null;
        }
        return OrderedSafeExecutor.newBuilder()
                .name(name).numThreads(numThreads).statsLogger(statsLogger.scope(BOOKIE_SCOPE)).build();
    }

    private void shutdownExecutor(ExecutorService service) {
        if (null != service) {
            service.shutdown();
        }
    }

    private void shutdownOrderedSafeExecutor(OrderedSafeExecutor service) {
        if (null != service) {
            service.shutdown();
        }
    }

    /**
     * Get the request type of the packet and dispatch a request to the appropriate
     * thread pool.
     * @param msg
     *          received request
     * @param channel
     *          channel that received the request.
     */
    @Override
    public void processRequest(Object msg, Channel channel) {
        // If we can decode this packet as a Request protobuf packet, process
        // it as a version 3 packet. Else, just use the old protocol.
        if (msg instanceof Request) {
            Request request = (Request) msg;
            //logger.info("Packet received : " + request.toString());
            BKPacketHeader header = request.getHeader();
            switch (header.getOperation()) {
            case ADD_ENTRY:
                processAddRequest(channel, new WriteEntryProcessorV3(request, channel, bookie, statsLogger, macManager));
                break;
            case READ_ENTRY:
                ExecutorService fenceThreadPool =
                        null == readThreadPool ? null : readThreadPool.chooseThread(channel);
                if (RequestUtils.isLongPollReadRequest(request.getReadRequest())) {
                    LongPollReadEntryProcessorV3 readProcessor =
                            new LongPollReadEntryProcessorV3(request, channel, bookie, fenceThreadPool,
                                    longPollThreadPool, requestTimer, statsLogger);
                    processLongPollReadRequest(readProcessor);
                } else {
                    ReadEntryProcessorV3 readProcessor =
                            new ReadEntryProcessorV3(request, channel, bookie, fenceThreadPool, statsLogger);
                    processReadRequest(channel, readProcessor);
                }
                break;
            default:
                Response.Builder response = Response.newBuilder().setHeader(request.getHeader())
                        .setStatus(StatusCode.EBADREQ);
                channel.write(response.build());
                BKStats.getInstance().getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                break;
            }
        } else {
            BookieProtocol.Request request = (BookieProtocol.Request) msg;
            // process as a prev v3 packet.
            switch (request.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                processAddRequest(channel, new WriteEntryProcessor(request, channel, bookie, statsLogger));
                break;
            case BookieProtocol.READENTRY:
                processReadRequest(channel, new ReadEntryProcessor(request, channel, bookie, statsLogger));
                break;
            default:
                // We don't know the request type and as a result, the ledgerId or entryId.
                channel.write(ResponseBuilder.buildErrorResponse(BookieProtocol.EBADREQ, request));
                BKStats.getInstance().getOpStats(BKStats.STATS_UNKNOWN).incrementFailedOps();
                break;
            }
        }
    }

    private void processAddRequest(Channel channel, SafeRunnable r) {
        if (null == writeThreadPool) {
            r.run();
        } else {
            writeThreadPool.submitOrdered(channel, r);
        }
    }

    private void processReadRequest(Channel channel, SafeRunnable r) {
        if (null == readThreadPool) {
            r.run();
        } else {
            readThreadPool.submitOrdered(channel, r);
        }
    }

    private void processLongPollReadRequest(SafeRunnable r) {
        if (null == longPollThreadPool) {
            r.run();
        } else {
            longPollThreadPool.submit(r);
        }
    }

    @Override
    public void shutdown() {
        this.requestTimer.stop();
        shutdownOrderedSafeExecutor(readThreadPool);
        shutdownOrderedSafeExecutor(writeThreadPool);
        shutdownExecutor(longPollThreadPool);
    }
}
