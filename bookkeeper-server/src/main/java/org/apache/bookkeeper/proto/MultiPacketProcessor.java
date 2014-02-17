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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.stats.Stats;
import org.apache.bookkeeper.util.MonitoredThreadPoolExecutor;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

/**
 * This class is a packet processor implementation that processes multiple packets at
 * a time. It starts a configurable number of threads that handle read and write requests.
 * Each thread dequeues a packet to be processed from the end of the queue, processes it and
 * sends a response to the NIOServerFactory#Cnxn. Internally this is implemented using
 * an ExecutorService
 */
public class MultiPacketProcessor implements NIOServerFactory.PacketProcessor {

    private final static Logger logger = LoggerFactory.getLogger(MultiPacketProcessor.class);
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

    public MultiPacketProcessor(ServerConfiguration serverCfg, Bookie bookie) {
        this.serverCfg = serverCfg;
        this.bookie = bookie;
        this.readThreadPool =
            createOrderedSafeExecutor(this.serverCfg.getNumReadWorkerThreads(),
                            "BookieReadThread-" + serverCfg.getBookiePort());
        this.writeThreadPool =
            createOrderedSafeExecutor(this.serverCfg.getNumAddWorkerThreads(),
                            "BookieWriteThread-" + serverCfg.getBookiePort());
        this.longPollThreadPool =
            createExecutor(this.serverCfg.getNumLongPollWorkerThreads(),
                            "BookieLongPollThread-" + serverCfg.getBookiePort() + "-%d",
                            BookkeeperServerStatsLogger.BookkeeperServerGauge.NUM_PENDING_LONG_POLL);
        this.requestTimer = new HashedWheelTimer(this.serverCfg.getRequestTimerTickDurationMs(), TimeUnit.MILLISECONDS, this.serverCfg.getRequestTimerNumTicks());
    }

    private ExecutorService createExecutor(int numThreads, String nameFormat, BookkeeperServerStatsLogger.BookkeeperServerGauge guage) {
        if (numThreads <= 0) {
            return null;
        } else {
            return new MonitoredThreadPoolExecutor(numThreads, nameFormat, ServerStatsProvider.getStatsLoggerInstance(), guage);
        }
    }

    private OrderedSafeExecutor createOrderedSafeExecutor(int numThreads, String name) {
        if (numThreads <= 0) {
            return null;
        }
        return OrderedSafeExecutor.newBuilder()
                .name(name).numThreads(numThreads).statsLogger(Stats.get().getStatsLogger("bookie")).build();
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
     * @param packet
     * @param srcConn
     */
    public void processPacket(ByteBuffer packet, Cnxn srcConn) {
        // If we can decode this packet as a Request protobuf packet, process
        // it as a version 3 packet. Else, just use the old protocol.
        try {
            Request request = Request.parseFrom(ByteString.copyFrom(packet));
            //logger.info("Packet received : " + request.toString());
            BKPacketHeader header = request.getHeader();
            switch (header.getOperation()) {
                case ADD_ENTRY:
                    processAddRequest(srcConn, new WriteEntryProcessorV3(request, srcConn, bookie));
                    break;
                case READ_ENTRY:
                    processReadRequest(srcConn,
                            new ReadEntryProcessorV3(request, srcConn, bookie,
                                                     null == readThreadPool ? null : readThreadPool.chooseThread(srcConn),
                                                     longPollThreadPool, requestTimer));
                    break;
                default:
                    Response.Builder response = Response.newBuilder()
                            .setHeader(request.getHeader())
                            .setStatus(StatusCode.EBADREQ);
                    srcConn.sendResponse(ByteBuffer.wrap(response.build().toByteArray()));
                    break;
            }
        } catch (InvalidProtocolBufferException e) {
            // process as a normal packet.
            // Rewind the packet as ByteString.copyFrom consumes all remaining bytes.
            packet.rewind();
            PacketHeader header = PacketHeader.fromInt(packet.getInt());
            packet.rewind();
            // The ByteBuffer is already allocated by the NIOServerFactory, so we shouldn't
            // copy it here. It's safe to pass it on as is.
            switch (header.getOpCode()) {
                case BookieProtocol.ADDENTRY:
                    processAddRequest(srcConn, new WriteEntryProcessor(packet, srcConn, bookie));
                    break;
                case BookieProtocol.READENTRY:
                    processReadRequest(srcConn, new ReadEntryProcessor(packet, srcConn, bookie));
                    break;
                default:
                    // We don't know the request type and as a result, the ledgerId or entryId.
                    srcConn.sendResponse(PacketProcessorBase.buildResponse(BookieProtocol.EBADREQ,
                            header.getVersion(), header.getOpCode(), -1, BookieProtocol.INVALID_ENTRY_ID));
                    break;
            }
        }
    }

    private void processAddRequest(Cnxn cnxn, SafeRunnable r) {
        if (null == writeThreadPool) {
            r.run();
        } else {
            writeThreadPool.submitOrdered(cnxn, r);
        }
    }

    private void processReadRequest(Cnxn cnxn, SafeRunnable r) {
        if (null == readThreadPool) {
            r.run();
        } else {
            readThreadPool.submitOrdered(cnxn, r);
        }
    }

    public void shutdown() {
        this.requestTimer.stop();
        shutdownOrderedSafeExecutor(readThreadPool);
        shutdownOrderedSafeExecutor(writeThreadPool);
        shutdownExecutor(longPollThreadPool);
    }
}
