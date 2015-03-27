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
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.ReadLastConfirmedAndEntryOp;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallbackCtx;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.BookkeeperProtocol.*;
import org.apache.bookkeeper.stats.ClientStatsProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.PCBookieClientStatsLogger;
import org.apache.bookkeeper.stats.PCBookieClientStatsLogger.PCBookieClientOp;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineCoverage;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */

@ChannelPipelineCoverage("one")
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    private final static Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);
    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M
    private final static long SECOND_MICROS = TimeUnit.SECONDS.toMicros(1);
    // TODO: txnId generator per bookie?
    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    // stats logger for 'pre' stage: complection key is removed from map (either response arrived or timeout)
    private final PCBookieClientStatsLogger preStatsLogger;
    // stats logger for 'callback' stage: callback is triggered
    private final PCBookieClientStatsLogger statsLogger;
    /**
     * Maps a completion key to a completion object that is of the respective completion type.
     */
    private final ConcurrentMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentHashMap<CompletionKey, CompletionValue>();

    final BookieSocketAddress addr;
    final ClientSocketChannelFactory channelFactory;
    final OrderedSafeExecutor executor;
    final HashedWheelTimer requestTimer;

    private volatile Queue<GenericCallback<PerChannelBookieClient>> pendingOps =
            new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
    volatile Channel channel = null;

    public enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    }

    volatile ConnectionState state;
    final ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
    private final ClientConfiguration conf;

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  BookieSocketAddress addr) {
        this(new ClientConfiguration(), executor, channelFactory, addr, null, NullStatsLogger.INSTANCE);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor,
                                  ClientSocketChannelFactory channelFactory, BookieSocketAddress addr,
                                  HashedWheelTimer requestTimer, StatsLogger parentStatsLogger) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.statsLogger = ClientStatsProvider.getPCBookieStatsLoggerInstance(conf, addr, parentStatsLogger);
        this.preStatsLogger = ClientStatsProvider.getPCBookieStatsLoggerInstance("pre", conf, addr, parentStatsLogger);
        this.requestTimer = requestTimer;
    }

    private CompletionValue removeCompletionKey(CompletionKey key, boolean completed) {
        CompletionValue value = completionObjects.remove(key);
        if (completed && null != value) {
            long elapsedMicros = MathUtils.elapsedMicroSec(value.requestTimeNanos);
            if (OperationType.ADD_ENTRY == key.operationType && elapsedMicros > SECOND_MICROS) {
                LOG.warn("ADD(txn={}, entry=({}, {})) takes too long {} micros to complete.",
                         new Object[] { key.txnId, value.ledgerId, value.entryId, elapsedMicros });
            }
            this.preStatsLogger.getOpStatsLogger(value.op).registerSuccessfulEvent(elapsedMicros);
        }
        return value;
    }

    private void completeOperation(GenericCallback<PerChannelBookieClient> op,
                                   int rc, PerChannelBookieClient client) {
        closeLock.readLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                op.operationComplete(BKException.Code.ClientClosedException, client);
            } else {
                op.operationComplete(rc, client);
            }
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void connect() {
        LOG.debug("Connecting to bookie: {}", addr);

        // Set up the ClientBootStrap so we can create a new Channel connection
        // to the bookie.
        ClientBootstrap bootstrap = new ClientBootstrap(channelFactory);
        bootstrap.setPipelineFactory(this);
        bootstrap.setOption("tcpNoDelay", conf.getClientTcpNoDelay());
        bootstrap.setOption("keepAlive", true);
        bootstrap.setOption("connectTimeoutMillis", conf.getConnectTimeoutMillis());
        bootstrap.setOption("sendBufferSize", conf.getClientSendBufferSize());
        bootstrap.setOption("receiveBufferSize", conf.getClientReceiveBufferSize());
        bootstrap.setOption("writeBufferLowWaterMark", conf.getClientWriteBufferLowWaterMark());
        bootstrap.setOption("writeBufferHighWaterMark", conf.getClientWriteBufferHighWaterMark());

        final long connectStartNanos = MathUtils.nowInNano();

        ChannelFuture future = bootstrap.connect(addr.getSocketAddress());

        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_CONNECT)
                            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(connectStartNanos));
                } else {
                    statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_CONNECT)
                            .registerFailedEvent(MathUtils.elapsedMicroSec(connectStartNanos));
                }

                int rc;
                Queue<GenericCallback<PerChannelBookieClient>> oldPendingOps;

                synchronized (PerChannelBookieClient.this) {

                    if (future.isSuccess() && state == ConnectionState.CONNECTING) {
                        rc = BKException.Code.OK;
                        channel = future.getChannel();
                        state = ConnectionState.CONNECTED;
                        LOG.debug("Successfully connected to bookie: {} with channel {}", addr, channel);
                    } else if (future.isSuccess() && state == ConnectionState.CONNECTED) {
                        LOG.info("Already connected with another channel {}, so close the new channel {}", channel, future.getChannel());
                        rc = BKException.Code.OK;
                        future.getChannel().close();
                        assert (state == ConnectionState.CONNECTED);
                    } else if (future.isSuccess() && (state == ConnectionState.CLOSED
                                                      || state == ConnectionState.DISCONNECTED)) {
                        LOG.error("Closed before connection completed state {}, clean up: {}", state, future.getChannel());
                        future.getChannel().close();
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                    } else {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Could not connect to bookie: {}", addr);
                        }
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                        if (state != ConnectionState.CLOSED) {
                            state = ConnectionState.DISCONNECTED;
                        }
                    }

                    // trick to not do operations under the lock, take the list
                    // of pending ops and assign it to a new variable, while
                    // emptying the pending ops by just assigning it to a new
                    // list
                    oldPendingOps = pendingOps;
                    pendingOps = new ArrayDeque<GenericCallback<PerChannelBookieClient>>();
                }

                for (GenericCallback<PerChannelBookieClient> pendingOp : oldPendingOps) {
                    completeOperation(pendingOp, rc, PerChannelBookieClient.this);
                }
            }
        });
    }

    void connectIfNeededAndDoOp(GenericCallback<PerChannelBookieClient> op) {
        boolean completeOpNow = false;
        int opRc = BKException.Code.OK;
        // common case without lock first
        if (state == ConnectionState.CONNECTED && channel != null && channel.isConnected()) {
            completeOpNow = true;
        } else {
            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && channel.isConnected() && state == ConnectionState.CONNECTED) {
                    completeOpNow = true;
                    opRc = BKException.Code.OK;
                } else if (state == ConnectionState.CLOSED) {
                    completeOpNow = true;
                    opRc = BKException.Code.BookieHandleNotAvailableException;
                } else {
                    // channel is either null (first connection attempt), or the
                    // channel is disconnected. Connection attempt is still in
                    // progress, queue up this op. Op will be executed when
                    // connection attempt either fails or succeeds
                    pendingOps.add(op);

                    if (state == ConnectionState.CONNECTING) {
                        // just return as connection request has already send
                        // and waiting for the response.
                        return;
                    }
                    // switch state to connecting and do connection attempt
                    state = ConnectionState.CONNECTING;
                }
            }
            if (!completeOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (completeOpNow) {
            completeOperation(op, opRc, this);
        }

    }

    // In case of tear down several exceptions maybe expected and as such should not pollute the log with
    // warning messages. Each such explicitly white-listed exceptions would have their own return code and
    // can be handled without generating any noise in the log
    //
    private interface ChannelRequestCompletionCode {
        int OK = 0;
        int ChannelClosedException = -1;
        int UnknownError = -2;
    }

    private static String getBasicInfoFromRequest(Request request) {
        StringBuilder sb = new StringBuilder("request(txn=")
                .append(request.getHeader().getTxnId())
                .append(", op=")
                .append(request.getHeader().getOperation());
        if (request.hasAddRequest()) {
            sb.append(", add(lid=").append(request.getAddRequest().getLedgerId())
                    .append(", eid=").append(request.getAddRequest().getEntryId())
                    .append(")");
        } else if (request.hasReadRequest()) {
            sb.append(", ").append(request.getReadRequest());
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Write to channel, invoking the write call directly.
     *
     * @param channel
     * @param request
     * @param cb
     */
    private void writeRequestToChannelDirect(final Channel channel, final Request request,
                                             final GenericCallback<Void> cb) {
        final long writeStartNanos = MathUtils.nowInNano();
        try {
            channel.write(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (!channelFuture.isSuccess()) {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_WRITE)
                                .registerFailedEvent(MathUtils.elapsedMicroSec(writeStartNanos));
                        if (channelFuture.getCause() instanceof ClosedChannelException) {
                            cb.operationComplete(ChannelRequestCompletionCode.ChannelClosedException, null);
                        } else {
                            LOG.warn("Writing a request: {} to channel {} failed : cause = {}",
                                    new Object[] { getBasicInfoFromRequest(request), channel,
                                            channelFuture.getCause().getMessage() });
                            cb.operationComplete(ChannelRequestCompletionCode.UnknownError, null);
                        }
                    } else {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_WRITE)
                                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(writeStartNanos));
                        cb.operationComplete(ChannelRequestCompletionCode.OK, null);
                    }
                }
            });
        } catch (Throwable t) {
            LOG.warn("Writing a request:{} to channel:{} failed : ",
                    new Object[] { getBasicInfoFromRequest(request), channel, t });
            cb.operationComplete(-1, null);
        }
    }

    /**
     * Write to channel, invoking the write call via the executor.
     *
     * @param channel
     * @param request
     * @param cb
     */
    private void writeRequestToChannelAsync(final Channel channel, final Request request,
                                            final GenericCallback<Void> cb) {
        executor.submit(new SafeRunnable() {
            @Override
            public void safeRun() {
                writeRequestToChannelDirect(channel, request, cb);
            }
            @Override
            public String toString() {
                return String.format("ChannelWrite(Txn=%d, Type=%s, Addr=%s, HashCode=%h, Request=%s)",
                    request.getHeader().getTxnId(), request.getHeader().getOperation(), addr,
                    System.identityHashCode(PerChannelBookieClient.this), request);
            }
        });
    }

    /**
     * @param channel
     * @param request
     * @param cb
     */
    private void writeRequestToChannel(final Channel channel, final Request request,
                                       final GenericCallback<Void> cb) {
        if (conf.getWriteToChannelAsync()) {
            writeRequestToChannelAsync(channel, request, cb);
        } else {
            writeRequestToChannelDirect(channel, request, cb);
        }
    }

    public void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ChannelBuffer toSend,
                    WriteCallback cb, Object ctx, final int options) {

        final long txnId = getTxnId();
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.ADD_ENTRY);
        completionObjects.put(completionKey,
                new AddCompletion(statsLogger, cb, ctx, ledgerId, entryId,
                                  scheduleTimeout(completionKey, conf.getAddEntryTimeout())));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.ADD_ENTRY)
                .setTxnId(txnId);

        AddRequest.Builder addBuilder = AddRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setBody(ByteString.copyFrom(toSend.toByteBuffer()));

        if (((short)options & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
            addBuilder.setFlag(AddRequest.Flag.RECOVERY_ADD);
        }

        final Request addRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setAddRequest(addBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutAddKey(completionKey);
            return;
        }

        final long writeStartNanos = MathUtils.nowInNano();
        writeRequestToChannel(c, addRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    errorOutAddKey(completionKey);
                } else {
                    // Success
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully wrote request for adding entry: " + entryId + " ledger-id: " + ledgerId
                                + " bookie: " + channel.getRemoteAddress() + " entry length: " + entrySize);
                    }
                }
            }
        });
        statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_WRITE_DISPATCH)
                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(writeStartNanos));
    }

    public void readEntryWaitForLACUpdate(final long ledgerId, final long entryId, final long previousLAC, final long timeOutInMillis, final boolean piggyBackEntry, ReadEntryCallback cb, Object ctx) {
        readEntryInternal(ledgerId, entryId, previousLAC, timeOutInMillis,
            piggyBackEntry, PCBookieClientOp.READ_ENTRY_LONG_POLL, cb, ctx);
    }


    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        readEntryInternal(ledgerId, entryId, null, null, false, PCBookieClientOp.READ_ENTRY, cb, ctx);
    }

    public void readEntryInternal(final long ledgerId, final long entryId, final Long previousLAC,
                                  final Long timeOutInMillis, final boolean piggyBackEntry, final PCBookieClientOp op,
                                  ReadEntryCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
                new ReadCompletion(statsLogger, op, cb, ctx, ledgerId, entryId,
                                   scheduleTimeout(completionKey, conf.getReadEntryTimeout())));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (null != previousLAC) {
            readBuilder = readBuilder.setPreviousLAC(previousLAC);
        }

        if (null != timeOutInMillis) {
            // Long poll requires previousLAC
            if (null == previousLAC) {
                cb.readEntryComplete(BKException.Code.IncorrectParameterException,
                    ledgerId, entryId, null, ctx);
                return;
            }

            readBuilder = readBuilder.setTimeOut(timeOutInMillis);
        }

        if (piggyBackEntry) {
            // Long poll requires previousLAC
            if (null == previousLAC) {
                cb.readEntryComplete(BKException.Code.IncorrectParameterException,
                    ledgerId, entryId, null, ctx);
                return;
            }

            readBuilder = readBuilder.setFlag(ReadRequest.Flag.ENTRY_PIGGYBACK);
        }

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        writeRequestToChannel(channel, readRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    errorOutReadKey(completionKey);
                } else {
                    // Success
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully wrote request for reading entry: " + entryId + " ledger-id: " + ledgerId
                                + " bookie: " + channel.getRemoteAddress());
                    }
                }
            }
        });
    }

    public void readEntryAndFenceLedger(final long ledgerId, byte[] masterKey, final long entryId,
                                          ReadEntryCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey,
            new ReadCompletion(statsLogger, PCBookieClientOp.READ_ENTRY_AND_FENCE,
                               cb, ctx, ledgerId, entryId,
                               scheduleTimeout(completionKey, conf.getReadEntryTimeout())));

        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId)
                .setMasterKey(ByteString.copyFrom(masterKey))
                .setFlag(ReadRequest.Flag.FENCE_LEDGER);

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        final Channel c = channel;
        if (c == null) {
            errorOutReadKey(completionKey);
            return;
        }

        writeRequestToChannel(c, readRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    errorOutReadKey(completionKey);
                } else {
                    // Success
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully wrote request to fence ledger and read entry: " + entryId + " ledger-id: " + ledgerId
                                + " bookie: " + channel.getRemoteAddress());
                    }
                }
            }
        });
    }

    /**
     * Disconnects the bookie client. It can be reused.
     */
    public void disconnect() {
        disconnect(true);
    }

    public void disconnect(boolean wait) {
        LOG.info("Disconnecting the per channel bookie client for {}", addr);
        closeInternal(false, wait);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        close(true);
    }

    public void close(boolean wait) {
        LOG.info("Closing the per channel bookie client for {}", addr);
        closeLock.writeLock().lock();
        try {
            if (ConnectionState.CLOSED == state) {
                return;
            }
            state = ConnectionState.CLOSED;
            errorOutOutstandingEntries(BKException.Code.ClientClosedException);
        } finally {
            closeLock.writeLock().unlock();
        }
        closeInternal(true, wait);
    }

    private void closeInternal(boolean permanent, boolean wait) {
        Channel channelToClose = null;

        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }

            if (null != channel) {
                channelToClose = channel;
            }
        }

        if (null != channelToClose) {
            ChannelFuture cf = channelToClose.close();
            if (wait) {
                cf.awaitUninterruptibly();
            }
        }
    }

    void errorOutReadKey(final CompletionKey key) {
        errorOutReadKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutReadKey(final CompletionKey key, final int rc) {
        final ReadCompletion readCompletion = (ReadCompletion)removeCompletionKey(key, false);
        if (null == readCompletion) {
            return;
        }
        executor.submitOrdered(readCompletion.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                if (null != channel) {
                    bAddress = channel.getRemoteAddress().toString();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for reading entry: {} ledger-id: {} bookie: {}",
                              new Object[] { readCompletion.entryId, readCompletion.ledgerId, bAddress });
                }
                readCompletion.cb.readEntryComplete(rc, readCompletion.ledgerId, readCompletion.entryId,
                                                    null, readCompletion.ctx);
            }

            @Override
            public String toString() {
                return String.format("ErrorOutReadKey(%s, lid=%d, eid=%d)",
                                     key, readCompletion.ledgerId, readCompletion.entryId);
            }
        });
    }

    void errorOutAddKey(final CompletionKey key) {
        errorOutAddKey(key, BKException.Code.BookieHandleNotAvailableException);
    }

    void errorOutAddKey(final CompletionKey key, final int rc) {
        final AddCompletion ac = (AddCompletion)removeCompletionKey(key, false);
        if (null == ac) {
            return;
        }
        executor.submitOrdered(ac.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                String bAddress = "null";
                Channel c = channel;
                if(c != null) {
                    bAddress = c.getRemoteAddress().toString();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for adding entry: {} ledger-id: {} bookie: {}",
                              new Object[] { ac.entryId, ac.ledgerId, bAddress });
                }
                ac.cb.writeComplete(rc, ac.ledgerId, ac.entryId, addr, ac.ctx);
                LOG.debug("Invoked callback method: {}", ac.entryId);
            }

            @Override
            public String toString() {
                return String.format("ErrorOutAddKey(%s, lid=%d, eid=%d)",
                                     key, ac.ledgerId, ac.entryId);
            }
        });
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries(int rc) {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. Make sure that the
        // callback is invoked in the thread responsible for the ledger.
        for (CompletionKey key : completionObjects.keySet()) {
            switch (key.operationType) {
                case ADD_ENTRY:
                    errorOutAddKey(key, rc);
                    break;
                case READ_ENTRY:
                    errorOutReadKey(key, rc);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * In the netty pipeline, we need to split packets based on length, so we
     * use the {@link LengthFieldBasedFrameDecoder}. Other than that all actions
     * are carried out in this class, e.g., making sense of received messages,
     * prepending the length to outgoing packets etc.
     */
    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("lengthbasedframedecoder", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4));
        pipeline.addLast("lengthprepender", new LengthFieldPrepender(4));
        pipeline.addLast("protobufdecoder", new ProtobufDecoder(Response.getDefaultInstance()));
        pipeline.addLast("protobufencoder", new ProtobufEncoder());
        pipeline.addLast("mainhandler", this);
        return pipeline;
    }

    /**
     * If our channel has disconnected, we just error out the pending entries
     */
    @Override
    public void channelDisconnected(ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
        errorOutOutstandingEntries(BKException.Code.BookieHandleNotAvailableException);
        reactToDisconnectedChannel(e);
        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
    }

    private void reactToDisconnectedChannel(ChannelStateEvent disconnectedChannelEvent) {
        if (null == disconnectedChannelEvent.getChannel()) {
            return;
        }

        LOG.info("Disconnecting channel {}", disconnectedChannelEvent.getChannel());
        disconnectedChannelEvent.getChannel().close();

        // If the channel being closed is the same as the current channel then mark the state as
        // disconnected forcing the next request to connect
        synchronized (this) {
            if (disconnectedChannelEvent.getChannel().equals(channel)) {
                if (state != ConnectionState.CLOSED) {
                    LOG.info("Disconnected from bookie: {} Event {}", addr, disconnectedChannelEvent);
                    state = ConnectionState.DISCONNECTED;
                }
            }
        }
    }

    /**
     * Called by netty when an exception happens in one of the netty threads
     * (mostly due to what we do in the netty threads)
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        Throwable t = e.getCause();
        if (t instanceof CorruptedFrameException || t instanceof TooLongFrameException) {
            LOG.error("Corrupted from received from bookie: {}", e.getChannel().getRemoteAddress());
            return;
        }

        if (t instanceof IOException) {
            // these are thrown when a bookie fails, logging them just pollutes
            // the logs (the failure is logged from the listeners on the write
            // operation), so I'll just ignore it here.
            return;
        }

        LOG.error("Unexpected exception caught by bookie client channel handler", t);
        // Since we are a library, cant terminate App here, can we?
    }

    /**
     * Called by netty when a message is received on a channel
     */
    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        if (!(e.getMessage() instanceof Response)) {
            ctx.sendUpstream(e);
            return;
        }

        final Response response = (Response) e.getMessage();
        final BKPacketHeader header = response.getHeader();

        final CompletionValue completionValue = removeCompletionKey(newCompletionKey(header.getTxnId(),
                header.getOperation()), true);
        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + header.getOperation() +
                    " and txnId : " + header.getTxnId());
            }

        } else {
            statsLogger.getOpStatsLogger(PCBookieClientOp.CHANNEL_RESPONSE)
                    .registerSuccessfulEvent(MathUtils.elapsedMicroSec(completionValue.requestTimeNanos));

            long orderingKey = completionValue.ledgerId;
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                @Override
                public void safeRun() {
                    OperationType type = header.getOperation();
                    switch (type) {
                        case ADD_ENTRY:
                            handleAddResponse(response.getAddResponse(), completionValue);
                            break;
                        case READ_ENTRY:
                            handleReadResponse(response.getReadResponse(), completionValue);
                            break;
                        default:
                            LOG.error("Unexpected response, type:{} received from bookie:{}, ignoring",
                                    type, addr);
                            break;
                    }
                }

                @Override
                public String toString() {
                    return String.format("HandleResponse(Txn=%d, Type=%s, Addr=%s, HashCode=%h, Entry=(%d, %d), Status=%s)",
                                         header.getTxnId(), header.getOperation(), addr, System.identityHashCode(PerChannelBookieClient.this),
                                         completionValue.ledgerId, completionValue.entryId, response.getStatus());
                }
            });
        }
    }

    /**
     * Note : Response handler functions for different types of responses follow. One function for each type of response.
     */

    void handleAddResponse(AddResponse response, CompletionValue completionValue) {

        // The completion value should always be an instance of an AddCompletion object when we reach here.
        AddCompletion ac = (AddCompletion)completionValue;

        long ledgerId = response.getLedgerId();
        long entryId = response.getEntryId();
        StatusCode status = response.getStatus();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for add request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                      + entryId + " rc: " + status);
        }
        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Add for ledger: " + ledgerId + ", entry: " + entryId + " failed on bookie: " + addr
                        + " with code:" + status);
            }
            rcToRet = BKException.Code.WriteException;
        }
        ac.cb.writeComplete(rcToRet, ledgerId, entryId, addr, ac.ctx);
    }

    void handleReadResponse(ReadResponse response, CompletionValue completionValue) {

        // The completion value should always be an instance of a ReadCompletion object when we reach here.
        ReadCompletion rc = (ReadCompletion)completionValue;

        long ledgerId = response.getLedgerId();
        long entryId = response.getEntryId();
        StatusCode status = response.getStatus();
        ChannelBuffer buffer = ChannelBuffers.buffer(0);

        if (response.hasBody()) {
            buffer = ChannelBuffers.copiedBuffer(response.getBody().asReadOnlyByteBuffer());
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for read request from bookie: " + addr + " for ledger: " + ledgerId + " entry: "
                      + entryId + " rc: " + rc + " entry length: " + buffer.readableBytes());
        }

        // convert to BKException code because thats what the uppper
        // layers expect. This is UGLY, there should just be one set of
        // error codes.
        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Read entry for ledger:{}, entry:{} failed on bookie:{} with code:{}",
                    new Object[] { ledgerId, entryId, addr, status });
            rcToRet = BKException.Code.ReadException;
        }
        if (response.hasMaxLAC() && (rc.ctx instanceof ReadEntryCallbackCtx)) {
            ((ReadEntryCallbackCtx) rc.ctx).setLastAddConfirmed(response.getMaxLAC());
        }

        if (response.hasLacUpdateTimestamp()) {
            long elapsedMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis() - response.getLacUpdateTimestamp());
            elapsedMicros = Math.max(0, elapsedMicros);
            statsLogger.getOpStatsLogger(PCBookieClientOp.READ_LONG_POLL_RESPONSE)
                .registerSuccessfulEvent(elapsedMicros);

            if (rc.ctx instanceof ReadLastConfirmedAndEntryOp.ReadLastConfirmedAndEntryContext) {
                ((ReadLastConfirmedAndEntryOp.ReadLastConfirmedAndEntryContext) rc.ctx).setLacUpdateTimestamp(response.getLacUpdateTimestamp());
            }
        }

        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, buffer.slice(), rc.ctx);
    }

    /**
     * Note : All completion objects follow. There should be a completion object for each different request type.
     */

    static abstract class CompletionValue {
        private final PCBookieClientOp op;
        public final Object ctx;
        // The ledgerId and entryId values are passed to the callbacks in case of a timeout.
        // TODO: change the callback signatures to remove these.
        protected final long ledgerId;
        protected final long entryId;
        protected final long requestTimeNanos;
        protected final Timeout timeout;

        public CompletionValue(PCBookieClientOp op,
                               Object ctx, long ledgerId, long entryId,
                               Timeout timeout) {
            this.op = op;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.requestTimeNanos = MathUtils.nowInNano();
            this.timeout = timeout;
        }

        void cancelTimeout() {
            if (null != timeout) {
                timeout.cancel();
            }
        }
    }

    static class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(final PCBookieClientStatsLogger statsLogger, final PCBookieClientOp statsOp,
                              final ReadEntryCallback originalCallback,
                              final Object originalCtx, final long ledgerId, final long entryId,
                              final Timeout timeout) {
            super(statsOp, originalCtx, ledgerId, entryId, timeout);
            this.cb = new ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
                    cancelTimeout();
                    if (rc != BKException.Code.OK) {
                        statsLogger.getOpStatsLogger(statsOp)
                            .registerFailedEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
                    } else {
                        statsLogger.getOpStatsLogger(statsOp)
                            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
                    }
                    originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, originalCtx);
                }
            };
        }
    }

    static class AddCompletion extends CompletionValue {
        final WriteCallback cb;

        public AddCompletion(final PCBookieClientStatsLogger statsLogger, final WriteCallback originalCallback,
                             final Object originalCtx, final long ledgerId, final long entryId,
                             final Timeout timeout) {
            super(PCBookieClientOp.ADD_ENTRY, originalCtx, ledgerId, entryId, timeout);
            this.cb = new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
                    cancelTimeout();
                    if (rc != BKException.Code.OK) {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.ADD_ENTRY)
                            .registerFailedEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
                    } else {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.ADD_ENTRY)
                            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
                    }
                    originalCallback.writeComplete(rc, ledgerId, entryId, addr, originalCtx);
                }
            };
        }
    }

    /**
     * Note : Code related to completion keys follows.
     */

    CompletionKey newCompletionKey(long txnId, OperationType operationType) {
        return new CompletionKey(txnId, operationType);
    }

    Timeout scheduleTimeout(CompletionKey key, long timeout) {
        if (null != requestTimer) {
            return requestTimer.newTimeout(key, timeout, TimeUnit.SECONDS);
        } else {
            return null;
        }
    }

    class CompletionKey implements TimerTask {
        final long txnId;
        final OperationType operationType;
        final long requestAt;

        CompletionKey(long txnId, OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
            this.requestAt = MathUtils.nowInNano();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.txnId == that.txnId && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return ((int) txnId);
        }

        @Override
        public String toString() {
            return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
        }

        private long elapsedTimeMicros() {
            return MathUtils.elapsedMicroSec(requestAt);
        }

        @Override
        public void run(Timeout timeout) throws Exception {
            if (timeout.isCancelled()) {
                return;
            }
            if (OperationType.ADD_ENTRY == operationType) {
                errorOutAddKey(this);
                statsLogger.getOpStatsLogger(PCBookieClientOp.NETTY_TIMEOUT_ADD_ENTRY)
                        .registerSuccessfulEvent(elapsedTimeMicros());
            } else {
                errorOutReadKey(this);
                statsLogger.getOpStatsLogger(PCBookieClientOp.NETTY_TIMEOUT_READ_ENTRY)
                        .registerSuccessfulEvent(elapsedTimeMicros());
            }
        }
    }

    /**
     * Note : Helper functions follow
     */

    /**
     * @param status
     * @return null if the statuscode is unknown.
     */
    private Integer statusCodeToExceptionCode(StatusCode status) {
        Integer rcToRet = null;
        switch (status) {
            case EOK:
                rcToRet = BKException.Code.OK;
                break;
            case ENOENTRY:
                rcToRet = BKException.Code.NoSuchEntryException;
                break;
            case ENOLEDGER:
                rcToRet = BKException.Code.NoSuchLedgerExistsException;
                break;
            case EBADVERSION:
                rcToRet = BKException.Code.ProtocolVersionException;
                break;
            case EUA:
                rcToRet = BKException.Code.UnauthorizedAccessException;
                break;
            case EFENCED:
                rcToRet = BKException.Code.LedgerFencedException;
                break;
            case EREADONLY:
                rcToRet = BKException.Code.WriteOnReadOnlyBookieException;
                break;
            default:
                break;
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }
}
