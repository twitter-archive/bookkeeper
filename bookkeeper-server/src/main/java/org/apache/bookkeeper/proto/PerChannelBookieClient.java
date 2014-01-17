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
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
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
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.CorruptedFrameException;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.frame.TooLongFrameException;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */

@ChannelPipelineCoverage("one")
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    private final static Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);
    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M
    // TODO: txnId generator per bookie?
    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    private final PCBookieClientStatsLogger statsLogger;
    /**
     * Maps a completion key to a completion object that is of the respective completion type.
     */
    private final ConcurrentMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentHashMap<CompletionKey, CompletionValue>();

    InetSocketAddress addr;
    AtomicLong totalBytesOutstanding;
    ClientSocketChannelFactory channelFactory;
    OrderedSafeExecutor executor;
    ScheduledExecutorService timeoutExecutor;
    // TODO(Aniruddha): Remove this completely or should we have a lower read timeout and a somewhat higher timeout task interval
    private Timer readTimeoutTimer;

    private volatile Queue<GenericCallback<Void>> pendingOps = new ArrayDeque<GenericCallback<Void>>();
    volatile Channel channel = null;

    /**
     * This task is submitted to the scheduled executor service thread. It periodically wakes up
     * and errors out entries that have timed out.
     */
    private class TimeoutTask implements Runnable {
        @Override
        public void run() {
            errorOutTimedOutEntries(false);
        }
    }

    public enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED, CLOSED
    };

    volatile ConnectionState state;
    private final ClientConfiguration conf;

    /**
     * Error out any entries that have timed out.
     */
    private void errorOutTimedOutEntries(boolean isNettyTimeout) {
        int numAdd = 0, numRead = 0;
        int total = 0;
        for (CompletionKey key : PerChannelBookieClient.this.completionObjects.keySet()) {
            total++;
            long elapsedTime = key.elapsedTime();
            try {
                switch (key.operationType) {
                    case ADD_ENTRY:
                        if (elapsedTime < conf.getAddEntryTimeout() * 1000) {
                            continue;
                        }
                        errorOutAddKey(key);
                        numAdd++;
                        if (isNettyTimeout) {
                            statsLogger.getOpStatsLogger(PCBookieClientOp.NETTY_TIMEOUT_ADD_ENTRY)
                                    .registerSuccessfulEvent(elapsedTime);
                        } else {
                            statsLogger.getOpStatsLogger(PCBookieClientOp.TIMEOUT_ADD_ENTRY)
                                    .registerSuccessfulEvent(elapsedTime);
                        }
                        break;
                    case READ_ENTRY:
                        if (elapsedTime < conf.getReadEntryTimeout() * 1000) {
                            continue;
                        }
                        errorOutReadKey(key);
                        numRead++;
                        if (isNettyTimeout) {
                            statsLogger.getOpStatsLogger(PCBookieClientOp.NETTY_TIMEOUT_READ_ENTRY)
                                    .registerSuccessfulEvent(elapsedTime);
                        } else {
                            statsLogger.getOpStatsLogger(PCBookieClientOp.TIMEOUT_READ_ENTRY)
                                    .registerSuccessfulEvent(elapsedTime);
                        }
                        break;
                }
            } catch (RuntimeException e) {
                LOG.error("Caught RuntimeException while erroring out key " + key + " : ", e);
            }
        }
        if (numAdd + numRead > 0) {
            LOG.warn("Timeout task iterated through a total of {} keys.", total);
            LOG.warn("Timeout Task errored out {} add entry requests.", numAdd);
            LOG.warn("Timeout Task errored out {} read entry requests.", numRead);
        }
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding, ScheduledExecutorService timeoutExecutor) {
        this(new ClientConfiguration(), executor, channelFactory, addr, totalBytesOutstanding, timeoutExecutor,
                NullStatsLogger.INSTANCE);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding) {
        this(new ClientConfiguration(), executor, channelFactory, addr, totalBytesOutstanding, null,
                NullStatsLogger.INSTANCE);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding, ScheduledExecutorService timeoutExecutor,
                                  StatsLogger parentStatsLogger) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.totalBytesOutstanding = totalBytesOutstanding;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.readTimeoutTimer = null;
        this.statsLogger = ClientStatsProvider.getPCBookieStatsLoggerInstance(addr, parentStatsLogger);
        this.timeoutExecutor = timeoutExecutor;
        // Schedule the timeout task
        if (null != this.timeoutExecutor) {
            this.timeoutExecutor.scheduleWithFixedDelay(new TimeoutTask(), conf.getTimeoutTaskIntervalMillis(),
                    conf.getTimeoutTaskIntervalMillis(), TimeUnit.MILLISECONDS);
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

        ChannelFuture future = bootstrap.connect(addr);

        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                int rc;
                Queue<GenericCallback<Void>> oldPendingOps;

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
                        LOG.warn("Could not connect to bookie: " + addr);
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
                    pendingOps = new ArrayDeque<GenericCallback<Void>>();
                }

                for (GenericCallback<Void> pendingOp : oldPendingOps) {
                    pendingOp.operationComplete(rc, null);
                }
            }
        });
    }

    void connectIfNeededAndDoOp(GenericCallback<Void> op) {
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
            op.operationComplete(opRc, null);
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


    /**
     * @param channel
     * @param request
     * @param cb
     */
    private void writeRequestToChannel(final Channel channel, final Request request,
                                       final GenericCallback<Void> cb) {
        try {
            channel.write(request).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                    if (!channelFuture.isSuccess()) {
                        if (channelFuture.getCause() instanceof ClosedChannelException) {
                            cb.operationComplete(ChannelRequestCompletionCode.ChannelClosedException, null);
                        } else {
                            LOG.warn("Writing a request:" + request + " to channel:" + channel + " failed",
                                 channelFuture.getCause());
                            cb.operationComplete(ChannelRequestCompletionCode.UnknownError, null);
                        }
                    } else {
                        cb.operationComplete(ChannelRequestCompletionCode.OK, null);
                    }
                }
            });
        } catch (Throwable t) {
            LOG.warn("Writing a request:" + request + " to channel:" + channel + " failed.", t);
            cb.operationComplete(-1, null);
        }
    }

    public void addEntry(final long ledgerId, byte[] masterKey, final long entryId, ChannelBuffer toSend,
                    WriteCallback cb, Object ctx, final int options) {

        final long txnId = getTxnId();
        final int entrySize = toSend.readableBytes();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.ADD_ENTRY);
        completionObjects.put(completionKey, new AddCompletion(statsLogger, cb, ctx, ledgerId, entryId));

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

        writeRequestToChannel(channel, addRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    if (rc == ChannelRequestCompletionCode.UnknownError) {
                        LOG.warn("Add entry operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
                    }
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
    }

    public void readEntry(final long ledgerId, final long entryId, ReadEntryCallback cb, Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.READ_ENTRY);
        completionObjects.put(completionKey, new ReadCompletion(statsLogger, cb, ctx, ledgerId, entryId));

        // Build the request and calculate the total size to be included in the packet.
        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.READ_ENTRY)
                .setTxnId(txnId);

        ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        final Request readRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setReadRequest(readBuilder)
                .build();

        writeRequestToChannel(channel, readRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    if (rc == ChannelRequestCompletionCode.UnknownError) {
                        LOG.warn("Read entry operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
                    }
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
        completionObjects.put(completionKey, new ReadCompletion(statsLogger, cb, ctx, ledgerId, entryId));

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

        writeRequestToChannel(channel, readRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    if (rc == ChannelRequestCompletionCode.UnknownError) {
                        LOG.warn("Read entry and fence operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
                    }
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
        LOG.info("Disconnecting the per channel bookie client for {}", addr);
        closeInternal(false);
    }

    /**
     * Closes the bookie client permanently. It cannot be reused.
     */
    public void close() {
        LOG.info("Closing the per channel bookie client for {}", addr);
        closeInternal(true);
    }

    private void closeInternal(boolean permanent) {
        Channel channelToClose = null;
        Timer timerToStop = null;

        synchronized (this) {
            if (permanent) {
                state = ConnectionState.CLOSED;
            } else if (state != ConnectionState.CLOSED) {
                state = ConnectionState.DISCONNECTED;
            }

            if (null != channel) {
                channelToClose = channel;
                timerToStop = readTimeoutTimer;
                readTimeoutTimer = null;
            }
        }

        if (null != channelToClose) {
            channelToClose.close().awaitUninterruptibly();
        }

        if (null != timerToStop) {
            timerToStop.stop();
        }
    }

    void errorOutReadKey(final CompletionKey key) {
        final ReadCompletion rc = (ReadCompletion)completionObjects.remove(key);
        if (null == rc) {
            return;
        }
        executor.submitOrdered(rc.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                if (null != channel) {
                    bAddress = channel.getRemoteAddress().toString();
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for reading entry: " + rc.entryId + " ledger-id: "
                              + rc.ledgerId + " bookie: " + bAddress);
                }
                rc.cb.readEntryComplete(BKException.Code.BookieHandleNotAvailableException,
                                        rc.ledgerId, rc.entryId, null, rc.ctx);
            }
        });
    }

    void errorOutAddKey(final CompletionKey key) {
        final AddCompletion ac = (AddCompletion)completionObjects.remove(key);
        if (null == ac) {
            return;
        }
        executor.submitOrdered(ac.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {

                String bAddress = "null";
                if(channel != null)
                    bAddress = channel.getRemoteAddress().toString();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Could not write request for adding entry: " + ac.entryId + " ledger-id: "
                          + ac.ledgerId + " bookie: " + bAddress);
                    LOG.debug("Invoked callback method: " + ac.entryId);
                }

                ac.cb.writeComplete(BKException.Code.BookieHandleNotAvailableException, ac.ledgerId,
                                    ac.entryId, addr, ac.ctx);
            }
        });
    }

    /**
     * Errors out pending entries. We call this method from one thread to avoid
     * concurrent executions to QuorumOpMonitor (implements callbacks). It seems
     * simpler to call it from BookieHandle instead of calling directly from
     * here.
     */

    void errorOutOutstandingEntries() {

        // DO NOT rewrite these using Map.Entry iterations. We want to iterate
        // on keys and see if we are successfully able to remove the key from
        // the map. Because the add and the read methods also do the same thing
        // in case they get a write failure on the socket. Make sure that the
        // callback is invoked in the thread responsible for the ledger.
        for (CompletionKey key : completionObjects.keySet()) {
            switch (key.operationType) {
                case ADD_ENTRY:
                    errorOutAddKey(key);
                    break;
                case READ_ENTRY:
                    errorOutReadKey(key);
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

        if (readTimeoutTimer == null) {
            readTimeoutTimer = new HashedWheelTimer();
        }

        pipeline.addLast("readTimeout", new ReadTimeoutHandler(readTimeoutTimer,
                                                               conf.getReadTimeout()));
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
        errorOutOutstandingEntries();
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
            LOG.error("Corrupted fram received from bookie: "
                      + e.getChannel().getRemoteAddress());
            return;
        }
        if (t instanceof ReadTimeoutException) {
            errorOutTimedOutEntries(true);
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

        final CompletionValue completionValue = completionObjects.remove(newCompletionKey(header.getTxnId(),
                header.getOperation()));
        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            if (LOG.isDebugEnabled()) {
                LOG.debug("Unexpected response received from bookie : " + addr + " for type : " + header.getOperation() +
                    " and txnId : " + header.getTxnId());
            }

        } else {
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
                            LOG.error("Unexpected response, type:" + type + " received from bookie:" +
                                    addr + ", ignoring");
                            break;
                    }
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
            LOG.error("Read entry for ledger:" + ledgerId + ", entry:" + entryId + " failed on bookie:" + addr
                    + " with code:" + status);
            rcToRet = BKException.Code.ReadException;
        }

        rc.cb.readEntryComplete(rcToRet, ledgerId, entryId, buffer.slice(), rc.ctx);
    }

    /**
     * Note : All completion objects follow. There should be a completion object for each different request type.
     */

    static abstract class CompletionValue {
        public final Object ctx;
        // The ledgerId and entryId values are passed to the callbacks in case of a timeout.
        // TODO: change the callback signatures to remove these.
        protected final long ledgerId;
        protected final long entryId;

        public CompletionValue(Object ctx, long ledgerId, long entryId) {
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }
    }

    static class ReadCompletion extends CompletionValue {
        final ReadEntryCallback cb;

        public ReadCompletion(final PCBookieClientStatsLogger statsLogger, final ReadEntryCallback originalCallback,
                              final Object originalCtx, final long ledgerId, final long entryId) {
            super(originalCtx, ledgerId, entryId);
            final long requestTimeMillis = MathUtils.now();
            this.cb = new ReadEntryCallback() {
                @Override
                public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
                    long latencyMillis = MathUtils.now() - requestTimeMillis;
                    if (rc != BKException.Code.OK) {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.READ_ENTRY).registerFailedEvent(latencyMillis);
                    } else {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.READ_ENTRY).registerSuccessfulEvent(latencyMillis);
                    }
                    originalCallback.readEntryComplete(rc, ledgerId, entryId, buffer, originalCtx);
                }
            };
        }
    }

    static class AddCompletion extends CompletionValue {
        final WriteCallback cb;

        public AddCompletion(final PCBookieClientStatsLogger statsLogger, final WriteCallback originalCallback,
                             final Object originalCtx, final long ledgerId, final long entryId) {
            super(originalCtx, ledgerId, entryId);
            final long requestTimeMillis = MathUtils.now();
            this.cb = new WriteCallback() {
                @Override
                public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
                    long latencyMillis = MathUtils.now() - requestTimeMillis;
                    if (rc != BKException.Code.OK) {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.ADD_ENTRY).registerFailedEvent(latencyMillis);
                    } else {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.ADD_ENTRY).registerSuccessfulEvent(latencyMillis);
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

    class CompletionKey {
        public final long txnId;
        public final OperationType operationType;
        public final long requestAt;

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

        public boolean shouldTimeout() {
            return elapsedTime() >= conf.getReadTimeout() * 1000;
        }

        public long elapsedTime() {
            return MathUtils.elapsedMSec(requestAt);
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
        }
        return rcToRet;
    }

    private long getTxnId() {
        return txnIdGenerator.incrementAndGet();
    }
}
