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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.RangeReadCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.ClientStatsProvider;
import org.apache.bookkeeper.stats.PCBookieClientStatsLogger;
import org.apache.bookkeeper.stats.PCBookieClientStatsLogger.PCBookieClientOp;
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
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.RangeReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.RangeReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;

import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalReadRequest;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalReadResponse;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalRangeReadRequest;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalRangeReadResponse;

/**
 * This class manages all details of connection to a particular bookie. It also
 * has reconnect logic if a connection to a bookie fails.
 *
 */

@ChannelPipelineCoverage("one")
public class PerChannelBookieClient extends SimpleChannelHandler implements ChannelPipelineFactory {

    public final Logger LOG = LoggerFactory.getLogger(PerChannelBookieClient.class);
    public static final int MAX_FRAME_LENGTH = 2 * 1024 * 1024; // 2M
    // TODO: txnId generator per bookie?
    public static final AtomicLong txnIdGenerator = new AtomicLong(0);

    private final PCBookieClientStatsLogger statsLogger;
    private final ClientConfiguration conf;
    /**
     * Maps a completion key to a completion object that is of the respective completion type.
     */
    private final ConcurrentMap<CompletionKey, CompletionValue> completionObjects = new ConcurrentHashMap<CompletionKey, CompletionValue>();

    private AtomicLong totalBytesOutstanding;
    private InetSocketAddress addr;
    private ClientSocketChannelFactory channelFactory;
    private OrderedSafeExecutor executor;
    private ScheduledExecutorService timeoutExecutor;
    private Timer readTimeoutTimer;

    private volatile Queue<GenericCallback<Void>> pendingOps = new ArrayDeque<GenericCallback<Void>>();
    private volatile Channel channel = null;
    private volatile ConnectionState state;

    /**
     * This task is submitted to the scheduled executor service thread. It periodically wakes up
     * and errors out entries that have timed out.
     */
    private class TimeoutTask implements Runnable {
        public void run() {
            errorOutTimedOutEntries();
        }
    }

    private enum ConnectionState {
        DISCONNECTED, CONNECTING, CONNECTED
    };

    /**
     * Error out any entries that have timed out.
     */
    private void errorOutTimedOutEntries() {
        int numAdd = 0, numRead = 0, numRangeRead = 0;
        int total = 0;
        for (CompletionKey key : PerChannelBookieClient.this.completionObjects.keySet()) {
            total++;
            if (!key.shouldTimeout()) {
                continue;
            }
            try {
                switch (key.operationType) {
                    case ADD_ENTRY:
                        errorOutAddKey(key);
                        numAdd++;
                        break;
                    case READ_ENTRY:
                        errorOutReadKey(key);
                        numRead++;
                        break;
                    case RANGE_READ_ENTRY:
                        errorOutRangeReadKey(key);
                        numRangeRead++;
                        break;
                }
            } catch (RuntimeException e) {
                LOG.error("Caught RuntimeException while erroring out key:" + key.toString());
            }
        }
        if (numAdd + numRead + numRangeRead > 0) {
            LOG.warn("Timeout task iterated through a total of " + total + " keys.");
            LOG.warn("Timeout Task errored out " + numAdd + " add entry requests");
            LOG.warn("Timeout Task errored out " + numRead + " read entry requests");
            LOG.warn("Timeout Task errored out " + numRangeRead + " range read entry requests");
        }
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding, ScheduledExecutorService timeoutExecutor) {
        this(new ClientConfiguration(), executor, channelFactory, addr, totalBytesOutstanding, timeoutExecutor);
    }

    public PerChannelBookieClient(OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding) {
        this(new ClientConfiguration(), executor, channelFactory, addr, totalBytesOutstanding, null);
    }

    public PerChannelBookieClient(ClientConfiguration conf, OrderedSafeExecutor executor, ClientSocketChannelFactory channelFactory,
                                  InetSocketAddress addr, AtomicLong totalBytesOutstanding, ScheduledExecutorService timeoutExecutor) {
        this.conf = conf;
        this.addr = addr;
        this.executor = executor;
        this.totalBytesOutstanding = totalBytesOutstanding;
        this.channelFactory = channelFactory;
        this.state = ConnectionState.DISCONNECTED;
        this.readTimeoutTimer = null;
        this.statsLogger = ClientStatsProvider.getPCBookieStatsLoggerInstance(addr);
        this.timeoutExecutor = timeoutExecutor;
        // Schedule the timeout task
        if (null != this.timeoutExecutor) {
            this.timeoutExecutor.scheduleWithFixedDelay(new TimeoutTask(), conf.getTimeoutTaskIntervalMillis(),
                    conf.getTimeoutTaskIntervalMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void connect() {
        if (LOG.isDebugEnabled())
            LOG.debug("Connecting to bookie: " + addr);

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

                    if (future.isSuccess()) {
                        LOG.info("Successfully connected to bookie: " + addr);
                        rc = BKException.Code.OK;
                        channel = future.getChannel();
                        state = ConnectionState.CONNECTED;
                    } else {
                        LOG.error("Could not connect to bookie: " + addr);
                        rc = BKException.Code.BookieHandleNotAvailableException;
                        channel = null;
                        state = ConnectionState.DISCONNECTED;
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
        boolean doOpNow = false;

        // common case without lock first
        if (channel != null && state == ConnectionState.CONNECTED) {
            doOpNow = true;
        } else {

            synchronized (this) {
                // check the channel status again under lock
                if (channel != null && state == ConnectionState.CONNECTED) {
                    doOpNow = true;
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
            if (!doOpNow) {
                // Start connection attempt to the input server host.
                connect();
            }
        }

        if (doOpNow) {
            op.operationComplete(BKException.Code.OK, null);
        }

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
                        LOG.warn("Writing a request:" + request.toString() + " to channel:" + channel.toString() + " failed",
                                 channelFuture.getCause());
                        cb.operationComplete(-1, null);
                    } else {
                        cb.operationComplete(0, null);
                    }
                }
            });
        } catch (Throwable t) {
            LOG.warn("Writing a request:" + request.toString() + " to channel:" + channel.toString() + " failed.", t);
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
                    LOG.warn("Add entry operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
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
                    LOG.warn("Read entry operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
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
                    LOG.warn("Read entry and fence operation for ledger:" + ledgerId + " and entry:" + entryId + " failed.");
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

    public void rangeReadEntry(final InternalRangeReadRequest request, final RangeReadCallback cb,
                               final Object ctx) {
        final long txnId = getTxnId();
        final CompletionKey completionKey = new CompletionKey(txnId, OperationType.RANGE_READ_ENTRY);
        completionObjects.put(completionKey, new RangeReadCompletion(statsLogger, cb, ctx, request.ledgerId));

        BKPacketHeader.Builder headerBuilder = BKPacketHeader.newBuilder()
                .setVersion(ProtocolVersion.VERSION_THREE)
                .setOperation(OperationType.RANGE_READ_ENTRY)
                .setTxnId(txnId);

        RangeReadRequest.Builder rangeReadBuilder = RangeReadRequest.newBuilder()
                .setNumRequest(request.numRequests);
        for (InternalReadRequest req : request.requests.keySet()) {
            ReadRequest.Builder readBuilder = ReadRequest.newBuilder()
                    .setLedgerId(req.ledgerId)
                    .setEntryId(req.entryId);
            rangeReadBuilder.addRequests(readBuilder.build());
        }

        final Request rangeReadRequest = Request.newBuilder()
                .setHeader(headerBuilder)
                .setRangeReadRequest(rangeReadBuilder)
                .build();

        writeRequestToChannel(channel, rangeReadRequest, new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (rc != 0) {
                    LOG.warn("Range read for ledger:" + request.ledgerId + " with numRequests:" + request.numRequests
                             + " failed.");
                    errorOutRangeReadKey(completionKey);
                } else {
                    // Success
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Successfully wrote range read request to read " + request.numRequests + " entries from ledger:" +
                                  request.ledgerId + " to bookie:" + channel.getRemoteAddress());
                    }
                }
            }
        });
    }

    public void close() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        if (readTimeoutTimer != null) {
            readTimeoutTimer.stop();
            readTimeoutTimer = null;
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
                LOG.error("Could not write request with txnId:" + key.txnId + " for reading entry: " + rc.entryId + " ledger-id: "
                          + rc.ledgerId + " bookie: " + bAddress);
                rc.cb.readEntryComplete(BKException.Code.BookieHandleNotAvailableException,
                                        rc.ledgerId, rc.entryId, null, rc.ctx);
            }
        });
    }

    void errorOutRangeReadKey(final CompletionKey key) {
        final RangeReadCompletion rrc = (RangeReadCompletion)completionObjects.remove(key);
        if (null == rrc) {
            return;
        }
        executor.submitOrdered(rrc.ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                String bAddress = "null";
                if(channel != null) {
                    bAddress = channel.getRemoteAddress().toString();
                }
                LOG.error("Could not write range read request with txnId:" + key.txnId +" for reading ledger:"
                          + rrc.ledgerId + " to bookie: " + bAddress);
                // No response to send, so send an empty response.
                rrc.cb.rangeReadComplete(BKException.Code.BookieHandleNotAvailableException, new InternalRangeReadResponse(),
                        rrc.ctx);
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
                if(channel != null) {
                    bAddress = channel.getRemoteAddress().toString();
                }
                LOG.error("Could not write request with txnId:" + key.txnId + " for adding entry: " + ac.entryId + " ledger-id: "
                          + ac.ledgerId + " bookie: " + bAddress);
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
                case RANGE_READ_ENTRY:
                    errorOutRangeReadKey(key);
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
        LOG.info("Disconnected from bookie: " + addr);
        errorOutOutstandingEntries();
        channel.close();
        synchronized (this) {
            state = ConnectionState.DISCONNECTED;
        }

        // we don't want to reconnect right away. If someone sends a request to
        // this address, we will reconnect.
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
            errorOutTimedOutEntries();
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Received response for request with txnId:" + header.getTxnId());
        }
        final CompletionValue completionValue = completionObjects.remove(newCompletionKey(header.getTxnId(),
                header.getOperation()));
        if (null == completionValue) {
            // Unexpected response, so log it. The txnId should have been present.
            LOG.error("Unexpected response received from bookie : " + addr + " for type : " + header.getOperation() +
                    " and txnId : " + header.getTxnId());

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
                        case RANGE_READ_ENTRY:
                            handleRangeReadResponse(response.getRangeReadResponse(), completionValue);
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
            LOG.error("Add for ledger: " + ledgerId + ", entry: " + entryId + " failed on bookie: " + addr
                    + " with code:" + status);
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
                      + entryId + " rc: " + status + "entry length: " + buffer.readableBytes());
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

    void handleRangeReadResponse(RangeReadResponse response, CompletionValue completionValue) {
        // The completion value should always be an instance of a RangeReadCompletion object when we reach here
        RangeReadCompletion rrc = (RangeReadCompletion) completionValue;
        int numResponse = response.getNumResponses();
        StatusCode status = response.getStatus();

        if (LOG.isDebugEnabled()) {
            LOG.debug("Got response for range read request from bookie:" + addr + " for ledger:" +
                    completionValue.ledgerId + " with return code:" + status);
        }

        Integer rcToRet = statusCodeToExceptionCode(status);
        if (null == rcToRet) {
            LOG.error("Range read entry for ledger:" + completionValue.ledgerId + " failed on bookie:" + addr
                    + " with code:" + status);
            rcToRet = BKException.Code.RangeReadException;
        }
        InternalRangeReadResponse rangeReadResponse = new InternalRangeReadResponse();
        rangeReadResponse.numResponses = response.getNumResponses();
        for (ReadResponse protocolReadResponse : response.getResponsesList()) {
            Integer readRc = statusCodeToExceptionCode(protocolReadResponse.getStatus());
            if (null == readRc) {
                readRc = BKException.Code.ReadException;
            }
            ChannelBuffer buffer = ChannelBuffers.buffer(0);

            if (protocolReadResponse.hasBody()) {
                buffer = ChannelBuffers.copiedBuffer(protocolReadResponse
                        .getBody().asReadOnlyByteBuffer());
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Got response for read request from bookie: " + addr + " for ledger: " +
                        protocolReadResponse.getLedgerId() + " entry: " + protocolReadResponse.getEntryId()
                        + " rc: " + readRc + "entry length: " + buffer.readableBytes());
            }

            InternalReadResponse readResponse = new InternalReadResponse(readRc,
                    protocolReadResponse.getLedgerId(),
                    protocolReadResponse.getEntryId(),
                    buffer);
            rangeReadResponse.responses.add(readResponse);
        }
        rrc.cb.rangeReadComplete(rcToRet, rangeReadResponse, rrc.ctx);
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

    static class RangeReadCompletion extends CompletionValue {
        final RangeReadCallback cb;
        public RangeReadCompletion(final PCBookieClientStatsLogger statsLogger, final RangeReadCallback originalCallback,
                              final Object originalCtx, final long ledgerId) {
            super(originalCtx, ledgerId, LedgerHandle.INVALID_ENTRY_ID);
            final long requestTimeMillis = MathUtils.now();
            this.cb = new RangeReadCallback() {
                @Override
                public void rangeReadComplete(int rc, InternalRangeReadResponse response, Object ctx) {
                    long latencyMillis = MathUtils.now() - requestTimeMillis;
                    if (rc != BKException.Code.OK) {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.RANGE_READ_ENTRY).registerFailedEvent(
                                latencyMillis);
                    } else {
                        statsLogger.getOpStatsLogger(PCBookieClientOp.RANGE_READ_ENTRY).registerSuccessfulEvent(
                                latencyMillis);
                    }
                    originalCallback.rangeReadComplete(rc, response, originalCtx);
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
        public final long timeoutAt;

        CompletionKey(long txnId, OperationType operationType) {
            this.txnId = txnId;
            this.operationType = operationType;
            this.timeoutAt = MathUtils.now() + (conf.getReadTimeout()*1000);
        }

        @Override
        public boolean equals(Object obj) {
            if (null == obj || !(obj instanceof CompletionKey)) {
                return false;
            }
            CompletionKey that = (CompletionKey) obj;
            return this.txnId == that.txnId && this.operationType == that.operationType;
        }

        @Override
        public int hashCode() {
            return ((int) txnId);
        }

        public String toString() {
            return String.format("TxnId(%d), OperationType(%s)", txnId, operationType);
        }

        public boolean shouldTimeout() {
            return this.timeoutAt <= MathUtils.now();
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
