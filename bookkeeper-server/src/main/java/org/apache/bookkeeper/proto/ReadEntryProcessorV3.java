package org.apache.bookkeeper.proto;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.ExecutorService;

class ReadEntryProcessorV3 extends PacketProcessorBaseV3 implements Observer {
    private long lastPhaseStartTimeNanos;
    private final HashedWheelTimer requestTimer;
    private final ExecutorService fenceThreadPool;
    private final ExecutorService longPollThreadPool;
    private Long previousLAC = null;
    private Timeout expirationTimerTask = null;
    private Future<?> deferredTask = null;
    private SettableFuture<Boolean> fenceResult = null;
    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    public ReadEntryProcessorV3(Request request,
                                Cnxn srcConn,
                                Bookie bookie,
                                ExecutorService fenceThreadPool,
                                ExecutorService longPollThreadPool,
                                HashedWheelTimer requestTimer) {
        super(request, srcConn, bookie);
        this.fenceThreadPool = fenceThreadPool;
        this.longPollThreadPool = longPollThreadPool;
        this.requestTimer = requestTimer;
        lastPhaseStartTimeNanos = enqueueNanos;
    }

    private ReadResponse getReadResponse() {
        final long startTimeNanos = MathUtils.nowInNano();
        final ReadRequest readRequest = request.getReadRequest();
        long ledgerId = readRequest.getLedgerId();
        long entryId = readRequest.getEntryId();

        final ReadResponse.Builder readResponse = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            readResponse.setStatus(StatusCode.EBADVERSION);
            return readResponse.build();
        }

        StatusCode status = StatusCode.EBADREQ;
        final ByteBuffer entryBody;
        boolean readLACPiggyBack = false;
        try {
            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER)) {
                logger.warn("Ledger fence request received for ledger:" + ledgerId + " from address:" + srcConn.getPeerName());
                // TODO: Move this to a different request which definitely has the master key.
                if (!readRequest.hasMasterKey()) {
                    logger.error("Fence ledger request received without master key for ledger:" + ledgerId +
                            " from address:" + srcConn.getRemoteAddress());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                } else {
                    byte[] masterKey = readRequest.getMasterKey().toByteArray();
                    fenceResult = bookie.fenceLedger(ledgerId, masterKey);
                }
            }
            if ((null == previousLAC) && (null == fenceResult) && (readRequest.hasPreviousLAC())) {
                previousLAC = readRequest.getPreviousLAC();
                logger.trace("Waiting For LAC Update {}", previousLAC);
                final Observable observable = bookie.waitForLastAddConfirmedUpdate(ledgerId,
                    readRequest.getPreviousLAC(), this);

                if (null != observable) {
                    ServerStatsProvider
                        .getStatsLoggerInstance()
                        .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_LONG_POLL_PRE_WAIT)
                        .registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));

                    lastPhaseStartTimeNanos = MathUtils.nowInNano();

                    if (readRequest.hasTimeOut()) {
                        logger.trace("Waiting For LAC Update {}: Timeout {}", previousLAC, readRequest.getTimeOut());
                        synchronized (this) {
                            expirationTimerTask = requestTimer.newTimeout(new TimerTask() {
                                @Override
                                public void run(Timeout timeout) throws Exception {
                                    // When the timeout expires just get whatever is the current
                                    // readLastConfirmed
                                    ReadEntryProcessorV3.this.scheduleDeferredRead(observable, true);
                                }
                            }, readRequest.getTimeOut(), TimeUnit.MILLISECONDS);
                        }
                    }
                    return null;
                }
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("previousLAC: {} fenceResult: {} hasPreviousLAC: {}",
                        new Object[]{previousLAC, fenceResult, readRequest.hasPreviousLAC()});
                }
            }

            boolean shouldReadEntry = true;

            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.ENTRY_PIGGYBACK)) {
                if(!readRequest.hasPreviousLAC() || (BookieProtocol.LAST_ADD_CONFIRMED != entryId)) {
                    // This is not a valid request - client bug?
                    logger.error("Incorrect read request, entry piggyback requested incorrectly for ledgerId {} entryId {}", ledgerId, entryId);
                    status = StatusCode.EBADREQ;
                    shouldReadEntry = false;
                } else {
                    long knownLAC = bookie.readLastAddConfirmed(ledgerId);
                    readResponse.setMaxLAC(knownLAC);
                    if (knownLAC > previousLAC) {
                        readLACPiggyBack = true;
                        entryId = previousLAC + 1;
                        readResponse.setMaxLAC(knownLAC);
                        if (logger.isDebugEnabled()) {
                            logger.debug("ReadLAC Piggy Back reading entry:{} from ledger: {}", entryId, ledgerId);
                        }
                    } else {
                        status = StatusCode.EOK;
                        shouldReadEntry = false;
                    }
                }
            }

            if (shouldReadEntry) {
                entryBody = bookie.readEntry(ledgerId, entryId);
                if (null != fenceResult) {
                    if (null != fenceThreadPool) {
                        Futures.addCallback(fenceResult, new FutureCallback<Boolean>() {
                            @Override
                            public void onSuccess(Boolean result) {
                                sendFenceResponse(readResponse, entryBody, result);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                logger.error("Fence request for ledgerId "+ readRequest.getLedgerId() + " entryId " + readRequest.getEntryId() + " encountered exception", t);
                                sendFenceResponse(readResponse, entryBody, false);
                            }
                        }, fenceThreadPool);

                        lastPhaseStartTimeNanos = MathUtils.nowInNano();

                        ServerStatsProvider
                            .getStatsLoggerInstance()
                            .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_FENCE_READ)
                            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));

                        return null;
                    } else {
                        try {
                            getFenceResponse(readResponse, entryBody,
                                             fenceResult.get(1000, TimeUnit.MILLISECONDS));
                            status = StatusCode.EOK;
                        } catch (Throwable t) {
                            logger.error("Fence request for ledgerId {} entryId {} encountered exception : ",
                                         new Object[] { readRequest.getLedgerId(), readRequest.getEntryId(), t });
                            getFenceResponse(readResponse, entryBody, false);
                            status = StatusCode.EIO;
                        }
                    }
                } else {
                    readResponse.setBody(ByteString.copyFrom(entryBody));
                    if (readLACPiggyBack) {
                        readResponse.setEntryId(entryId);
                    } else {
                        long knownLAC = bookie.readLastAddConfirmed(ledgerId);
                        readResponse.setMaxLAC(knownLAC);
                    }
                    status = StatusCode.EOK;
                }
            }
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            logger.error("No ledger found while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (Bookie.NoEntryException e) {
            // piggy back is best effort and this request can fail genuinely because of striping
            // entries across the ensemble
            if (readLACPiggyBack) {
                ServerStatsProvider.getStatsLoggerInstance().getCounter(
                    BookkeeperServerStatsLogger.BookkeeperServerCounter.READ_LAST_ENTRY_NOENTRY_ERROR)
                    .inc();
                status = StatusCode.EOK;
            } else {
                status = StatusCode.ENOENTRY;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("No entry found while reading entry:" + entryId + " from ledger:" +
                        ledgerId);
            }
        } catch (IOException e) {
            status = StatusCode.EIO;
            logger.error("IOException while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId + " while reading entry:" + entryId + " in request " +
                    "from address:" + srcConn.getPeerName());
            status = StatusCode.EUA;
        }

        Enum op = BookkeeperServerOp.READ_ENTRY;
        if (null != fenceResult) {
            op = BookkeeperServerOp.READ_ENTRY_FENCE_READ;
        } else if (readRequest.hasPreviousLAC()) {
            op = BookkeeperServerOp.READ_ENTRY_LONG_POLL_READ;
        }
        if (status.equals(StatusCode.EOK)) {
            ServerStatsProvider
                .getStatsLoggerInstance()
                .getOpStatsLogger(op)
                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        } else {
            ServerStatsProvider
                .getStatsLoggerInstance()
                .getOpStatsLogger(op)
                .registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        }

        // Finally set status and return. The body would have been updated if
        // a read went through.
        readResponse.setStatus(status);
        return readResponse.build();

    }

    @Override
    public void safeRun() {
        ServerStatsProvider
            .getStatsLoggerInstance()
            .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_SCHEDULING_DELAY)
            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(lastPhaseStartTimeNanos));

        ReadResponse readResponse = getReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    private void getFenceResponse(ReadResponse.Builder readResponse, ByteBuffer entryBody, boolean fenceResult) {
        StatusCode status;
        if (!fenceResult) {
            status = StatusCode.EIO;
            ServerStatsProvider
                .getStatsLoggerInstance()
                .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_FENCE_WAIT)
                .registerFailedEvent(MathUtils.elapsedMicroSec(lastPhaseStartTimeNanos));
        } else {
            status = StatusCode.EOK;
            readResponse.setBody(ByteString.copyFrom(entryBody));
            ServerStatsProvider
                .getStatsLoggerInstance()
                .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_FENCE_WAIT)
                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(lastPhaseStartTimeNanos));
        }

        readResponse.setStatus(status);
    }

    private void sendFenceResponse(ReadResponse.Builder readResponse, ByteBuffer entryBody, boolean fenceResult) {
        getFenceResponse(readResponse, entryBody, fenceResult);
        sendResponse(readResponse.build());
    }

    private void sendResponse(ReadResponse readResponse) {
        Response.Builder response = Response.newBuilder()
            .setHeader(getHeader())
            .setStatus(readResponse.getStatus())
            .setReadResponse(readResponse);
        Enum op = BookkeeperServerOp.READ_ENTRY_REQUEST;
        if (null != previousLAC) {
            op = BookkeeperServerOp.READ_ENTRY_LONG_POLL_REQUEST;
        } else if (null != fenceResult) {
            op = BookkeeperServerOp.READ_ENTRY_FENCE_REQUEST;
        }
        sendResponse(response.getStatus(), op, encodeResponse(response.build()));
    }

    private synchronized void scheduleDeferredRead(Observable observable, boolean timeout) {
        if (null == deferredTask) {
            if (logger.isTraceEnabled()) {
                logger.trace("Deferred Task, expired: {}, request: {}", timeout, request);
            }
            observable.deleteObserver(this);
            try {
                deferredTask = longPollThreadPool.submit(this);
            } catch (RejectedExecutionException exc) {
                // If the threadPool has been shutdown, simply drop the task
            }
            if (null != expirationTimerTask) {
                expirationTimerTask.cancel();
            }

            if (timeout) {
                ServerStatsProvider
                    .getStatsLoggerInstance()
                    .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_LONG_POLL_WAIT)
                    .registerFailedEvent(MathUtils.elapsedMicroSec(lastPhaseStartTimeNanos));
            } else {
                ServerStatsProvider
                    .getStatsLoggerInstance()
                    .getOpStatsLogger(BookkeeperServerOp.READ_ENTRY_LONG_POLL_WAIT)
                    .registerSuccessfulEvent(MathUtils.elapsedMicroSec(lastPhaseStartTimeNanos));
            }
            lastPhaseStartTimeNanos = MathUtils.nowInNano();
        }
    }

    @Override
    public void update(Observable observable, Object o) {
        Long newLAC = (Long)o;
        if (newLAC > previousLAC) {
            if (logger.isTraceEnabled()) {
                logger.trace("Last Add Confirmed Advanced to {} for request {}", newLAC, request);
            }
            scheduleDeferredRead(observable, false);
        }
    }
}
