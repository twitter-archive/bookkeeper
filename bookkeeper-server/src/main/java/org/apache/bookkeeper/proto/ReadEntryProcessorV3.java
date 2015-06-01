package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.Observable;
import java.util.Observer;

import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.bookie.LastAddConfirmedUpdateNotification;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryProcessorV3 extends PacketProcessorBaseV3 implements Observer {
    private Stopwatch lastPhaseStartTime;
    private final HashedWheelTimer requestTimer;
    private final ExecutorService fenceThreadPool;
    private final ExecutorService longPollThreadPool;
    private Long previousLAC = null;
    private Timeout expirationTimerTask = null;
    private Future<?> deferredTask = null;
    private SettableFuture<Boolean> fenceResult = null;
    private Optional<Long> lastAddConfirmedUpdateTime = Optional.<Long>absent();
    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    public ReadEntryProcessorV3(Request request,
                                Channel channel,
                                Bookie bookie,
                                ExecutorService fenceThreadPool,
                                ExecutorService longPollThreadPool,
                                HashedWheelTimer requestTimer) {
        super(request, channel, bookie);
        this.fenceThreadPool = fenceThreadPool;
        this.longPollThreadPool = longPollThreadPool;
        this.requestTimer = requestTimer;
        lastPhaseStartTime = Stopwatch.createStarted();
    }

    boolean isFenceRequest() {
        final ReadRequest readRequest = request.getReadRequest();
        return readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER);
    }

    boolean isLongPollRequest() {
        final ReadRequest readRequest = request.getReadRequest();
        return !isFenceRequest() && readRequest.hasPreviousLAC();
    }

    private ReadResponse getReadResponse() {
        final Stopwatch startTimeSw = Stopwatch.createStarted();
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
            // handle fence reqest
            if (isFenceRequest()) {
                logger.info("Ledger fence request received for ledger: {} from address: {}", ledgerId,
                        channel.getRemoteAddress());
                if (!readRequest.hasMasterKey()) {
                    logger.error(
                            "Fence ledger request received without master key for ledger:{} from address: {}",
                            ledgerId, channel.getRemoteAddress());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                } else {
                    byte[] masterKey = readRequest.getMasterKey().toByteArray();
                    fenceResult = bookie.fenceLedger(ledgerId, masterKey);
                }
            }
            // handle long poll request
            if ((null == previousLAC) && (null == fenceResult) && (readRequest.hasPreviousLAC())) {
                previousLAC = readRequest.getPreviousLAC();
                logger.trace("Waiting For LAC Update {}", previousLAC);
                final Observable observable = bookie.waitForLastAddConfirmedUpdate(ledgerId,
                    readRequest.getPreviousLAC(), this);

                if (null != observable) {
                    registerSuccessfulEvent(BookkeeperServerOp.READ_ENTRY_LONG_POLL_PRE_WAIT, startTimeSw);
                    lastPhaseStartTime.reset().start();

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
                logger.trace("previousLAC: {} fenceResult: {} hasPreviousLAC: {}",
                    new Object[]{ previousLAC, fenceResult, readRequest.hasPreviousLAC() });
            }

            boolean shouldReadEntry = true;
            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.ENTRY_PIGGYBACK)) {
                if(!readRequest.hasPreviousLAC() || (BookieProtocol.LAST_ADD_CONFIRMED != entryId)) {
                    // This is not a valid request - client bug?
                    logger.error("Incorrect read request, entry piggyback requested incorrectly for ledgerId {} entryId {}",
                            ledgerId, entryId);
                    status = StatusCode.EBADREQ;
                    shouldReadEntry = false;
                } else {
                    long knownLAC = bookie.readLastAddConfirmed(ledgerId);
                    readResponse.setMaxLAC(knownLAC);
                    if (knownLAC > previousLAC) {
                        readLACPiggyBack = true;
                        entryId = previousLAC + 1;
                        readResponse.setMaxLAC(knownLAC);
                        if (lastAddConfirmedUpdateTime.isPresent()) {
                            readResponse.setLacUpdateTimestamp(lastAddConfirmedUpdateTime.get());
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("ReadLAC Piggy Back reading entry:{} from ledger: {}", entryId, ledgerId);
                        }
                    } else {
                        if (knownLAC < previousLAC) {
                            logger.debug("Found smaller lac when piggy back reading lac and entry from ledger {} :" +
                                    " previous lac = {}, known lac = {}", new Object[] { ledgerId, previousLAC, knownLAC });
                        }
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
                                logger.error("Fence request for ledgerId {} entryId {} encountered exception",
                                        new Object[] { readRequest.getLedgerId(), readRequest.getEntryId(), t });
                                sendFenceResponse(readResponse, entryBody, false);
                            }
                        }, fenceThreadPool);

                        lastPhaseStartTime.reset().start();
                        registerSuccessfulEvent(BookkeeperServerOp.READ_ENTRY_FENCE_READ, startTimeSw);

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
            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER)) {
                logger.info("No ledger found reading entry {} when fencing ledger {}", entryId, ledgerId);
            } else {
                logger.info("No ledger found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
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

            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.ENTRY_PIGGYBACK)) {
                logger.info("No entry found while piggyback reading entry {} from ledger {} : previous lac = {}",
                        new Object[] { entryId, ledgerId, previousLAC });
            } else if (logger.isDebugEnabled()) {
                logger.debug("No entry found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
        } catch (IOException e) {
            status = StatusCode.EIO;
            logger.error("IOException while reading entry: {} from ledger: {}", entryId, ledgerId);
        } catch (BookieException e) {
            logger.error(
                    "Unauthorized access to ledger:{} while reading entry:{} in request from address: {}",
                    new Object[] { ledgerId, entryId, channel.getRemoteAddress() });
            status = StatusCode.EUA;
        }

        Enum op = BookkeeperServerOp.READ_ENTRY;
        if (null != fenceResult) {
            op = BookkeeperServerOp.READ_ENTRY_FENCE_READ;
        } else if (readRequest.hasPreviousLAC()) {
            op = BookkeeperServerOp.READ_ENTRY_LONG_POLL_READ;
        }
        registerEvent(!status.equals(StatusCode.EOK), op, startTimeSw);

        // Finally set status and return. The body would have been updated if
        // a read went through.
        readResponse.setStatus(status);
        return readResponse.build();

    }

    @Override
    public void safeRun() {
        registerSuccessfulEvent(BookkeeperServerOp.READ_ENTRY_SCHEDULING_DELAY, enqueueStopwatch);

        ReadResponse readResponse = getReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    private void registerSuccessfulEvent(Enum op, Stopwatch startTime) {
        registerEvent(false, op, startTime);
    }

    private void registerFailedEvent(Enum op, Stopwatch startTime) {
        registerEvent(true, op, startTime);
    }

    private void registerEvent(boolean failed, Enum op, Stopwatch startTime) {
        final OpStatsLogger statsLogger = ServerStatsProvider
            .getStatsLoggerInstance()
            .getOpStatsLogger(op);

        if (failed) {
            statsLogger.registerFailedEvent(startTime.elapsed(TimeUnit.MICROSECONDS));
        } else {
            statsLogger.registerSuccessfulEvent(startTime.elapsed(TimeUnit.MICROSECONDS));

        }
    }


    private void getFenceResponse(ReadResponse.Builder readResponse, ByteBuffer entryBody, boolean fenceResult) {
        StatusCode status;
        if (!fenceResult) {
            status = StatusCode.EIO;
            registerFailedEvent(BookkeeperServerOp.READ_ENTRY_FENCE_WAIT, lastPhaseStartTime);
        } else {
            status = StatusCode.EOK;
            readResponse.setBody(ByteString.copyFrom(entryBody));
            registerSuccessfulEvent(BookkeeperServerOp.READ_ENTRY_FENCE_WAIT, lastPhaseStartTime);
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
        sendResponse(response.getStatus(), op, response.build());
    }

    private synchronized void scheduleDeferredRead(Observable observable, boolean timeout) {
        if (null == deferredTask) {
            logger.trace("Deferred Task, expired: {}, request: {}", timeout, request);
            observable.deleteObserver(this);
            try {
                deferredTask = longPollThreadPool.submit(this);
            } catch (RejectedExecutionException exc) {
                // If the threadPool has been shutdown, simply drop the task
            }
            if (null != expirationTimerTask) {
                expirationTimerTask.cancel();
            }

            registerEvent(timeout, BookkeeperServerOp.READ_ENTRY_LONG_POLL_WAIT, lastPhaseStartTime);
            lastPhaseStartTime.reset().start();
        }
    }

    @Override
    public void update(Observable observable, Object o) {

        LastAddConfirmedUpdateNotification newLACNotification = (LastAddConfirmedUpdateNotification)o;
        if (newLACNotification.lastAddConfirmed > previousLAC) {
            if (newLACNotification.lastAddConfirmed != Long.MAX_VALUE &&
                !lastAddConfirmedUpdateTime.isPresent()) {
                lastAddConfirmedUpdateTime = Optional.of(newLACNotification.timestamp);
            }
            logger.trace("Last Add Confirmed Advanced to {} for request {}", newLACNotification.lastAddConfirmed, request);
            scheduleDeferredRead(observable, false);
        }
    }
}
