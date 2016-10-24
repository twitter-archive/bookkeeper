package org.apache.bookkeeper.proto;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

class ReadEntryProcessorV3 extends PacketProcessorBaseV3 {

    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    protected Stopwatch lastPhaseStartTime;
    private final ExecutorService fenceThreadPool;

    private SettableFuture<Boolean> fenceResult = null;

    protected final ReadRequest readRequest;
    protected final long ledgerId;
    protected final long entryId;

    // Stats
    protected final StatsLogger statsLogger;
    protected final OpStatsLogger readStats;
    protected final OpStatsLogger reqStats;

    public ReadEntryProcessorV3(Request request,
                                Channel channel,
                                Bookie bookie,
                                ExecutorService fenceThreadPool,
                                StatsLogger statsLogger) {
        super(request, channel, bookie, statsLogger);
        this.readRequest = request.getReadRequest();
        this.ledgerId = readRequest.getLedgerId();
        this.entryId = readRequest.getEntryId();
        this.statsLogger = statsLogger;
        if (RequestUtils.isFenceRequest(this.readRequest)) {
            this.readStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_READ);
            this.reqStats = statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_REQUEST);
        } else if (readRequest.hasPreviousLAC()) {
            this.readStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_READ);
            this.reqStats = statsLogger.getOpStatsLogger(READ_ENTRY_LONG_POLL_REQUEST);
        } else {
            this.readStats = statsLogger.getOpStatsLogger(READ_ENTRY);
            this.reqStats = statsLogger.getOpStatsLogger(READ_ENTRY_REQUEST);
        }

        this.fenceThreadPool = fenceThreadPool;
        lastPhaseStartTime = Stopwatch.createStarted();
    }

    protected Long getPreviousLAC() {
        if (readRequest.hasPreviousLAC()) {
            return readRequest.getPreviousLAC();
        } else {
            return null;
        }
    }

    /**
     * Handle read result for fence read.
     *
     * @param entryBody
     *          read result
     * @param readResponseBuilder
     *          read response builder
     * @param entryId
     *          entry id
     * @param startTimeSw
     *          timer for the read request
     */
    protected void handleReadResultForFenceRead(
            final ByteBuffer entryBody,
            final ReadResponse.Builder readResponseBuilder,
            final long entryId,
            final Stopwatch startTimeSw) {
        // reset last phase start time to measure fence result waiting time
        lastPhaseStartTime.reset().start();
        if (null != fenceThreadPool) {
            Futures.addCallback(fenceResult, new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    sendFenceResponse(readResponseBuilder, entryBody, result, startTimeSw);
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("Fence request for ledgerId {} entryId {} encountered exception",
                            new Object[] { ledgerId, entryId, t });
                    sendFenceResponse(readResponseBuilder, entryBody, false, startTimeSw);
                }
            }, fenceThreadPool);
        } else {
            boolean success = false;
            try {
                success = fenceResult.get(1000, TimeUnit.MILLISECONDS);
            } catch (Throwable t) {
                logger.error("Fence request for ledgerId {} entryId {} encountered exception : ",
                        new Object[]{ readRequest.getLedgerId(), readRequest.getEntryId(), t });
            }
            sendFenceResponse(readResponseBuilder, entryBody, success, startTimeSw);
        }
    }

    /**
     * Read a specific entry.
     *
     * @param readResponseBuilder
     *          read response builder.
     * @param entryId
     *          entry to read
     * @param startTimeSw
     *          stop watch to measure the read operation.
     * @return read response or null if it is a fence read operation.
     * @throws IOException
     */
    protected ReadResponse readEntry(ReadResponse.Builder readResponseBuilder,
                                     long entryId,
                                     Stopwatch startTimeSw)
            throws IOException {
        return readEntry(readResponseBuilder, entryId, false, startTimeSw);
    }

    /**
     * Read a specific entry.
     *
     * @param readResponseBuilder
     *          read response builder.
     * @param entryId
     *          entry to read
     * @param startTimeSw
     *          stop watch to measure the read operation.
     * @return read response or null if it is a fence read operation.
     * @throws IOException
     */
    protected ReadResponse readEntry(ReadResponse.Builder readResponseBuilder,
                                     long entryId,
                                     boolean readLACPiggyBack,
                                     Stopwatch startTimeSw)
            throws IOException {
        final ByteBuffer entryBody = bookie.readEntry(ledgerId, entryId);
        if (null != fenceResult) {
            handleReadResultForFenceRead(entryBody, readResponseBuilder, entryId, startTimeSw);
            return null;
        } else {
            readResponseBuilder.setBody(ByteString.copyFrom(entryBody));
            if (readLACPiggyBack) {
                readResponseBuilder.setEntryId(entryId);
            } else {
                long knownLAC = bookie.readLastAddConfirmed(ledgerId);
                readResponseBuilder.setMaxLAC(knownLAC);
            }
            registerSuccessfulEvent(readStats, startTimeSw);
            readResponseBuilder.setStatus(StatusCode.EOK);
            return readResponseBuilder.build();
        }
    }

    protected ReadResponse getReadResponse() {
        final Stopwatch startTimeSw = Stopwatch.createStarted();

        final ReadResponse.Builder readResponse = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);
        try {
            // handle fence reqest
            if (RequestUtils.isFenceRequest(readRequest)) {
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
            return readEntry(readResponse, entryId, startTimeSw);
        } catch (Bookie.NoLedgerException e) {
            if (readRequest.hasFlag() && readRequest.getFlag().equals(ReadRequest.Flag.FENCE_LEDGER)) {
                logger.info("No ledger found reading entry {} when fencing ledger {}", entryId, ledgerId);
            } else {
                logger.info("No ledger found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
            return buildResponse(readResponse, StatusCode.ENOLEDGER, startTimeSw);
        } catch (Bookie.NoEntryException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("No entry found while reading entry: {} from ledger: {}", entryId, ledgerId);
            }
            return buildResponse(readResponse, StatusCode.ENOENTRY, startTimeSw);
        } catch (IOException e) {
            logger.error("IOException while reading entry: {} from ledger: {}", entryId, ledgerId);
            return buildResponse(readResponse, StatusCode.EIO, startTimeSw);
        } catch (BookieException e) {
            logger.error(
                    "Unauthorized access to ledger:{} while reading entry:{} in request from address: {}",
                    new Object[] { ledgerId, entryId, channel.getRemoteAddress() });
            return buildResponse(readResponse, StatusCode.EUA, startTimeSw);
        }
    }

    @Override
    public void safeRun() {
        registerSuccessfulEvent(statsLogger.getOpStatsLogger(READ_ENTRY_SCHEDULING_DELAY), enqueueStopwatch);

        if (!isVersionCompatible()) {
            ReadResponse readResponse = ReadResponse.newBuilder()
                    .setLedgerId(ledgerId)
                    .setEntryId(entryId)
                    .setStatus(StatusCode.EBADVERSION)
                    .build();
            sendResponse(readResponse);
            return;
        }

        executeOp();
    }

    protected void executeOp() {
        ReadResponse readResponse = getReadResponse();
        if (null != readResponse) {
            sendResponse(readResponse);
        }
    }

    private void getFenceResponse(ReadResponse.Builder readResponse, ByteBuffer entryBody, boolean fenceResult) {
        StatusCode status;
        if (!fenceResult) {
            status = StatusCode.EIO;
            registerFailedEvent(statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_WAIT), lastPhaseStartTime);
        } else {
            status = StatusCode.EOK;
            readResponse.setBody(ByteString.copyFrom(entryBody));
            registerSuccessfulEvent(statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_WAIT), lastPhaseStartTime);
        }
        readResponse.setStatus(status);
    }

    private void sendFenceResponse(ReadResponse.Builder readResponse,
                                   ByteBuffer entryBody,
                                   boolean fenceResult,
                                   Stopwatch startTimeSw) {
        // build the fence read response
        getFenceResponse(readResponse, entryBody, fenceResult);
        // register fence read stat
        registerEvent(!fenceResult, statsLogger.getOpStatsLogger(READ_ENTRY_FENCE_READ), startTimeSw);
        // send the fence read response
        sendResponse(readResponse.build());
    }

    protected ReadResponse buildResponse(
            ReadResponse.Builder readResponseBuilder,
            StatusCode statusCode,
            Stopwatch startTimeSw) {
        registerEvent(!statusCode.equals(StatusCode.EOK), readStats, startTimeSw);
        readResponseBuilder.setStatus(statusCode);
        return readResponseBuilder.build();
    }

    protected void sendResponse(ReadResponse readResponse) {
        Response.Builder response = Response.newBuilder()
            .setHeader(getHeader())
            .setStatus(readResponse.getStatus())
            .setReadResponse(readResponse);
        sendResponse(response.getStatus(), reqStats, response.build());
    }

    //
    // Stats Methods
    //

    protected void registerSuccessfulEvent(OpStatsLogger statsLogger, Stopwatch startTime) {
        registerEvent(false, statsLogger, startTime);
    }

    protected void registerFailedEvent(OpStatsLogger statsLogger, Stopwatch startTime) {
        registerEvent(true, statsLogger, startTime);
    }

    protected void registerEvent(boolean failed, OpStatsLogger statsLogger, Stopwatch startTime) {
        if (failed) {
            statsLogger.registerFailedEvent(startTime.elapsed(TimeUnit.MICROSECONDS));
        } else {
            statsLogger.registerSuccessfulEvent(startTime.elapsed(TimeUnit.MICROSECONDS));
        }
    }
}
