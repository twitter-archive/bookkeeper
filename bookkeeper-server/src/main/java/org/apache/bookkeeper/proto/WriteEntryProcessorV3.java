package org.apache.bookkeeper.proto;

import com.google.common.base.Optional;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.DigestManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

class WriteEntryProcessorV3 extends PacketProcessorBaseV3 {
    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessorV3.class);

    final Optional<DigestManager> macManager;

    public WriteEntryProcessorV3(Request request, Channel channel, Bookie bookie, StatsLogger statsLogger, Optional<DigestManager> macManager) {
        super(request, channel, bookie, statsLogger);
        this.macManager = macManager;
    }

    // Returns null if there is no exception thrown
    private AddResponse getAddResponse() {
        // TODO: Move all these common operations into a function in the base class.
        final long startTimeNanos = MathUtils.nowInNano();
        AddRequest addRequest = request.getAddRequest();
        long ledgerId = addRequest.getLedgerId();
        long entryId = addRequest.getEntryId();

        final AddResponse.Builder addResponse = AddResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            addResponse.setStatus(StatusCode.EBADVERSION);
            return addResponse.build();
        }

        if (bookie.isReadOnly()) {
            logger.debug("BookieServer is running as readonly mode,"
                    + " so rejecting the request from the client!");
            addResponse.setStatus(StatusCode.EREADONLY);
            return addResponse.build();
        }

        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      BookieSocketAddress addr, Object ctx) {
                if (rc == BookieProtocol.EOK) {
                    statsLogger.getOpStatsLogger(ADD_ENTRY)
                            .registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                } else {
                    statsLogger.getOpStatsLogger(ADD_ENTRY)
                            .registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                }
                // rc can only be EOK in the current implementation. Could be EIO in future?
                // TODO: Map for all return codes.
                StatusCode status;
                switch (rc) {
                    case BookieProtocol.EOK:
                        status = StatusCode.EOK;
                        break;
                    case BookieProtocol.EIO:
                        status = StatusCode.EIO;
                        break;
                    default:
                        status = StatusCode.EUA;
                        break;
                }
                addResponse.setStatus(status);
                Response.Builder response = Response.newBuilder()
                        .setHeader(getHeader())
                        .setStatus(addResponse.getStatus())
                        .setAddResponse(addResponse);
                sendResponse(status, statsLogger.getOpStatsLogger(ADD_ENTRY_REQUEST),
                             response.build());
            }
        };
        StatusCode status = null;
        byte[] masterKey = addRequest.getMasterKey().toByteArray();
        ByteBuffer entryToAdd = addRequest.getBody().asReadOnlyByteBuffer();
        try {
            if (macManager.isPresent()) {
                macManager.get().verifyDigest(ledgerId, entryId, ChannelBuffers.wrappedBuffer(entryToAdd));
            }
            if (addRequest.hasFlag() && addRequest.getFlag().equals(AddRequest.Flag.RECOVERY_ADD)) {
                bookie.recoveryAddEntry(entryToAdd, wcb, channel, masterKey);
            } else {
                bookie.addEntry(entryToAdd, wcb, channel, masterKey);
            }
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error writing entry:" + entryId + " to ledger:" + ledgerId, e);
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Ledger fenced while writing entry:" + entryId +
                        " to ledger:" + ledgerId);
            }
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId +
                    " while writing entry:" + entryId);
            status = StatusCode.EUA;
        } catch (BKDigestMatchException e) {
            statsLogger.getCounter(ADD_ENTRY_DIGEST_FAILURE).inc();
            status = StatusCode.EBADREQ;
        } catch (Throwable t) {
            logger.error("Unexpected exception while writing {}@{} : ",
                    new Object[] { entryId, ledgerId, t });
            // some bad request which cause unexpected exception
            status = StatusCode.EBADREQ;
        }

        // If everything is okay, we return null so that the calling function
        // doesn't return a response back to the caller.
        if (!status.equals(StatusCode.EOK)) {
            addResponse.setStatus(status);
            return addResponse.build();
        }
        return null;
    }

    @Override
    public void safeRun() {
        AddResponse addResponse = getAddResponse();
        if (null != addResponse) {
            // This means there was an error and we should send this back.
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(addResponse.getStatus())
                    .setAddResponse(addResponse);
            sendResponse(addResponse.getStatus(), statsLogger.getOpStatsLogger(ADD_ENTRY_REQUEST),
                         response.build());
        }
    }
}
