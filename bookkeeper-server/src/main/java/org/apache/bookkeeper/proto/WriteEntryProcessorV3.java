package org.apache.bookkeeper.proto;

import com.google.protobuf.ByteString;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.PacketProcessorBaseV3;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.AddResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.OperationType;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class WriteEntryProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessorV3.class);

    public WriteEntryProcessorV3(Request request, Cnxn srcConn, Bookie bookie) {
        super(request, srcConn, bookie);
    }

    // Returns null if there is no exception thrown
    private AddResponse getAddResponse() {
        // TODO: Move all these common operations into a function in the base class.
        final long startTimeMillis = MathUtils.now();
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
            logger.warn("BookieServer is running as readonly mode,"
                    + " so rejecting the request from the client!");
            addResponse.setStatus(StatusCode.EREADONLY);
            return addResponse.build();
        }

        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      InetSocketAddress addr, Object ctx) {
                long latencyMillis = MathUtils.now() - startTimeMillis;
                if (rc == BookieProtocol.EOK) {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerSuccessfulEvent(latencyMillis);
                } else {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerFailedEvent(latencyMillis);
                }
                Cnxn conn = (Cnxn) ctx;
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
                conn.sendResponse(encodeResponse(response.build()));
            }
        };
        StatusCode status = null;
        byte[] masterKey = addRequest.getMasterKey().toByteArray();
        ByteBuffer entryToAdd = addRequest.getBody().asReadOnlyByteBuffer();
        try {
            if (addRequest.hasFlag() && addRequest.getFlag().equals(AddRequest.Flag.RECOVERY_ADD)) {
                bookie.recoveryAddEntry(entryToAdd, wcb, srcConn, masterKey);
            } else {
                bookie.addEntry(entryToAdd, wcb, srcConn, masterKey);
            }
            status = StatusCode.EOK;
        } catch (IOException e) {
            logger.error("Error writing entry:" + entryId + " to ledger:" + ledgerId, e);
            status = StatusCode.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.error("Ledger fenced while writing entry:" + entryId +
                    " to ledger:" + ledgerId);
            status = StatusCode.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId +
                    " while writing entry:" + entryId);
            status = StatusCode.EUA;
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
    public void run() {
        AddResponse addResponse = getAddResponse();
        if (null != addResponse) {
            // This means there was an error and we should send this back.
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(addResponse.getStatus())
                    .setAddResponse(addResponse);
            srcConn.sendResponse(encodeResponse(response.build()));
        }
    }
}
