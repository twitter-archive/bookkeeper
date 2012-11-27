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

import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ReadEntryProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessorV3.class);

    public ReadEntryProcessorV3(Request request, Cnxn srcConn, Bookie bookie) {
        super(request, srcConn, bookie);
    }

    private ReadResponse getReadResponse() {
        final long startTimeMillis = MathUtils.now();
        ReadRequest readRequest = request.getReadRequest();
        long ledgerId = readRequest.getLedgerId();
        long entryId = readRequest.getEntryId();

        ReadResponse.Builder readResponse = ReadResponse.newBuilder()
                .setLedgerId(ledgerId)
                .setEntryId(entryId);

        if (!isVersionCompatible()) {
            readResponse.setStatus(StatusCode.EBADVERSION);
            return readResponse.build();
        }

        StatusCode status;
        ByteBuffer entryBody = null;
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
                    bookie.fenceLedger(ledgerId, masterKey);
                }
            }
            entryBody = bookie.readEntry(ledgerId, entryId);
            readResponse.setBody(ByteString.copyFrom(entryBody));
            status = StatusCode.EOK;
        } catch (Bookie.NoLedgerException e) {
            status = StatusCode.ENOLEDGER;
            logger.error("No ledger found while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (Bookie.NoEntryException e) {
            status = StatusCode.ENOENTRY;
            logger.error("No entry found while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (IOException e) {
            status = StatusCode.EIO;
            logger.error("IOException while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId + " while reading entry:" + entryId + " in request " +
                    "from address:" + srcConn.getPeerName());
            status = StatusCode.EUA;
        }

        long latencyMillis = MathUtils.now() - startTimeMillis;
        if (status.equals(StatusCode.EOK)) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerSuccessfulEvent(latencyMillis);
        } else {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerFailedEvent(latencyMillis);
        }

        // Finally set status and return. The body would have been updated if
        // a read went through.
        readResponse.setStatus(status);
        return readResponse.build();

    }

    @Override
    public void run() {
        ReadResponse readResponse = getReadResponse();
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(readResponse.getStatus())
                .setReadResponse(readResponse);
        srcConn.sendResponse(encodeResponse(response.build()));
    }
}
