package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.bookkeeper.proto.PacketProcessorBaseV3;
import org.apache.bookkeeper.proto.ReadEntryProcessorV3;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.RangeReadResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.RangeReadRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

public class RangeReadEntryProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(RangeReadEntryProcessorV3.class);

    public RangeReadEntryProcessorV3(Request request, Cnxn srcConn, Bookie bookie) {
        super(request, srcConn, bookie);
    }

    public RangeReadResponse getRangeReadResponse() {
        final long startTimeMillis = MathUtils.now();
        RangeReadRequest rangeReadRequest = request.getRangeReadRequest();
        int numRequests = rangeReadRequest.getNumRequest();

        RangeReadResponse.Builder rangeReadResponse = RangeReadResponse.newBuilder()
                .setNumResponses(0);

        if (!isVersionCompatible()) {
            rangeReadResponse.setStatus(StatusCode.EBADVERSION);
            return rangeReadResponse.build();
        }

        int numResponses = 0;
        StatusCode statusCode = StatusCode.EOK;
        long ledgerid = -1;
        for (ReadRequest readRequest : rangeReadRequest.getRequestsList()) {
            //if (logger.isDebugEnabled()) {
                logger.info("Range read issued request for ledger:" + readRequest.getLedgerId() +
                        " and entry:" + readRequest.getEntryId());
            //}
            ledgerid = readRequest.getLedgerId();
            // Create a modified Request and use it to create a new ReadEntryProcessorV3. We just
            // use this to read each individual entry.
            Request.Builder modifiedRequest = Request.newBuilder()
                    .setHeader(request.getHeader())
                    .setReadRequest(readRequest);
            ReadResponse readResponse = new ReadEntryProcessorV3(modifiedRequest.build(), srcConn, bookie)
                    .getReadResponse();

            // Ignore the error only if the entry was not present. Otherwise, don't issue any more requests
            // and return immediately.
            numResponses++;
            rangeReadResponse.addResponses(readResponse);

            statusCode = readResponse.getStatus();

            if (statusCode.equals(StatusCode.EOK) ||
                statusCode.equals(StatusCode.ENOENTRY)) {
                continue;
            }

            // This is an error so we want to return immediately.
            logger.error("Error while reading ledger:"+readRequest.getLedgerId() + " with" +
                    " status code:" + statusCode);
            break;
        }
        rangeReadResponse.setStatus(statusCode)
                .setNumResponses(numResponses);
        logger.info("Returning response for ledger:" +ledgerid + " nument:" + rangeReadRequest.getNumRequest()
        + " status:" + statusCode);
        return rangeReadResponse.build();
    }

    @Override
    public void run() {
        RangeReadResponse rangeReadResponse = getRangeReadResponse();
        Response.Builder response = Response.newBuilder()
                .setHeader(getHeader())
                .setStatus(rangeReadResponse.getStatus())
                .setRangeReadResponse(rangeReadResponse);
        srcConn.sendResponse(encodeResponse(response.build()));
    }
}
