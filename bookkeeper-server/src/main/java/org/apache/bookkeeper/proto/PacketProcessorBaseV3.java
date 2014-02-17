package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

import java.nio.ByteBuffer;

abstract class PacketProcessorBaseV3 {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBaseV3.class);
    final Request request;
    final Cnxn srcConn;
    final Bookie  bookie;
    protected long enqueueNanos;

    PacketProcessorBaseV3(Request request, Cnxn srcConn, Bookie bookie) {
        this.request = request;
        this.srcConn = srcConn;
        this.bookie = bookie;
        this.enqueueNanos = MathUtils.nowInNano();
    }

    protected void sendResponse(StatusCode code, Enum statOp, ByteBuffer...response) {
        srcConn.sendResponse(response);
        if (StatusCode.EIO == code) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(statOp)
                    .registerSuccessfulEvent(MathUtils.elapsedMicroSec(enqueueNanos));
        } else {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(statOp)
                    .registerFailedEvent(MathUtils.elapsedMicroSec(enqueueNanos));
        }
    }

    protected boolean isVersionCompatible() {
        // TODO: Change this to include LOWEST_COMPAT
        // For now we just support version 3
        return this.request.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
    }

    protected ByteBuffer encodeResponse(Response response) {
        return ByteBuffer.wrap(response.toByteArray());
    }

    /**
     * Build a header with protocol version 3 and the operation type same as what was in the
     * request.
     * @return
     */
    protected BKPacketHeader getHeader() {
        BKPacketHeader.Builder header = BKPacketHeader.newBuilder();
        header.setVersion(ProtocolVersion.VERSION_THREE);
        header.setOperation(request.getHeader().getOperation());
        header.setTxnId(request.getHeader().getTxnId());
        return header.build();
    }
}
