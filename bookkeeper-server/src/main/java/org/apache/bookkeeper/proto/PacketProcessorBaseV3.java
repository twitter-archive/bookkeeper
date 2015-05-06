package org.apache.bookkeeper.proto;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.jboss.netty.channel.Channel;

abstract class PacketProcessorBaseV3 extends SafeRunnable {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBaseV3.class);
    final Request request;
    final Channel channel;
    final Bookie  bookie;
    protected Stopwatch enqueueTimeSw;

    PacketProcessorBaseV3(Request request, Channel channel, Bookie bookie) {
        this.request = request;
        this.channel = channel;
        this.bookie = bookie;
        this.enqueueTimeSw = Stopwatch.createStarted();
    }

    protected void sendResponse(StatusCode code, Enum statOp, Object response) {
        channel.write(response);
        if (StatusCode.EOK == code) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(statOp)
                    .registerSuccessfulEvent(enqueueTimeSw.elapsed(TimeUnit.MICROSECONDS));
        } else {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(statOp)
                    .registerFailedEvent(enqueueTimeSw.elapsed(TimeUnit.MICROSECONDS));
        }
    }

    protected boolean isVersionCompatible() {
        // TODO: Change this to include LOWEST_COMPAT
        // For now we just support version 3
        return this.request.getHeader().getVersion().equals(ProtocolVersion.VERSION_THREE);
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

    @Override
    public String toString() {
        return request.toString();
    }
}
