package org.apache.bookkeeper.proto;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.SafeRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.BookkeeperProtocol.BKPacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ProtocolVersion;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

abstract class PacketProcessorBaseV3 extends SafeRunnable {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBaseV3.class);
    final Request request;
    final Channel channel;
    final Bookie  bookie;
    final OpStatsLogger channelWriteOpStatsLogger;
    protected final Stopwatch enqueueStopwatch;
    protected final StatsLogger statsLogger;

    PacketProcessorBaseV3(Request request,
                          Channel channel,
                          Bookie bookie,
                          StatsLogger statsLogger) {
        this.request = request;
        this.channel = channel;
        this.bookie = bookie;
        this.statsLogger = statsLogger;
        this.channelWriteOpStatsLogger = statsLogger.getOpStatsLogger(CHANNEL_WRITE);
        this.enqueueStopwatch = Stopwatch.createStarted();
    }

    protected void sendResponse(final StatusCode code, final OpStatsLogger statsLogger, Object response) {
        final Stopwatch writeStopwatch = Stopwatch.createStarted();
        channel.write(response).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {

                long writeMicros = writeStopwatch.elapsed(TimeUnit.MICROSECONDS);
                if (!channelFuture.isSuccess()) {
                    channelWriteOpStatsLogger.registerFailedEvent(writeMicros);
                } else {
                    channelWriteOpStatsLogger.registerSuccessfulEvent(writeMicros);
                }

                long requestMicros = enqueueStopwatch.elapsed(TimeUnit.MICROSECONDS);
                if (StatusCode.EOK == code) {
                    statsLogger.registerSuccessfulEvent(requestMicros);
                } else {
                    statsLogger.registerFailedEvent(requestMicros);
                }
            }
        });
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
