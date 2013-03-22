package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;

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

import java.nio.ByteBuffer;

public abstract class PacketProcessorBaseV3 {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBaseV3.class);
    final Request request;
    final Cnxn srcConn;
    final Bookie  bookie;

    public PacketProcessorBaseV3(Request request, Cnxn srcConn, Bookie bookie) {
        this.request = request;
        this.srcConn = srcConn;
        this.bookie = bookie;
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
