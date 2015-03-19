/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferFactory;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.InvalidProtocolBufferException;

public class BookieProtoEncoding {
    static final Logger LOG = LoggerFactory.getLogger(BookieProtoEncoding.class);

    static final EnDecoder PREV3 = new EnDecoderPreV3();
    static final EnDecoder V3 = new EnDecoderV3();

    public static interface EnDecoder {
        /**
         * Decode a <i>packet</i> into an object.
         *
         * @param packet
         *          received packet.
         * @return parsed object.
         * @throws Exception
         */
        public Object decode(ChannelBuffer packet) throws Exception;

        /**
         * Encode a <i>object</i> into channel buffer.
         *
         * @param object
         *          object.
         * @return encode buffer.
         * @throws Exception
         */
        public Object encode(Object object, ChannelBufferFactory factory) throws Exception;
    }

    public static class EnDecoderPreV3 implements EnDecoder {

        @Override
        public Object decode(ChannelBuffer packet) throws Exception {
            PacketHeader h = PacketHeader.fromInt(packet.readInt());

            // packet format is different between ADDENTRY and READENTRY
            long ledgerId = -1;
            long entryId = BookieProtocol.INVALID_ENTRY_ID;
            byte[] masterKey = null;
            short flags = h.getFlags();

            ServerStats.getInstance().incrementPacketsReceived();

            switch (h.getOpCode()) {
            case BookieProtocol.ADDENTRY:
                // first read master key
                masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);

                ChannelBuffer bb = packet.duplicate();
                ledgerId = bb.readLong();
                entryId = bb.readLong();

                return new BookieProtocol.AddRequest(h.getVersion(), ledgerId, entryId, flags, masterKey,
                        packet.toByteBuffer().slice());
            case BookieProtocol.READENTRY:
                ledgerId = packet.readLong();
                entryId = packet.readLong();

                if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING
                        && h.getVersion() >= 2) {
                    masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                    packet.readBytes(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
                    return new BookieProtocol.ReadRequest(h.getVersion(), ledgerId, entryId, flags, masterKey);
                } else {
                    return new BookieProtocol.ReadRequest(h.getVersion(), ledgerId, entryId, flags);
                }
            }
            return packet;
        }

        @Override
        public Object encode(Object msg, ChannelBufferFactory bufferFactory) throws Exception {
            BookieProtocol.Response r = (BookieProtocol.Response) msg;
            ChannelBuffer buf = bufferFactory.getBuffer(24);
            buf.writeInt(new PacketHeader(r.getProtocolVersion(), r.getOpCode(), (short) 0).toInt());
            buf.writeInt(r.getErrorCode());
            buf.writeLong(r.getLedgerId());
            buf.writeLong(r.getEntryId());

            ServerStats.getInstance().incrementPacketsSent();

            if (msg instanceof BookieProtocol.ReadResponse) {
                BookieProtocol.ReadResponse rr = (BookieProtocol.ReadResponse) r;
                return ChannelBuffers.wrappedBuffer(buf, ChannelBuffers.wrappedBuffer(rr.getData()));
            } else if ((msg instanceof BookieProtocol.AddResponse)
                    || (msg instanceof BookieProtocol.ErrorResponse)) {
                return buf;
            } else {
                LOG.error("Cannot encode unknown response type {}", msg.getClass().getName());
                return msg;
            }
        }

    }

    public static class EnDecoderV3 implements EnDecoder {

        @Override
        public Object decode(ChannelBuffer packet) throws Exception {
            return Request.parseFrom(new ChannelBufferInputStream(packet));
        }

        @Override
        public Object encode(Object msg, ChannelBufferFactory factory) throws Exception {
            Response response = (Response) msg;
            return ChannelBuffers.wrappedBuffer(response.toByteArray());
        }

    }

    public static class Decoder extends OneToOneDecoder {
        @Override
        public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received request {} from channel {} to decode.", msg, channel);
            }
            if (!(msg instanceof ChannelBuffer)) {
                return msg;
            }
            ChannelBuffer buffer = (ChannelBuffer) msg;
            try {
                buffer.markReaderIndex();
                try {
                    return V3.decode(buffer);
                } catch (InvalidProtocolBufferException e) {
                    buffer.resetReaderIndex();
                    return PREV3.decode(buffer);
                }
            } catch (Exception e) {
                LOG.error("Failed to decode a message : ", e);
                throw e;
            }
        }
    }

    public static class Encoder extends OneToOneEncoder {
        @Override
        public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg)
                throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Encode response {} to channel {}.", msg, channel);
            }
            if (msg instanceof Response) {
                return V3.encode(msg, ctx.getChannel().getConfig().getBufferFactory());
            } else if (msg instanceof BookieProtocol.Response) {
                return PREV3.encode(msg, ctx.getChannel().getConfig().getBufferFactory());
            } else {
                LOG.warn("Invalid response to encode : {}", msg.getClass().getName());
                return msg;
            }
        }
    }

}
