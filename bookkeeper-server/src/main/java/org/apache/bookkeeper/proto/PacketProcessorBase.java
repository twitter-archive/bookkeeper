/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.proto;

import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PacketProcessorBase {
    private final static Logger logger = LoggerFactory.getLogger(PacketProcessorBase.class);
    final ByteBuffer packet;
    final Cnxn srcConn;
    final Bookie bookie;

    // The following members should be populated by a child class before calling
    // buildResponse(int rc)
    protected PacketHeader header;
    protected long ledgerId;
    protected long entryId;

    public PacketProcessorBase(ByteBuffer packet, Cnxn srcConn, Bookie bookie) {
        this.packet = packet;
        this.srcConn = srcConn;
        this.bookie = bookie;
    }

    // Builds a response packet without the actual entry.
    public static ByteBuffer buildResponse(int rc, byte version, byte opCode, long ledgerId, long entryId) {
        ByteBuffer response = ByteBuffer.allocate(24);
        response.putInt(new BookieProtocol.PacketHeader(version, opCode, (short)0).toInt());
        response.putInt(rc);
        response.putLong(ledgerId);
        response.putLong(entryId);
        response.flip();
        return response;
    }

    /**
     * This is a helper function to build a response for this request. Ensure
     * that the various values are populated before calling this function.
     * @param rc
     * @return
     */
    public ByteBuffer buildResponse(int rc) {
        return buildResponse(rc, header.getVersion(), header.getOpCode(), ledgerId, entryId);
    }

    public boolean isVersionCompatible(PacketHeader header) {
        byte version = header.getVersion();
        if (version < BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION
                || version > BookieProtocol.CURRENT_PROTOCOL_VERSION) {
            logger.error("Invalid protocol version. Expected something between " +
                    BookieProtocol.LOWEST_COMPAT_PROTOCOL_VERSION + " and " +
                    BookieProtocol.CURRENT_PROTOCOL_VERSION + ". Got " + version + ".");
            return false;
        }
        return true;
    }
}
