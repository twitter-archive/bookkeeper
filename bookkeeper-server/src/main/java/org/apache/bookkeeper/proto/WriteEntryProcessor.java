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

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * Processes add entry requests
 */
class WriteEntryProcessor extends PacketProcessorBase {

    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessor.class);
    private byte[] masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];

    WriteEntryProcessor(ByteBuffer packet, Cnxn srcConn, Bookie bookie) {
        super(packet, srcConn, bookie);
    }

    @Override
    public void safeRun() {
        final long startTimeNanos = MathUtils.nowInNano();
        header = PacketHeader.fromInt(packet.getInt());
        packet.get(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
        // We mark the packet's position because we need the ledgerId and entryId in case
        // there is a version mis match and for logging.
        packet.mark();
        ledgerId = packet.getLong();
        entryId = packet.getLong();
        packet.reset();
        // Check the version.
        if (!isVersionCompatible(header)) {
            // The client and server versions are not compatible. Just return
            // an error.
            sendResponse(BookieProtocol.EBADVERSION,
                         BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         buildResponse(BookieProtocol.EBADVERSION));
            return;
        }
        if (bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode,"
                    + " so rejecting the request from the client!");
            sendResponse(BookieProtocol.EBADVERSION,
                         BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         buildResponse(BookieProtocol.EREADONLY));
            return;
        }
        short flags = header.getFlags();
        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      InetSocketAddress addr, Object ctx) {
                if (rc == BookieProtocol.EOK) {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                } else {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                }
                Cnxn conn = (Cnxn) ctx;
                assert ledgerId == WriteEntryProcessor.this.ledgerId;
                assert entryId == WriteEntryProcessor.this.entryId;
                sendResponse(rc, BookkeeperServerOp.ADD_ENTRY_REQUEST,
                             buildResponse(rc));
            }
        };
        int rc = BookieProtocol.EOK;
        try {
            if ((flags & BookieProtocol.FLAG_RECOVERY_ADD) == BookieProtocol.FLAG_RECOVERY_ADD) {
                bookie.recoveryAddEntry(packet.slice(), wcb, srcConn, masterKey);
            } else {
                bookie.addEntry(packet.slice(), wcb, srcConn, masterKey);
            }
            rc = BookieProtocol.EOK;
        } catch (IOException e) {
            logger.error("Error writing entry:" + entryId + " to ledger:" + ledgerId, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.error("Ledger fenced while writing entry:" + entryId +
                    " to ledger:" + ledgerId);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId +
                    " while writing entry:" + entryId);
            rc = BookieProtocol.EUA;
        }

        if (rc != BookieProtocol.EOK) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .ADD_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
            sendResponse(rc, BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         buildResponse(rc));
        }
    }
}
