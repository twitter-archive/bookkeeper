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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.proto.BookieProtocol.PacketHeader;
import org.apache.bookkeeper.proto.NIOServerFactory.Cnxn;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadEntryProcessor extends PacketProcessorBase implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessor.class);

    public ReadEntryProcessor(ByteBuffer packet, Cnxn srcConn, Bookie bookie) {
        super(packet, srcConn, bookie);
    }

    public void run() {
        final long startTimeNanos = MathUtils.nowInNano();
        header = PacketHeader.fromInt(packet.getInt());
        ledgerId = packet.getLong();
        entryId = packet.getLong();
        if (!isVersionCompatible(header)) {
            srcConn.sendResponse(buildResponse(BookieProtocol.EBADVERSION));
            return;
        }
        short flags = header.getFlags();
        // The response consists of 2 bytebuffers. The first one contains the packet header and meta data
        // The second contains the actual entry.
        ByteBuffer[] toSend = new ByteBuffer[2];
        int rc = BookieProtocol.EIO;
        try {
            Future<Boolean> fenceResult = null;
            if ((flags & BookieProtocol.FLAG_DO_FENCING) == BookieProtocol.FLAG_DO_FENCING) {
                logger.warn("Ledger fence request received for ledger:" + ledgerId + " from address:" + srcConn.getPeerName());
                if (header.getVersion() >= 2) {
                    // Versions below 2 don't allow fencing ledgers.
                    byte[] masterKey = new byte[BookieProtocol.MASTER_KEY_LENGTH];
                    packet.get(masterKey, 0, BookieProtocol.MASTER_KEY_LENGTH);
                    fenceResult = bookie.fenceLedger(ledgerId, masterKey);
                } else {
                    logger.error("Fencing a ledger is not supported by version:" + header.getVersion());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            toSend[1] = bookie.readEntry(ledgerId, entryId);
            if (null != fenceResult) {
                // TODO:
                // currently we don't have readCallback to run in separated read
                // threads. after BOOKKEEPER-429 is complete, we could improve
                // following code to make it not wait here
                //
                // For now, since we only try to wait after read entry. so writing
                // to journal and read entry are executed in different thread
                // it would be fine.
                try {
                    Boolean fenced = fenceResult.get(1000, TimeUnit.MILLISECONDS);
                    if (null == fenced || !fenced) {
                        // if failed to fence, fail the read request to make it retry.
                        rc = BookieProtocol.EIO;
                        toSend[1] = null;
                    } else {
                        rc = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    logger.error("Interrupting fence read entry (lid:" + ledgerId
                              + ", eid:" + entryId + ") :", ie);
                    rc = BookieProtocol.EIO;
                    toSend[1] = null;
                } catch (ExecutionException ee) {
                    logger.error("Failed to fence read entry (lid:" + ledgerId
                              + ", eid:" + entryId + ") :", ee);
                    rc = BookieProtocol.EIO;
                    toSend[1] = null;
                } catch (TimeoutException te) {
                    logger.error("Timeout to fence read entry (lid:" + ledgerId
                              + ", eid:" + entryId + ") :", te);
                    rc = BookieProtocol.EIO;
                    toSend[1] = null;
                }
            } else {
                rc = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            rc = BookieProtocol.ENOLEDGER;
            logger.error("No ledger found while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (Bookie.NoEntryException e) {
            rc = BookieProtocol.ENOENTRY;
            logger.error("No entry found while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (IOException e) {
            rc = BookieProtocol.EIO;
            logger.error("IOException while reading entry:" + entryId + " from ledger:" +
                    ledgerId);
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:" + ledgerId + " while reading entry:" + entryId + " in request " +
                    "from address:" + srcConn.getPeerName());
            rc = BookieProtocol.EUA;
        }

        if (rc == BookieProtocol.EOK) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        } else {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        }

        toSend[0] = buildResponse(rc);
        // If we caught an exception, we still need to fill in the response with the ledger id and entry id.
        if (null == toSend[1]) {
            toSend[1] = ByteBuffer.allocate(16);
            toSend[1].putLong(ledgerId);
            toSend[1].putLong(entryId);
            toSend[1].flip();
        }
        srcConn.sendResponse(toSend);
    }
}
