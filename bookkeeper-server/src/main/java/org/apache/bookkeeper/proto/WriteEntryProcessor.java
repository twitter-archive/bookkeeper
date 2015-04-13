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
import java.net.InetSocketAddress;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol.AddRequest;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes add entry requests
 */
class WriteEntryProcessor extends PacketProcessorBase {

    private final static Logger logger = LoggerFactory.getLogger(WriteEntryProcessor.class);

    public WriteEntryProcessor(Request request, Channel channel, Bookie bookie) {
        super(request, channel, bookie);
    }

    @Override
    public void safeRun() {
        final long startTimeNanos = MathUtils.nowInNano();
        // Check the version.
        if (!isVersionCompatible(request)) {
            // The client and server versions are not compatible. Just return
            // an error.
            sendResponse(BookieProtocol.EBADVERSION,
                         BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, request));
            return;
        }
        AddRequest add = (AddRequest) request;
        if (bookie.isReadOnly()) {
            logger.warn("BookieServer is running as readonly mode,"
                    + " so rejecting the request from the client!");
            sendResponse(BookieProtocol.EBADVERSION,
                         BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EREADONLY, add));
            return;
        }
        BookkeeperInternalCallbacks.WriteCallback wcb = new BookkeeperInternalCallbacks.WriteCallback() {
            @Override
            public void writeComplete(int rc, long ledgerId, long entryId,
                                      BookieSocketAddress addr, Object ctx) {
                if (rc == BookieProtocol.EOK) {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                } else {
                    ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                            .ADD_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                }
                assert ledgerId == request.getLedgerId();
                assert entryId == request.getEntryId();
                sendResponse(rc, BookkeeperServerOp.ADD_ENTRY_REQUEST,
                             ResponseBuilder.buildAddResponse(request));
            }
        };
        int rc = BookieProtocol.EOK;
        try {
            if (add.isRecoveryAdd()) {
                bookie.recoveryAddEntry(add.getDataAsByteBuffer(), wcb, channel, add.getMasterKey());
            } else {
                bookie.addEntry(add.getDataAsByteBuffer(), wcb, channel, add.getMasterKey());
            }
            rc = BookieProtocol.EOK;
        } catch (IOException e) {
            logger.error("Error writing {} : ", add, e);
            rc = BookieProtocol.EIO;
        } catch (BookieException.LedgerFencedException e) {
            logger.error("Ledger fenced while writing {}", add);
            rc = BookieProtocol.EFENCED;
        } catch (BookieException e) {
            logger.error("Unauthorized access to ledger:{} while writing entry: {}", add.getLedgerId(),
                    add.getEntryId());
            rc = BookieProtocol.EUA;
        }

        if (rc != BookieProtocol.EOK) {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .ADD_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
            sendResponse(rc, BookkeeperServerOp.ADD_ENTRY_REQUEST,
                         ResponseBuilder.buildErrorResponse(rc, add));
        }
    }

    @Override
    public String toString() {
        return String.format("WriteEntry(%d, %d)",
                             request.getLedgerId(), request.getEntryId());
    }
}
