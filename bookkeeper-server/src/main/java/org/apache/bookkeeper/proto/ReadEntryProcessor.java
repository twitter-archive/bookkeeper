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
import org.apache.bookkeeper.proto.BookieProtocol.ReadRequest;
import org.apache.bookkeeper.proto.BookieProtocol.Request;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReadEntryProcessor extends PacketProcessorBase {
    private final static Logger logger = LoggerFactory.getLogger(ReadEntryProcessor.class);

    ReadEntryProcessor(Request request, Channel channel, Bookie bookie) {
        super(request, channel, bookie);
    }

    @Override
    public void safeRun() {
        if (!isVersionCompatible(request)) {
            sendResponse(BookieProtocol.EBADVERSION,
                         BookkeeperServerOp.READ_ENTRY_REQUEST,
                         ResponseBuilder.buildErrorResponse(BookieProtocol.EBADVERSION, request));
            return;
        }
        ReadRequest read = (ReadRequest) request;
        int rc = BookieProtocol.EIO;
        final long startTimeNanos = MathUtils.nowInNano();
        ByteBuffer data = null;
        try {
            Future<Boolean> fenceResult = null;
            if (read.isFencingRequest()) {
                logger.warn("Ledger fence request received for ledger:{} from address:{}",
                        read.getLedgerId(), channel.getRemoteAddress());
                if (read.hasMasterKey()) {
                    fenceResult = bookie.fenceLedger(read.getLedgerId(), read.getMasterKey());
                } else {
                    logger.error("Password not provided, Not safe to fence {}", read.getLedgerId());
                    throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
                }
            }
            data = bookie.readEntry(read.getLedgerId(), read.getEntryId());
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
                        data = null;
                    } else {
                        rc = BookieProtocol.EOK;
                    }
                } catch (InterruptedException ie) {
                    logger.error("Interrupting fence read entry {} : ", read, ie);
                    rc = BookieProtocol.EIO;
                    data = null;
                } catch (ExecutionException ee) {
                    logger.error("Failed to fence read entry {} : ", read, ee);
                    rc = BookieProtocol.EIO;
                    data = null;
                } catch (TimeoutException te) {
                    logger.error("Timeout to fence read entry {} : ", read, te);
                    rc = BookieProtocol.EIO;
                    data = null;
                }
            } else {
                rc = BookieProtocol.EOK;
            }
        } catch (Bookie.NoLedgerException e) {
            rc = BookieProtocol.ENOLEDGER;
            logger.error("No ledger found while reading {}", read);
        } catch (Bookie.NoEntryException e) {
            rc = BookieProtocol.ENOENTRY;
            logger.error("No entry found while reading {}", read);
        } catch (IOException e) {
            rc = BookieProtocol.EIO;
            logger.error("IOException while reading {}", read);
        } catch (BookieException e) {
            logger.error(
                    "Unauthorized access to ledger:{} while reading entry:{} in request from address : {}",
                    new Object[] { read.getLedgerId(), read.getEntryId(), channel.getRemoteAddress() });
            rc = BookieProtocol.EUA;
        }

        if (rc == BookieProtocol.EOK) {
            sendResponse(rc, BookkeeperServerOp.READ_ENTRY_REQUEST,
                         ResponseBuilder.buildReadResponse(data, read));
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        } else {
            sendResponse(rc, BookkeeperServerOp.READ_ENTRY_REQUEST,
                         ResponseBuilder.buildErrorResponse(rc, read));
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                    .READ_ENTRY).registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
        }
    }

    @Override
    public String toString() {
        return String.format("ReadEntry(%d, %d)", request.getLedgerId(), request.getEntryId());
    }
}
