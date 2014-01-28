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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLastConfirmedLongPollOp extends TryReadLastConfirmedOp {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedLongPollOp.class);

    private long timeOutInMillis;


    ReadLastConfirmedLongPollOp(LedgerHandle lh, ReadLastConfirmedOp.LastConfirmedDataCallback cb, long lac, long timeOutInMillis) {
        super(lh, cb, lac);
        this.timeOutInMillis = timeOutInMillis;
    }

    @Override
    public void initiate() {
        long previousLAC = maxRecoveredData.lastAddConfirmed;
        LOG.trace("Calling Long Polling with previousLAC:{}, timeOutMillis: {}", previousLAC, timeOutInMillis);
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.bookieClient.readEntryWaitForLACUpdate(lh.metadata.currentEnsemble.get(i),
                lh.ledgerId,
                BookieProtocol.LAST_ADD_CONFIRMED,
                previousLAC,
                timeOutInMillis,
                false,
                this, i);
        }
    }

    @Override
    protected Enum getRequestStatsOp() {
        return BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_LONG_POLL;
    }
}
