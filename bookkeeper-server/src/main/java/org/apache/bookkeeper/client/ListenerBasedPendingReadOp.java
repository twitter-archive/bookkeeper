/*
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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryListener;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger.BookkeeperClientOp;
import org.apache.bookkeeper.util.MathUtils;

import java.util.NoSuchElementException;
import java.util.concurrent.ScheduledExecutorService;

class ListenerBasedPendingReadOp extends PendingReadOp {

    final ReadEntryListener listener;

    ListenerBasedPendingReadOp(LedgerHandle lh, ScheduledExecutorService scheduler,
                               long startEntryId, long endEntryId,
                               ReadEntryListener listener, Object ctx) {
        super(lh, scheduler, startEntryId, endEntryId, null, ctx);
        this.listener = listener;
    }

    @Override
    protected void submitCallback(int code) {
        LedgerEntryRequest request;
        while ((request = seq.peek()) != null) {
            if (!request.isComplete()) {
                return;
            }
            seq.remove();
            long latencyMillis = MathUtils.now() - requestTimeMillis;
            if (BKException.Code.OK == request.getRc()) {
                lh.getStatsLogger().getOpStatsLogger(BookkeeperClientOp.READ_ENTRY)
                        .registerSuccessfulEvent(latencyMillis);
            } else {
                lh.getStatsLogger().getOpStatsLogger(BookkeeperClientOp.READ_ENTRY)
                        .registerFailedEvent(latencyMillis);
            }
            // callback with completed entry
            listener.onEntryComplete(request.getRc(), lh, request, ctx);
        }
        // if all entries are already completed.
        cancelSpeculativeTask(true);
    }

    @Override
    public boolean hasMoreElements() {
        return false;
    }

    @Override
    public LedgerEntry nextElement() throws NoSuchElementException {
        throw new NoSuchElementException();
    }
}
