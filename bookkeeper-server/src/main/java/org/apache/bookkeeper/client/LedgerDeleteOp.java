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

import org.apache.bookkeeper.client.AsyncCallback.DeleteCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.client.BookKeeperClientStats.LEDGER_DELETE;

/**
 * Encapsulates asynchronous ledger delete operation
 *
 */
class LedgerDeleteOp extends OrderedSafeGenericCallback<Void> {

    static final Logger LOG = LoggerFactory.getLogger(LedgerDeleteOp.class);

    BookKeeper bk;
    long ledgerId;
    DeleteCallback cb;
    Object ctx;
    long startTime;
    OpStatsLogger deleteOpLogger;

    /**
     * Constructor
     *
     * @param bk
     *            BookKeeper object
     * @param ledgerId
     *            ledger Id
     * @param cb
     *            callback implementation
     * @param ctx
     *            optional control object
     */
    LedgerDeleteOp(BookKeeper bk, long ledgerId, DeleteCallback cb, Object ctx) {
        super(bk.mainWorkerPool, ledgerId);
        this.bk = bk;
        this.ledgerId = ledgerId;
        this.cb = cb;
        this.ctx = ctx;
        this.startTime = MathUtils.nowInNano();
        this.deleteOpLogger = bk.getStatsLogger().getOpStatsLogger(LEDGER_DELETE);
    }

    /**
     * Initiates the operation
     */
    public void initiate() {
        // Asynchronously delete the ledger from meta manager
        // When this completes, it will invoke the callback method below.
        bk.getLedgerManager().deleteLedger(ledgerId, this);
    }

    /**
     * Implements Delete Callback.
     */
    @Override
    public void safeOperationComplete(int rc, Void result) {
        if (BKException.Code.OK != rc) {
            deleteOpLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTime));
        } else {
            deleteOpLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTime));
        }
        cb.deleteComplete(rc, this.ctx);
    }

    @Override
    public String toString() {
        return String.format("LedgerDeleteOp(%d)", ledgerId);
    }
}
