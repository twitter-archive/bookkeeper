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

package org.apache.bookkeeper.bookie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Observable;
import java.util.Observer;

import com.google.common.util.concurrent.SettableFuture;

import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.Bookie.METAENTRY_ID_FENCE_KEY;

/**
 * Implements a ledger inside a bookie. In particular, it implements operations
 * to write entries to a ledger and read entries from a ledger.
 */
public abstract class LedgerDescriptor {

    static LedgerDescriptor create(byte[] masterKey,
                                   long ledgerId,
                                   LedgerStorage ledgerStorage,
                                   StatsLogger statsLogger)
            throws IOException {
        LedgerDescriptor ledger = new LedgerDescriptorImpl(masterKey, ledgerId, ledgerStorage, statsLogger);
        ledgerStorage.setMasterKey(ledgerId, masterKey);
        return ledger;
    }

    static LedgerDescriptor createReadOnly(long ledgerId,
                                           LedgerStorage ledgerStorage,
                                           StatsLogger statsLogger)
            throws IOException, Bookie.NoLedgerException {
        if (!ledgerStorage.ledgerExists(ledgerId)) {
            throw new Bookie.NoLedgerException(ledgerId);
        }
        return new LedgerDescriptorReadOnlyImpl(ledgerId, ledgerStorage, statsLogger);
    }

    static ByteBuffer createLedgerFenceEntry(Long ledgerId) {
        ByteBuffer bb = ByteBuffer.allocate(8 + 8);
        bb.putLong(ledgerId);
        bb.putLong(METAENTRY_ID_FENCE_KEY);
        bb.flip();
        return bb;
    }

    abstract void checkAccess(byte masterKey[]) throws BookieException, IOException;

    abstract long getLedgerId();

    abstract boolean setFenced() throws IOException;
    abstract boolean isFenced() throws IOException;

    /**
     * When we fence a ledger, we need to first set ledger to fenced state in memory and
     * then log the fence entry in Journal so that we can rebuild the state.
     *
     * We should satisfy the future only after we complete logging fence entry in Journal
     */
    abstract SettableFuture<Boolean> fenceAndLogInJournal(Journal journal) throws IOException;

    abstract long addEntry(ByteBuffer entry) throws IOException;
    abstract ByteBuffer readEntry(long entryId) throws IOException;
    abstract long getLastAddConfirmed() throws IOException;
    abstract Observable waitForLastAddConfirmedUpdate(long previoisLAC, Observer observer) throws IOException;
}
