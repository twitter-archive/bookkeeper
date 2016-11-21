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
import java.util.Arrays;
import java.util.Observable;
import java.util.Observer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.util.concurrent.SettableFuture;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

/**
 * Implements a ledger inside a bookie. In particular, it implements operations
 * to write entries to a ledger and read entries from a ledger.
 *
 */
public class LedgerDescriptorImpl extends LedgerDescriptor {
    final static Logger LOG = LoggerFactory.getLogger(LedgerDescriptor.class);
    final LedgerStorage ledgerStorage;
    private long ledgerId;
    final byte[] masterKey;

    private AtomicBoolean fenceEntryPersisted = new AtomicBoolean();
    private SettableFuture<Boolean> logFenceResult = null;

    // Stats
    final Counter writeBytesCounter;
    final Counter readBytesCounter;
    final OpStatsLogger addEntryBytesStats;
    final OpStatsLogger readEntryBytesStats;

    LedgerDescriptorImpl(byte[] masterKey,
                         long ledgerId,
                         LedgerStorage ledgerStorage,
                         StatsLogger statsLogger) {
        this.masterKey = masterKey;
        this.ledgerId = ledgerId;
        this.ledgerStorage = ledgerStorage;
        // Stats
        this.writeBytesCounter = statsLogger.getCounter(WRITE_BYTES);
        this.readBytesCounter = statsLogger.getCounter(READ_BYTES);
        this.addEntryBytesStats = statsLogger.getOpStatsLogger(BOOKIE_ADD_ENTRY_BYTES);
        this.readEntryBytesStats = statsLogger.getOpStatsLogger(BOOKIE_READ_ENTRY_BYTES);
    }

    @Override
    void checkAccess(byte masterKey[]) throws BookieException, IOException {
        if (!Arrays.equals(this.masterKey, masterKey)) {
            throw BookieException.create(BookieException.Code.UnauthorizedAccessException);
        }
    }

    @Override
    public long getLedgerId() {
        return ledgerId;
    }

    @Override
    boolean setFenced() throws IOException {
        return ledgerStorage.setFenced(ledgerId);
    }

    @Override
    boolean isFenced() throws IOException {
        return ledgerStorage.isFenced(ledgerId);
    }

    @Override
    synchronized SettableFuture<Boolean> fenceAndLogInJournal(Journal journal) throws IOException {
        boolean success = this.setFenced();
        if(success) {
            // fenced for first time, we should add the key to journal ensure we can rebuild.
            return logFenceEntryInJournal(journal);
        } else {
            // If we reach here, it means this ledger has been fenced before.
            // However, fencing might still be in progress.
            if(logFenceResult == null || fenceEntryPersisted.get()){
                // Either ledger's fenced state is recovered from Journal
                // Or Log fence entry in Journal succeed
                SettableFuture<Boolean> result = SettableFuture.create();
                result.set(true);
                return result;
            } else if (logFenceResult.isDone()) {
                // We failed to log fence entry in Journal, try again.
                return logFenceEntryInJournal(journal);
            }
            // Fencing is in progress
            return logFenceResult;
        }
    }

    /**
     * Log the fence ledger entry in Journal so that we can rebuild the state.
     * @param journal log the fence entry in the Journal
     * @return A future which will be satisfied when add entry to journal complete
     */
    private SettableFuture<Boolean> logFenceEntryInJournal(Journal journal) {
        logFenceResult = SettableFuture.create();
        ByteBuffer entry = createLedgerFenceEntry(ledgerId);
        journal.logAddEntry(entry, (rc, ledgerId, entryId, addr, ctx) -> {
            LOG.debug("Record fenced state for ledger {} in journal with rc {}", ledgerId, rc);
            if (rc == 0) {
                fenceEntryPersisted.compareAndSet(false, true);
                logFenceResult.set(true);
            } else {
                logFenceResult.set(false);
            }
        }, null);
        return logFenceResult;
    }


    @Override
    long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();

        if (ledgerId != this.ledgerId) {
            throw new IOException("Entry for ledger " + ledgerId + " was sent to " + this.ledgerId);
        }
        entry.rewind();

        int dataSize = entry.remaining();
        boolean success = false;
        try {
            long entryId =  ledgerStorage.addEntry(entry);
            success = true;
            return entryId;
        } finally {
            if (success) {
                writeBytesCounter.add(dataSize);
                addEntryBytesStats.registerSuccessfulEvent(dataSize);
            } else {
                addEntryBytesStats.registerFailedEvent(dataSize);
            }
        }
    }

    @Override
    ByteBuffer readEntry(long entryId) throws IOException {
        boolean success = false;
        int dataSize = 0;
        try {
            ByteBuffer data = ledgerStorage.getEntry(ledgerId, entryId);
            success = true;
            dataSize = data.remaining();
            return data;
        } finally {
            if (success) {
                readBytesCounter.add(dataSize);
                readEntryBytesStats.registerSuccessfulEvent(dataSize);
            } else {
                readEntryBytesStats.registerFailedEvent(dataSize);
            }
        }
    }

    @Override
    long getLastAddConfirmed() throws IOException {
        return ledgerStorage.getLastAddConfirmed(ledgerId);
    }

    @Override
    Observable waitForLastAddConfirmedUpdate(long previoisLAC, Observer observer) throws IOException {
        return ledgerStorage.waitForLastAddConfirmedUpdate(ledgerId, previoisLAC, observer);
    }
}
