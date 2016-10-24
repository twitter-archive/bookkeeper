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
