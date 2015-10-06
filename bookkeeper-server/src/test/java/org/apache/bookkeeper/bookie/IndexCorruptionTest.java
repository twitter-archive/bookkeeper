package org.apache.bookkeeper.bookie;

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

import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;
import java.util.Enumeration;

import org.apache.bookkeeper.bookie.CheckpointProgress.CheckPoint;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.Assert;
import org.junit.Test;

import static com.google.common.base.Charsets.UTF_8;

/**
 * This class tests that index corruption cases
 */
public class IndexCorruptionTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(IndexCorruptionTest.class);

    DigestType digestType;

    int pageSize = 1024;

    public IndexCorruptionTest() {
        super(1);
        this.digestType = DigestType.CRC32;
        baseConf.setPageSize(pageSize);
    }

    @Test(timeout=60000)
    public void testNoSuchLedger() throws Exception {
        LOG.debug("Testing NoSuchLedger");

        SyncThread syncThread = bs.get(0).getBookie().syncThread;
        syncThread.suspendSync();
        // Create a ledger
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, "".getBytes());

        // Close the ledger which cause a readEntry(0) call
        LedgerHandle newLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());

        // Create a new ledger to write entries
        String dummyMsg = "NoSuchLedger";
        int numMsgs = 3;
        LedgerHandle wlh = bkc.createLedger(1, 1, digestType, "".getBytes());
        for (int i=0; i<numMsgs; i++) {
            wlh.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // trigger sync
        syncThread.checkPoint(CheckPoint.MAX);

        // restart bookies
        restartBookies();

        Enumeration<LedgerEntry> seq = wlh.readEntries(0, numMsgs - 1);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements() == true);
        int entryId = 0;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, numMsgs);
    }

    @Test(timeout = 60000)
    public void testEmptyIndexPage() throws Exception {
        LOG.debug("Testing EmptyIndexPage");

        SyncThread syncThread = bs.get(0).getBookie().syncThread;
        assertNotNull("Not found SyncThread.", syncThread);

        syncThread.suspendSync();

        // Create a ledger
        LedgerHandle lh1 = bkc.createLedger(1, 1, digestType, "".getBytes());

        String dummyMsg = "NoSuchLedger";

        // write two page entries to ledger 2
        int numMsgs = 2 * pageSize / 8;
        LedgerHandle lh2 = bkc.createLedger(1, 1, digestType, "".getBytes());
        for (int i=0; i<numMsgs; i++) {
            lh2.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // trigger sync
        syncThread.checkPoint(CheckPoint.MAX);

        syncThread.suspendSync();

        // Close ledger 1 which cause a readEntry(0) call
        LedgerHandle newLh1 = bkc.openLedger(lh1.getId(), digestType, "".getBytes());

        // write another 3 entries to ledger 2
        for (int i=0; i<3; i++) {
            lh2.addEntry(dummyMsg.getBytes());
        }

        syncThread.resumeSync();

        // wait for sync again
        syncThread.checkPoint(CheckPoint.MAX);

        // restart bookies
        restartBookies();

        numMsgs += 3;
        Enumeration<LedgerEntry> seq = lh2.readEntries(0, numMsgs - 1);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements() == true);
        int entryId = 0;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, numMsgs);
    }

    @Test(timeout = 60000)
    public void testPartialFlushedIndexPageAlignedWithIndexEntrySize() throws Exception {
        testPartialFlushedIndexPage(pageSize / 2);
    }

    @Test(timeout = 60000)
    public void testPartialFlushedIndexPageWithPartialIndexEntry() throws Exception {
        testPartialFlushedIndexPage(pageSize / 2 - 3);
    }

    void testPartialFlushedIndexPage(int sizeToTruncate) throws Exception {
        SyncThread syncThread = bs.get(0).getBookie().getSyncThread();
        assertNotNull("Not found SyncThread.", syncThread);

        // suspend sync thread
        syncThread.suspendSync();

        // Create a ledger and write entries
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, "".getBytes());
        String dummyMsg = "PartialFlushedIndexPage";
        // write two page entries to ledger
        int numMsgs = 2 * pageSize / 8;
        for (int i = 0; i < numMsgs; i++) {
            lh.addEntry(dummyMsg.getBytes(UTF_8));
        }

        // simulate partial flushed index page during shutdown
        // 0. flush the ledger storage
        Bookie bookie = bs.get(0).getBookie();
        bookie.ledgerStorage.flush();
        // 1. truncate the ledger index file to simulate partial flushed index page
        LedgerCacheImpl ledgerCache = (LedgerCacheImpl) ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache;
        File lf = ledgerCache.indexPersistenceManager.findIndexFile(lh.getId());
        FileChannel fc = new FileOutputStream(lf, true).getChannel();
        fc.truncate(fc.size() - sizeToTruncate);
        fc.close();

        syncThread.resumeSync();

        // restart bookies
        // 1. bookies should restart successfully without hitting ShortReadException
        // 2. since SyncThread was suspended, bookie would still replay journals. so all entries should not be lost.
        restartBookies();

        // Open the ledger
        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        Enumeration<LedgerEntry> seq = readLh.readEntries(0, numMsgs - 1);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements());
        int entryId = 0;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, numMsgs);
    }

    @Test(timeout = 60000)
    public void testMissingEntriesWithoutPartialIndexEntry() throws Exception {
        long availableStartEntryId = 0L;
        long availableEndEntryId = (pageSize + pageSize / 2) / 8 - 1;
        long missingStartEntryId = availableEndEntryId + 1;
        long missingEndEntryId = (2 * pageSize) / 8 - 1;
        testPartialIndexPage(false, pageSize / 2, availableStartEntryId, availableEndEntryId,
                             missingStartEntryId, missingEndEntryId);
    }

    @Test(timeout = 60000)
    public void testMissingEntriesWithPartialIndexEntry() throws Exception {
        long availableStartEntryId = 0L;
        long availableEndEntryId = (pageSize + pageSize / 2 - 3) / 8 - 1;
        long missingStartEntryId = availableEndEntryId + 1;
        long missingEndEntryId = (2 * pageSize) / 8 - 1;
        testPartialIndexPage(false, pageSize / 2 + 3, availableStartEntryId, availableEndEntryId,
                missingStartEntryId, missingEndEntryId);
    }

    @Test(timeout = 60000)
    public void testNoPartialIndexEntry() throws Exception {
        long availableStartEntryId = 0L;
        long availableEndEntryId = (pageSize + pageSize / 2) / 8 - 1;
        testPartialIndexPage(true, pageSize / 2, availableStartEntryId, availableEndEntryId, 0, -1);
    }

    @Test(timeout = 60000)
    public void testPartialIndexEntry() throws Exception {
        long availableStartEntryId = 0L;
        long availableEndEntryId = (pageSize + pageSize / 2 - 3) / 8 - 1;
        testPartialIndexPage(true, pageSize / 2 + 3, availableStartEntryId, availableEndEntryId, 0, -1);
    }

    void testPartialIndexPage(boolean startNewBookie,
                              int sizeToTruncate,
                              long availableStartEntryId, long avaialbleEndEntryId,
                              long missingStartEntryId, long missingEndEntryId) throws Exception {
        int ensembleSize = 1;
        if (startNewBookie) {
            int newBookiePort = this.startNewBookie();
            LOG.info("Started new bookie @ port {}", newBookiePort);
            ++ensembleSize;
        }

        // Create a ledger and write entries
        LedgerHandle lh = bkc.createLedger(ensembleSize, ensembleSize, digestType, "".getBytes());
        String dummyMsg = "PartialIndexPage";
        int numMsgs = 2 * pageSize / 8;
        for (int i = 0; i < numMsgs; i++) {
            lh.addEntry(dummyMsg.getBytes(UTF_8));
        }

        for (BookieServer bookieServer : bs) {
            CheckPoint cp = bookieServer.getBookie().getSyncThread().requestCheckpoint();
            bookieServer.getBookie().getSyncThread().checkPoint(cp);
        }

        lh.close();

        // cause partial index page
        Bookie bookie = bs.get(0).getBookie();
        LedgerCacheImpl ledgerCache = (LedgerCacheImpl) ((InterleavedLedgerStorage) bookie.ledgerStorage).ledgerCache;
        File lf = ledgerCache.indexPersistenceManager.findIndexFile(lh.getId());
        FileChannel fc = new FileOutputStream(lf, true).getChannel();
        fc.truncate(fc.size() - sizeToTruncate);
        fc.close();

        // restart bookies
        restartBookies();

        // Open the ledger
        LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        Enumeration<LedgerEntry> seq = readLh.readEntries(availableStartEntryId, avaialbleEndEntryId);
        assertTrue("Enumeration of ledger entries has no element", seq.hasMoreElements());
        long entryId = availableStartEntryId;
        while (seq.hasMoreElements()) {
            LedgerEntry e = seq.nextElement();
            assertEquals(entryId, e.getEntryId());

            Assert.assertArrayEquals(dummyMsg.getBytes(), e.getEntry());
            ++entryId;
        }
        assertEquals(entryId, avaialbleEndEntryId + 1);

        // missing entries
        for (entryId = missingStartEntryId; entryId <= missingEndEntryId; ++entryId) {
            try {
                readLh.readEntries(entryId, entryId);
                fail("Should fail on reading missing entry " + entryId);
            } catch (BKException.BKNoSuchEntryException bnsee) {
                // expected
            }
        }
    }

}
