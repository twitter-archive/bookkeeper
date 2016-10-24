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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.meta.FlatLedgerManager;
import org.apache.bookkeeper.meta.SnapshotMap;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.TestUtils;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the entry log compaction functionality.
 * TODO: Modify the test to handle dynamically allocated chunks.
 */
abstract class CompactionTest extends BookKeeperClusterTestCase {

    private final static Logger LOG = LoggerFactory.getLogger(CompactionTest.class);

    static int ENTRY_OVERHEAD_SIZE = 44; // Metadata + CRC + Length
    static int ENTRY_SIZE = 1024;
    static int NUM_BOOKIES = 1;

    private boolean isThrottleByBytes;
    DigestType digestType;
    int numEntries;
    int gcWaitTime;
    double minorCompactionThreshold;
    double majorCompactionThreshold;
    long minorCompactionInterval;
    long majorCompactionInterval;

    String msg;

    protected CompactionTest(boolean isByBytes) {
        super(NUM_BOOKIES);

        this.isThrottleByBytes = isByBytes;
        this.digestType = DigestType.CRC32;

        numEntries = 100;
        gcWaitTime = 1000;
        minorCompactionThreshold = 0.1f;
        majorCompactionThreshold = 0.5f;
        minorCompactionInterval = 2 * gcWaitTime / 1000;
        majorCompactionInterval = 4 * gcWaitTime / 1000;

        // a dummy message
        StringBuilder msgSB = new StringBuilder();
        for (int i = 0; i < ENTRY_SIZE - ENTRY_OVERHEAD_SIZE; i++) {
            msgSB.append("a");
        }
        msg = msgSB.toString();
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseConf.setEntryLogSizeLimit(numEntries * ENTRY_SIZE);
        // Disable skip list for compaction
        baseConf.setSortedLedgerStorageEnabled(false);
        baseConf.setGcWaitTime(gcWaitTime);
        baseConf.setMinorCompactionThreshold(minorCompactionThreshold);
        baseConf.setMajorCompactionThreshold(majorCompactionThreshold);
        baseConf.setMinorCompactionInterval(minorCompactionInterval);
        baseConf.setMajorCompactionInterval(majorCompactionInterval);
        baseConf.setEntryLogFilePreAllocationEnabled(false);
        baseConf.setIsThrottleByBytes(this.isThrottleByBytes);
        super.setUp();
    }

    LedgerHandle[] prepareData(int numEntryLogs, boolean changeNum)
        throws Exception {
        // since an entry log file can hold at most 100 entries
        // first ledger write 2 entries, which is less than low water mark
        int num1 = 2;
        // third ledger write more than high water mark entries
        int num3 = (int)(numEntries * 0.7f);
        // second ledger write remaining entries, which is higher than low water mark
        // and less than high water mark
        int num2 = numEntries - num3 - num1;

        LedgerHandle[] lhs = new LedgerHandle[3];
        for (int i=0; i<3; ++i) {
            lhs[i] = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        }

        for (int n = 0; n < numEntryLogs; n++) {
            for (int k = 0; k < num1; k++) {
                lhs[0].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num2; k++) {
                lhs[1].addEntry(msg.getBytes());
            }
            for (int k = 0; k < num3; k++) {
                lhs[2].addEntry(msg.getBytes());
            }
            if (changeNum) {
                --num2;
                ++num3;
            }
        }

        return lhs;
    }

    private void verifyLedger(long lid, long startEntryId, long endEntryId) throws Exception {
        LedgerHandle lh = bkc.openLedger(lid, digestType, "".getBytes());
        Enumeration<LedgerEntry> entries = lh.readEntries(startEntryId, endEntryId);
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(msg, new String(entry.getEntry()));
        }
    }

    @Test(timeout = 60000)
    public void testCompactionOnDeletedLedgers() throws Exception {
        // prepare data
        final LedgerHandle[] lhs = prepareData(3, false);

        // disable compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies();

        // remove ledger2 and ledger3
        // so entry log 1 and 2 would have ledger1 entries left
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                + baseConf.getGcWaitTime());

        Bookie bookie = this.bs.get(0).getBookie();
        final GarbageCollectorThread gcThread = ((InterleavedLedgerStorage) bookie.ledgerStorage).gcThread;

        final CountDownLatch flushLatch = new CountDownLatch(1);
        final CountDownLatch flushNotifier = new CountDownLatch(1);
        Thread flushThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    flushNotifier.await();
                } catch (InterruptedException e) {
                    // no-op
                }
                try {
                    bkc.deleteLedger(lhs[0].getId());
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted on deleting ledger {} : ",
                            lhs[0].getId(), e);
                } catch (BKException e) {
                    LOG.error("Error on deleting ledger {} : ",
                            lhs[0].getId(), e);
                }
                gcThread.doGcLedgers();
                flushLatch.countDown();
            }
        }, "flush-thread");
        flushThread.start();

        gcThread.doCompactEntryLogs(0.99, flushNotifier, flushLatch);

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }
    }

    @Test(timeout = 60000)
    public void testDisableCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        // disable compaction
        baseConf.setMinorCompactionThreshold(0.0f);
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger2 and ledger3
        // so entry log 1 and 2 would have ledger1 entries left
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1].log) should not be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0,1].log that should have been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1));
        }
    }

    @Test(timeout = 60000)
    public void testForceGarbageCollection() throws Exception {
        baseConf.setGcWaitTime(60000);
        baseConf.setMinorCompactionInterval(120000);
        baseConf.setMajorCompactionInterval(240000);
        LedgerDirsManager dirManager = new LedgerDirsManager(baseConf, tmpDirs.toArray(new File[tmpDirs.size()]));
        CheckpointSource cp = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                // Do nothing.
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                    throws IOException {
                // Do nothing
            }
        };
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage(baseConf,
                new FlatLedgerManager(baseConf, zkc), dirManager, dirManager, cp, NullStatsLogger.INSTANCE);
        storage.start();
        long startTime = MathUtils.now();
        Thread.sleep(2000);
        storage.gcThread.enableForceGC(false, false);
        // wait until current gc cycle is done
        while (storage.gcThread.forceGarbageCollection.get()) {
            Thread.sleep(100);
        }
        // Minor and Major compaction times should be larger than when we started
        // this test.
        assertTrue("Minor or major compaction did not trigger even on forcing.",
                storage.gcThread.lastMajorCompactionTime > startTime &&
                storage.gcThread.lastMinorCompactionTime > startTime);
        // disable compaction
        long lastMajorCompactionTime = storage.gcThread.lastMajorCompactionTime;
        long lastMinorCompactionTime = storage.gcThread.lastMinorCompactionTime;
        storage.gcThread.enableForceGC(true, true);
        // wait until current gc cycle is done
        while (storage.gcThread.forceGarbageCollection.get()) {
            Thread.sleep(100);
        }
        assertEquals("Major compaction shouldn't be triggered",
                lastMajorCompactionTime, storage.gcThread.lastMajorCompactionTime);
        assertEquals("Minor compaction shouldn't be triggered",
                lastMinorCompactionTime, storage.gcThread.lastMinorCompactionTime);
    }

    @Test(timeout = 60000)
    public void testMinorCompaction() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable major compaction
        baseConf.setMajorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());

        LOG.info("Finished deleting the ledgers contains most entries.");
        Thread.sleep(baseConf.getMinorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                            + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    @Test(timeout = 60000)
    public void testMajorCompaction() throws Exception {

        // prepare data
        LedgerHandle[] lhs = prepareData(3, true);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // disable minor compaction
        baseConf.setMinorCompactionThreshold(0.0f);

        // restart bookies
        restartBookies(baseConf);

        // remove ledger1 and ledger3
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");

        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should be compacted
        for (File ledgerDirectory : tmpDirs) {
            assertFalse("Found entry log file ([0,1,2].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0, 1, 2));
        }

        // even entry log files are removed, we still can access entries for ledger2
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[1].getId(), 0, lhs[1].getLastAddConfirmed());
    }

    @Test(timeout = 60000)
    public void testMajorCompactionAboveThreshold() throws Exception {
        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // remove ledger1 and ledger2
        bkc.deleteLedger(lhs[0].getId());
        bkc.deleteLedger(lhs[1].getId());
        LOG.info("Finished deleting the ledgers contains less entries.");
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs ([0,1,2].log) should not be compacted
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([1,2].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, false, 0, 1, 2));
        }
    }

    @Test(timeout = 60000)
    public void testCompactionSmallEntryLogs() throws Exception {

        // create a ledger to write a few entries
        LedgerHandle alh = bkc.createLedger(NUM_BOOKIES, NUM_BOOKIES, digestType, "".getBytes());
        for (int i=0; i<3; i++) {
           alh.addEntry(msg.getBytes());
        }
        alh.close();

        // restart bookie to roll entry log files
        restartBookies();

        // prepare data
        LedgerHandle[] lhs = prepareData(3, false);

        for (LedgerHandle lh : lhs) {
            lh.close();
        }

        // remove ledger2 and ledger3
        bkc.deleteLedger(lhs[1].getId());
        bkc.deleteLedger(lhs[2].getId());
        LOG.info("Finished deleting the ledgers contains most entries.");
        // restart bookies again to roll entry log files.
        restartBookies();
        Thread.sleep(baseConf.getMajorCompactionInterval() * 1000
                   + baseConf.getGcWaitTime());

        // entry logs (0.log) should not be compacted
        // entry logs ([1,2,3].log) should be compacted.
        for (File ledgerDirectory : tmpDirs) {
            assertTrue("Not Found entry log file ([0].log that should have been compacted in ledgerDirectory: "
                     + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 0));
            assertFalse("Found entry log file ([1,2,3].log that should have not been compacted in ledgerDirectory: "
                      + ledgerDirectory, TestUtils.hasLogFiles(ledgerDirectory, true, 1, 2, 3));
        }

        // even entry log files are removed, we still can access entries for ledger1
        // since those entries has been compacted to new entry log
        verifyLedger(lhs[0].getId(), 0, lhs[0].getLastAddConfirmed());
    }

    /**
     * Test that compaction doesnt add to index without having persisted
     * entrylog first. This is needed because compaction doesn't go through the journal.
     * {@see https://issues.apache.org/jira/browse/BOOKKEEPER-530}
     * {@see https://issues.apache.org/jira/browse/BOOKKEEPER-664}
     */
    @Test(timeout=60000)
    public void testCompactionSafety() throws Exception {
        tearDown(); // I dont want the test infrastructure
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        final Set<Long> ledgers = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        ActiveLedgerManager manager = getActiveLedgerManager(ledgers);

        File tmpDir = createTempDir("compaction", "compactionSafety");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[]{tmpDir.toString()});

        conf.setEntryLogSizeLimit(EntryLogger.LOGFILE_HEADER_LENGTH + 3 * (4 + ENTRY_SIZE));
        conf.setGcWaitTime(100);
        conf.setMinorCompactionThreshold(0.7f);
        conf.setMajorCompactionThreshold(0.0f);
        conf.setMinorCompactionInterval(1);
        conf.setMajorCompactionInterval(10);
        conf.setPageLimit(1);

        CheckpointSource checkpointProgress = new CheckpointSource() {
            AtomicInteger idGen = new AtomicInteger(0);
            class MyCheckpoint implements Checkpoint {
                int id = idGen.incrementAndGet();
                @Override
                public int compareTo(Checkpoint o) {
                    if (o == Checkpoint.MAX) {
                        return -1;
                    } else if (o == Checkpoint.MIN) {
                        return 1;
                    }
                    return id - ((MyCheckpoint)o).id;
                }
            }

            @Override
            public Checkpoint newCheckpoint() {
                return new MyCheckpoint();
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                    throws IOException {
            }
        };

        final byte[] KEY = "foobar".getBytes();
        File log0 = new File(curDir, "0.log");
        File log1 = new File(curDir, "1.log");
        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs());
        assertFalse("Log 0 shouldnt exist", log0.exists());
        assertFalse("Log 1 shouldnt exist", log1.exists());
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage(conf, manager,
                                                                        dirs, dirs, checkpointProgress,
                                                                        NullStatsLogger.INSTANCE);
        ledgers.add(1l);
        ledgers.add(2l);
        ledgers.add(3l);
        storage.setMasterKey(1, KEY);
        storage.setMasterKey(2, KEY);
        storage.setMasterKey(3, KEY);
        LOG.info("Write Ledger 1");
        storage.addEntry(genEntry(1, 1, ENTRY_SIZE));
        LOG.info("Write Ledger 2");
        storage.addEntry(genEntry(2, 1, ENTRY_SIZE));
        storage.addEntry(genEntry(2, 2, ENTRY_SIZE));
        LOG.info("Write ledger 3");
        storage.addEntry(genEntry(3, 2, ENTRY_SIZE));
        storage.flush();
        storage.shutdown();

        assertTrue("Log 0 should exist", log0.exists());
        assertTrue("Log 1 should exist", log1.exists());
        ledgers.remove(2l);
        ledgers.remove(3l);

        storage = new InterleavedLedgerStorage(conf, manager, dirs, dirs, checkpointProgress, NullStatsLogger.INSTANCE);
        storage.start();
        for (int i = 0; i < 10; i++) {
            if (!log0.exists() && !log1.exists()) {
                break;
            }
            Thread.sleep(1000);
            storage.entryLogger.flush(); // simulate sync thread
        }
        assertFalse("Log shouldnt exist", log0.exists());
        assertFalse("Log shouldnt exist", log1.exists());

        LOG.info("Write ledger 4");
        ledgers.add(4l);
        storage.setMasterKey(4, KEY);
        storage.addEntry(genEntry(4, 1, ENTRY_SIZE)); // force ledger 1 page to flush

        storage = new InterleavedLedgerStorage(conf, manager, dirs, dirs, checkpointProgress, NullStatsLogger.INSTANCE);
        storage.getEntry(1, 1); // entry should exist
    }

    private static ActiveLedgerManager getActiveLedgerManager(final Set<Long> remoteLedgers) {

        final SnapshotMap<Long, Long> localLedgers = new SnapshotMap<Long, Long>();

        return new ActiveLedgerManager() {
            @Override
            public void addActiveLedger(long ledgerId, boolean active) {
                LOG.info("ActiveLedgerManager add ledger {}", ledgerId);
                localLedgers.put(ledgerId, ledgerId);
            }

            @Override
            public void removeActiveLedger(long ledgerId) {
                LOG.info("ActiveLedgerManager remove ledger {}", ledgerId);
                localLedgers.remove(ledgerId);
            }

            @Override
            public boolean containsActiveLedger(long ledgerId) {
                return localLedgers.containsKey(ledgerId);
            }

            @Override
            public void garbageCollectLedgers(GarbageCollector gc) {
                Map<Long, Long> activeLocalLedgers = localLedgers.snapshot();
                LOG.info("Garbage collection ledgers : local = {}, remote = {}", activeLocalLedgers, remoteLedgers);
                for (Long bkLid : activeLocalLedgers.keySet()) {
                    if (!remoteLedgers.contains(bkLid)) {
                        activeLocalLedgers.remove(bkLid);
                        LOG.info("Garbage collecting ledger {}", bkLid);
                        gc.gc(bkLid);
                    }
                }
            }

            @Override
            public void close() throws IOException {
                // no-op
            }
        };
    }

    /**
     * Test that compaction should execute silently when there is no entry logs
     * to compact. {@see https://issues.apache.org/jira/browse/BOOKKEEPER-700}
     */
    @Test(timeout = 60000)
    public void testWhenNoLogsToCompact() throws Exception {
        tearDown(); // I dont want the test infrastructure
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);
        conf.setLedgerDirNames(new String[]{tmpDir.toString()});

        LedgerDirsManager dirs = new LedgerDirsManager(conf, conf.getLedgerDirs());
        final Set<Long> ledgers = Collections
                .newSetFromMap(new ConcurrentHashMap<Long, Boolean>());
        ActiveLedgerManager manager = getActiveLedgerManager(ledgers);
        CheckpointSource checkpointSource = new CheckpointSource() {
            @Override
            public Checkpoint newCheckpoint() {
                return null;
            }

            @Override
            public void checkpointComplete(Checkpoint checkpoint, boolean compact)
                    throws IOException {
            }
        };
        InterleavedLedgerStorage storage = new InterleavedLedgerStorage(conf,
                manager, dirs, dirs, checkpointSource, NullStatsLogger.INSTANCE);

        double threshold = 0.1;
        // shouldn't throw exception
        storage.gcThread.doCompactEntryLogs(threshold);
    }

    private ByteBuffer genEntry(long ledger, long entry, int size) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[size]);
        bb.putLong(ledger);
        bb.putLong(entry);
        while (bb.hasRemaining()) {
            bb.put((byte)0xFF);
        }
        bb.flip();
        return bb;
    }
}
