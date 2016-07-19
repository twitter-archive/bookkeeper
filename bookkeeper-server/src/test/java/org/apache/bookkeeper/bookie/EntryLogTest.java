/**
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Optional;
import org.apache.bookkeeper.bookie.EntryLogMetadataManager.EntryLogMetadata;
import org.apache.bookkeeper.bookie.EntryLogger.ExtractionScanner;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public class EntryLogTest {
    static Logger LOG = LoggerFactory.getLogger(EntryLogTest.class);

    final List<File> tempDirs = new ArrayList<File>();

    File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix);
        tempDirs.add(dir);
        return dir;
    }

    @After
    public void tearDown() throws Exception {
        for (File dir : tempDirs) {
            FileUtils.deleteDirectory(dir);
        }
        tempDirs.clear();
    }

    private boolean verifyData(byte[] src, byte[] dst, int pos, int cap) {
        for (int i = pos; i < pos + cap; i++) {
            if (src[i] != dst[i - pos]) {
                return false;
            }
        }
        return true;
    }

    private static Bookie newBookie(ServerConfiguration conf) throws Exception {
        Bookie b = new Bookie(conf);
        b.initialize();
        return b;
    }

    @Test(timeout = 60000)
    public void testBufferedReadChannel() throws Exception {
        File tmpFile = IOUtils.createTempFileAndDeleteOnExit("bufferedReadTest", ".tmp");
        RandomAccessFile raf = null;
        BufferedChannel bc = new BufferedChannel((raf = new RandomAccessFile(tmpFile, "rw")).getChannel(), 64);
        final int cap = 2048;
        byte[] src = new byte[cap];
        byte [] dst = new byte[cap];
        ByteBuffer dstBuff = null;
        // Populate the file
        for (int i = 0; i < cap; i++) {
            src[i] = (byte)(i % Byte.MAX_VALUE);
        }
        bc.write(ByteBuffer.wrap(src));
        bc.flush(true);

        // Now read and verify everything works.
        BufferedReadChannel brc = new BufferedReadChannel(new RandomAccessFile(tmpFile, "r").getChannel(), 64);
        // Verify that we wrote properly.
        assertTrue(brc.size() == cap);

        // This should read all the data
        dst = new byte[cap];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        brc.read(dstBuff, 0);
        assertTrue(verifyData(src, dst, 0, dst.length));

        // Read only the last byte cap-1
        dst = new byte[1];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        brc.read(dstBuff, cap-1);
        assertTrue(verifyData(src, dst, cap-1, dst.length));

        // Read some data, then read again with an overlap. Both reads should be smaller than read channel
        // capacity/2 so that the second read is served from the buffer.
        dst = new byte[16];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        brc.read(dstBuff, 50);
        assertTrue(verifyData(src, dst, 50, dst.length));
        dst = new byte[16];
        dstBuff = ByteBuffer.wrap(dst);
        brc.read(dstBuff, 64);
        assertTrue(verifyData(src, dst, 64, dst.length));

        // Read data that partially overlaps
        dst = new byte[100];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        brc.read(dstBuff, 500);
        assertTrue(verifyData(src, dst, 500, dst.length));
        dst = new byte[200];
        dstBuff = ByteBuffer.wrap(dst);
        brc.read(dstBuff, 580);
        assertTrue(verifyData(src, dst, 580, dst.length));

        // Read from the end of the file such that the readBuffer hits EOF
        dst = new byte[16];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        brc.read(dstBuff, cap - 16);
        assertTrue(verifyData(src, dst, cap-16, dst.length));

        // Read from a position beyond the end of the file.
        dst = new byte[100];
        dstBuff = ByteBuffer.wrap(dst);
        brc.clear();
        assertEquals(50, brc.read(dstBuff, cap - 50));
    }

    @Test(timeout = 60000)
    public void testCorruptEntryLog() throws Exception {
        extractEntryLogMetadataFromCorruptedLog(false);
    }

    @Test(timeout = 60000)
    public void testGcExtractEntryLogMetadataFromCorruptedLog() throws Exception {
        extractEntryLogMetadataFromCorruptedLog(true);
    }

    private void extractEntryLogMetadataFromCorruptedLog(boolean useGCExtracter) throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = newBookie(conf);
        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage)bookie.ledgerStorage).entryLogger;
        logger.addEntry(1L, generateEntry(1, 1));
        logger.addEntry(3L, generateEntry(3, 1));
        logger.addEntry(2L, generateEntry(2, 1));
        logger.flush();
        // now lets truncate the file to corrupt the last entry, which simulates a partial write
        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        long lenNew = raf.length() - 10;
        raf.setLength(lenNew);
        raf.close();
        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        EntryLogMetadata meta = new EntryLogMetadata(0L);
        if (useGCExtracter) {
            // it should scan short-read entry log successfully.
            meta = logger.extractEntryLogMetadata(0);
        } else {
            ExtractionScanner scanner = new ExtractionScanner(meta);
            try {
                logger.scanEntryLog(0L, scanner);
                fail("Should not reach here!");
            } catch (IOException ie) {
            }
            LOG.info("Extracted Meta From Entry Log {}", meta);
        }
        assertNotNull(meta.ledgersMap.get(1L));
        assertNull(meta.ledgersMap.get(2L));
        assertNotNull(meta.ledgersMap.get(3L));
    }

    private ByteBuffer generateEntry(long ledger, long entry) {
        byte[] data = ("ledger-" + ledger + "-" + entry).getBytes();
        ByteBuffer bb = ByteBuffer.wrap(new byte[8 + 8 + data.length]);
        bb.putLong(ledger);
        bb.putLong(entry);
        bb.put(data);
        bb.flip();
        return bb;
    }

    @Test(timeout = 60000)
    public void testInitializeEntryLogNoWritableDirs() throws Exception {
        File tmpLedgerDir = createTempDir("initializeEntryLogNoWritableDirs", "ledgers");
        File curLedgerDir = Bookie.getCurrentDirectory(tmpLedgerDir);
        Bookie.checkDirectoryStructure(curLedgerDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpLedgerDir.toString() });

        LedgerDirsManager dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());
        dirsManager.addToFilledDirs(curLedgerDir);

        try {
            new EntryLogger(conf, dirsManager);
            fail("Should fail initialize entry logger if there isn't writable dirs");
        } catch (IOException ioe) {
            // expected
        }
    }

    static class TestEntryLogger extends EntryLogger {

        public TestEntryLogger(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager)
                throws IOException {
            super(conf, ledgerDirsManager);
        }

        @Override
        protected void setLastLogId(File dir, long logId) throws IOException {
            throw new IOException("Failed to write log id " + logId + " under dir " + dir);
        }
    }

    @Test(timeout = 60000)
    public void testWriteLogIdFailure() throws Exception {
        File tmpLedgerDir = createTempDir("writeLogIdFailure", "ledgers");
        File curLedgerDir = Bookie.getCurrentDirectory(tmpLedgerDir);
        Bookie.checkDirectoryStructure(curLedgerDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpLedgerDir.toString() });

        LedgerDirsManager dirsManager = new LedgerDirsManager(conf, conf.getLedgerDirs());
        // create logs
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2*numLogs][];
        for (int i = 0; i < numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger entryLogger = new TestEntryLogger(conf, dirsManager);
            for (int j = 0; j < numEntries; j++) {
                positions[i][j] = entryLogger.addEntry(i, generateEntry(i, j));
            }
            entryLogger.flush();
            entryLogger.shutdown();
        }

        EntryLogger newLogger = new TestEntryLogger(conf, dirsManager);
        for (int i = 0; i < numLogs + 1; i++) {
            File logFile = new File(curLedgerDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }

        for (int i=0; i<numLogs; i++) {
            for (int j=0; j<numEntries; j++) {
                String expectedValue = "ledger-" + i + "-" + j;
                byte[] value = newLogger.readEntry(i, j, positions[i][j]);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long ledgerId = buf.getLong();
                long entryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                assertEquals(i, ledgerId);
                assertEquals(j, entryId);
                assertEquals(expectedValue, new String(data));
            }
        }

        File lastId = new File(curLedgerDir, "lastId");
        assertFalse(lastId.exists());
    }

    @Test(timeout = 60000)
    public void testMissingLogId() throws Exception {
        File tmpDir = createTempDir("entryLogTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = newBookie(conf);
        // create some entries
        int numLogs = 3;
        int numEntries = 10;
        long[][] positions = new long[2*numLogs][];
        for (int i=0; i<numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j=0; j<numEntries; j++) {
                positions[i][j] = logger.addEntry(i, generateEntry(i, j));
            }
            logger.flush();
        }
        // delete last log id
        File lastLogId = new File(curDir, "lastId");
        lastLogId.delete();

        // write another entries
        for (int i=numLogs; i<2*numLogs; i++) {
            positions[i] = new long[numEntries];

            EntryLogger logger = new EntryLogger(conf,
                    bookie.getLedgerDirsManager());
            for (int j=0; j<numEntries; j++) {
                positions[i][j] = logger.addEntry(i, generateEntry(i, j));
            }
            logger.flush();
        }

        EntryLogger newLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        for (int i=0; i<(2*numLogs+1); i++) {
            File logFile = new File(curDir, Long.toHexString(i) + ".log");
            assertTrue(logFile.exists());
        }
        for (int i=0; i<2*numLogs; i++) {
            for (int j=0; j<numEntries; j++) {
                String expectedValue = "ledger-" + i + "-" + j;
                byte[] value = newLogger.readEntry(i, j, positions[i][j]);
                ByteBuffer buf = ByteBuffer.wrap(value);
                long ledgerId = buf.getLong();
                long entryId = buf.getLong();
                byte[] data = new byte[buf.remaining()];
                buf.get(data);
                assertEquals(i, ledgerId);
                assertEquals(j, entryId);
                assertEquals(expectedValue, new String(data));
            }
        }
    }

    @Test(timeout = 60000)
    /** Test that EntryLogger Should fail with FNFE, if entry logger directories does not exist*/
    public void testEntryLoggerShouldThrowFNFEIfDirectoriesDoesNotExist()
            throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        EntryLogger entryLogger = null;
        try {
            entryLogger = new EntryLogger(conf, new LedgerDirsManager(conf, conf.getLedgerDirs()));
            fail("Expecting FileNotFoundException");
        } catch (FileNotFoundException e) {
            assertEquals("Entry log directory does not exist", e
                    .getLocalizedMessage());
        } finally {
            if (entryLogger != null) {
                entryLogger.shutdown();
            }
        }
    }

    @Test(timeout = 60000, expected = Bookie.NoEntryException.class)
    public void testFileNotFoundOnReadingEntries() throws Exception {
        File ledgerDir = createTempDir("EntryLogTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir.getAbsolutePath() });
        Bookie bookie = newBookie(conf);
        EntryLogger entryLogger = new EntryLogger(conf, bookie.getLedgerDirsManager());
        long pos = (9999L << 32L) | 9999L;
        entryLogger.readEntry(1L, 0L, pos);
    }

    /**
     * Test to verify the DiskFull during addEntry
     */
    @Test(timeout = 60000)
    public void testAddEntryFailureOnDiskFull() throws Exception {
        File ledgerDir1 = createTempDir("bkTest", ".dir");
        File ledgerDir2 = createTempDir("bkTest", ".dir");
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setLedgerDirNames(new String[] { ledgerDir1.getAbsolutePath(),
                ledgerDir2.getAbsolutePath() });
        Bookie bookie = newBookie(conf);
        EntryLogger entryLogger = new EntryLogger(conf,
                bookie.getLedgerDirsManager());
        InterleavedLedgerStorage ledgerStorage = ((InterleavedLedgerStorage) bookie.ledgerStorage);
        ledgerStorage.entryLogger = entryLogger;
        // Create ledgers
        ledgerStorage.setMasterKey(1, "key".getBytes());
        ledgerStorage.setMasterKey(2, "key".getBytes());
        ledgerStorage.setMasterKey(3, "key".getBytes());
        // Add entries
        ledgerStorage.addEntry(generateEntry(1, 1));
        ledgerStorage.addEntry(generateEntry(2, 1));
        ledgerStorage.flush();
        // Add entry with disk full failure simulation
        bookie.getLedgerDirsManager().addToFilledDirs(entryLogger.currentDir);
        ledgerStorage.addEntry(generateEntry(3, 1));
        ledgerStorage.flush();
        // Verify written entries
        Assert.assertTrue(0 == generateEntry(1, 1).compareTo(ledgerStorage.getEntry(1, 1)));
        Assert.assertTrue(0 == generateEntry(2, 1).compareTo(ledgerStorage.getEntry(2, 1)));
        Assert.assertTrue(0 == generateEntry(3, 1).compareTo(ledgerStorage.getEntry(3, 1)));
    }

    @Test(timeout=60000)
    public void testGetLedgersMapFromIndex() throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] {tmpDir.toString()});
        Bookie bookie = new Bookie(conf);
        bookie.initialize();

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage)bookie.ledgerStorage).entryLogger;
        logger.addEntry(1, generateEntry(1, 1));
        logger.addEntry(3, generateEntry(3, 1));
        logger.addEntry(2, generateEntry(2, 1));
        logger.addEntry(1, generateEntry(1, 2));
        logger.rollLog();
        logger.flush();

        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());
        Optional<EntryLogMetadata> metaOptional = logger.extractEntryLogMetadataFromIndex(0L);
        assertTrue(metaOptional.isPresent());
        EntryLogMetadata meta = metaOptional.get();
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.ledgersMap.get(1L).longValue());
        assertEquals(30, meta.ledgersMap.get(2L).longValue());
        assertEquals(30, meta.ledgersMap.get(3L).longValue());
        assertNull(meta.ledgersMap.get(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

    @Test(timeout = 60000)
    public void testGetLedgersMapOnV0EntryLog() throws Exception {
        testGetLedgersMap(true, false, false, false, false);
    }

    @Test(timeout = 60000)
    public void testGetLedgersMapCorruptedOffset() throws Exception {
        testGetLedgersMap(false, true, false, false, false);
    }

    @Test(timeout = 60000)
    public void testGetLedgersMapCorruptedTotalLedgers() throws Exception {
        testGetLedgersMap(false, false, true, false, false);
    }

    @Test(timeout = 60000)
    public void testGetLedgersMapCorruptedLedgersMapEntry() throws Exception {
        testGetLedgersMap(false, false, false, true, false);
    }

    @Test(timeout = 60000)
    public void testGetLedgersMapMissingLedgersMapEntry() throws Exception {
        testGetLedgersMap(false, false, false, false, true);
    }

    private void testGetLedgersMap(boolean writeAsV0Log,
                                   boolean corruptedOffset,
                                   boolean corruptedTotalLedgers,
                                   boolean corruptedLedgersMapEntry,
                                   boolean missingLedgersMapEntry) throws Exception {
        File tmpDir = createTempDir("bkTest", ".dir");
        File curDir = Bookie.getCurrentDirectory(tmpDir);
        Bookie.checkDirectoryStructure(curDir);

        int gcWaitTime = 1000;
        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        conf.setGcWaitTime(gcWaitTime);
        conf.setLedgerDirNames(new String[] { tmpDir.toString() });
        Bookie bookie = new Bookie(conf);
        bookie.initialize();

        // create some entries
        EntryLogger logger = ((InterleavedLedgerStorage) bookie.ledgerStorage).entryLogger;
        logger.addEntry(1, generateEntry(1, 1));
        logger.addEntry(3, generateEntry(3, 1));
        logger.addEntry(2, generateEntry(2, 1));
        logger.addEntry(1, generateEntry(1, 2));
        logger.rollLog();
        logger.flush();

        // Rewrite the entry log header to be on V0 format

        File f = new File(curDir, "0.log");
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        try {
            if (writeAsV0Log) {
                raf.seek(0L);
                // rewrite the header to v0
                ByteBuffer buf = ByteBuffer.allocate(EntryLogger.LOGFILE_HEADER_LENGTH);
                buf.put(EntryLogger.MAGIC_BYTES);
                raf.write(buf.array());
            } else {
                if (corruptedLedgersMapEntry) {
                    raf.setLength(raf.length() - 6);
                } else if (missingLedgersMapEntry) {
                    raf.setLength(raf.length() - 16);
                } else if (corruptedTotalLedgers) {
                    ByteBuffer buf = ByteBuffer.allocate(4);
                    buf.putInt(99999);
                    raf.seek(EntryLogger.HEADER_VERSION_LENGTH + 8);
                    raf.write(buf.array());
                } else if (corruptedOffset) {
                    ByteBuffer buf = ByteBuffer.allocate(8);
                    buf.putLong(999999);
                    raf.seek(EntryLogger.HEADER_VERSION_LENGTH);
                    raf.write(buf.array());
                }
            }
        } finally {
            raf.close();
        }

        // now see which ledgers are in the log
        logger = new EntryLogger(conf, bookie.getLedgerDirsManager());

        assertFalse(logger.extractEntryLogMetadataFromIndex(0L).isPresent());

        EntryLogMetadata meta = logger.extractEntryLogMetadata(0L);
        LOG.info("Extracted Meta From Entry Log {}", meta);
        assertEquals(60, meta.ledgersMap.get(1L).longValue());
        assertEquals(30, meta.ledgersMap.get(2L).longValue());
        assertEquals(30, meta.ledgersMap.get(3L).longValue());
        assertNull(meta.ledgersMap.get(4L));
        assertEquals(120, meta.getTotalSize());
        assertEquals(120, meta.getRemainingSize());
    }

}
