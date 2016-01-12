package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.bookkeeper.util.BookKeeperConstants.*;
import static org.junit.Assert.*;
import static com.google.common.base.Charsets.UTF_8;

/**
 * {@link org.apache.bookkeeper.bookie.IndexPersistenceMgr} related test cases.
 */
public class IndexPersistenceMgrTest {

    static Logger LOG = LoggerFactory.getLogger(IndexPersistenceMgrTest.class);

    LedgerManagerFactory ledgerManagerFactory;
    ActiveLedgerManager activeLedgerManager;
    LatchableLedgerDirsManager ledgerDirsManager;
    ServerConfiguration conf;
    File txnDir, ledgerDir;

    // index persistence manager
    IndexPersistenceMgr indexPersistenceMgr;

    static final class LatchableLedgerDirsManager extends LedgerDirsManager {

        CountDownLatch latch = new CountDownLatch(0);

        public LatchableLedgerDirsManager(ServerConfiguration conf, File[] dirs) {
            super(conf, dirs);
        }

        public synchronized void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public synchronized CountDownLatch getLatch() {
            return this.latch;
        }

        @Override
        public List<File> getAllLedgerDirs() {
            try {
                getLatch().await();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted on waiting ldm latch : ", e);
            }
            return super.getAllLedgerDirs();
        }
    }

    @Before
    public void setUp() throws Exception {
        txnDir = IOUtils.createTempDir("index-persistence-mgr", "txn");
        ledgerDir = IOUtils.createTempDir("index-persistence-mgr", "ledger");
        // create current dir
        new File(txnDir, CURRENT_DIR).mkdir();
        new File(ledgerDir, CURRENT_DIR).mkdir();

        conf = new ServerConfiguration();
        conf.setZkServers(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        ledgerDirsManager = new LatchableLedgerDirsManager(conf, conf.getLedgerDirs());

        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, null);
        activeLedgerManager = ledgerManagerFactory.newActiveLedgerManager();

        indexPersistenceMgr = new IndexPersistenceMgr(
                conf.getPageSize(), conf.getPageSize() / LedgerEntryPage.getIndexEntrySize(),
                conf, activeLedgerManager, ledgerDirsManager, NullStatsLogger.INSTANCE);
    }

    @After
    public void tearDown() throws Exception {
        indexPersistenceMgr.close();
        activeLedgerManager.close();
        ledgerManagerFactory.uninitialize();
        ledgerDirsManager.shutdown();
        FileUtils.deleteDirectory(txnDir);
        FileUtils.deleteDirectory(ledgerDir);
    }

    @Test(timeout = 60000)
    public void testGetFileInfo() throws Exception {
        final long lid = 1L;
        final CountDownLatch readLatch = new CountDownLatch(1);
        final CountDownLatch readDoneLatch = new CountDownLatch(1);
        final AtomicInteger readRc = new AtomicInteger(-1234);
        final CountDownLatch ldmLatch = new CountDownLatch(1);
        ledgerDirsManager.setLatch(ldmLatch);
        Thread readThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    readLatch.countDown();
                    indexPersistenceMgr.getFileInfo(lid, null);
                    readRc.set(BKException.Code.OK);
                } catch (Bookie.NoLedgerException nle) {
                    // expected
                    readRc.set(BKException.Code.NoSuchLedgerExistsException);
                } catch (IOException e) {
                    LOG.error("Failed to get file info of an unexisted ledger : ", e);
                    readRc.set(BKException.Code.ReadException);
                }
                readDoneLatch.countDown();
            }
        }, "read-thread");
        readThread.start();
        readLatch.await();

        TimeUnit.SECONDS.sleep(2);

        final CountDownLatch writeLatch = new CountDownLatch(1);
        final CountDownLatch writeDoneLatch = new CountDownLatch(1);
        final AtomicInteger writeRc = new AtomicInteger(-1234);
        Thread writeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    writeLatch.countDown();
                    indexPersistenceMgr.getFileInfo(lid, "write".getBytes());
                    writeRc.set(BKException.Code.OK);
                } catch (Bookie.NoLedgerException nle) {
                    writeRc.set(BKException.Code.NoSuchLedgerExistsException);
                } catch (IOException e) {
                    writeRc.set(BKException.Code.WriteException);
                    LOG.error("Failed to get file info with master key : ", e);
                }
                writeDoneLatch.countDown();
            }
        });
        writeThread.start();
        writeLatch.await();

        // wait for a while until write thread executed #getFileInfo
        // which it would pending for read thread #getFileInfo to load cache
        TimeUnit.SECONDS.sleep(2);

        // notify ledgers directory manager
        ldmLatch.countDown();

        writeDoneLatch.await();
        writeThread.join();
        readDoneLatch.await();
        readThread.join();

        assertEquals(BKException.Code.OK, writeRc.get());
        assertEquals(BKException.Code.NoSuchLedgerExistsException, readRc.get());
        assertEquals("write", new String(indexPersistenceMgr.getFileInfo(lid, null).getMasterKey(), UTF_8));

        indexPersistenceMgr.getFileInfo(lid, null).checkOpen(true);

        IndexPersistenceMgr newMgr = new IndexPersistenceMgr(
                conf.getPageSize(), conf.getPageSize() / LedgerEntryPage.getIndexEntrySize(),
                conf, activeLedgerManager, ledgerDirsManager, NullStatsLogger.INSTANCE);
        assertEquals("write", new String(newMgr.getFileInfo(lid, "fake".getBytes()).getMasterKey(), UTF_8));
        assertEquals("write", new String(newMgr.getFileInfo(lid, null).getMasterKey(), UTF_8));
        newMgr.close();
    }

    IndexPersistenceMgr getPersistenceManager(int cacheSize) throws Exception {

        ServerConfiguration localConf = new ServerConfiguration();
        localConf.addConfiguration(this.conf);
        localConf.setOpenFileLimit(cacheSize);

        return new IndexPersistenceMgr(
            localConf.getPageSize(), localConf.getPageSize() / LedgerEntryPage.getIndexEntrySize(),
            localConf, activeLedgerManager, ledgerDirsManager, NullStatsLogger.INSTANCE);
    }

    void fillCache(IndexPersistenceMgr indexPersistenceMgr, int numEntries) throws Exception {
        for (long i = 0; i < numEntries; i++) {
            indexPersistenceMgr.getFileInfo(i, masterKey);
        }
    }

    final long lid = 1L;
    final byte[] masterKey = "write".getBytes();

    @Test(timeout = 60000)
    public void testGetFileInfoEvictPreExistingFile() throws Exception {

        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = getPersistenceManager(10);

            // get file info and make sure the underlying file exists
            FileInfo fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            fi.checkOpen(true);
            fi.setFenced();

            // force evict by filling up cache
            fillCache(indexPersistenceMgr, 20);

            // now reload the file info from disk, state should have been flushed
            fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(true, fi.isFenced());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testGetFileInfoEvictNewFile() throws Exception {

        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = getPersistenceManager(10);

            // get file info, but don't persist metadata right away
            FileInfo fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            fi.setFenced();
            fillCache(indexPersistenceMgr, 20);
            fi = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(true, fi.isFenced());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testGetFileInfoReadBeforeWrite() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = getPersistenceManager(1);
            // get the file info for read
            try {
                indexPersistenceMgr.getFileInfo(lid, null);
                fail("Should fail get file info for reading if the file doesn't exist");
            } catch (Bookie.NoLedgerException nle) {
                // exepcted
            }
            assertEquals(0, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(0, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(0, indexPersistenceMgr.fileInfoMap.size());

            FileInfo writeFileInfo = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(1, writeFileInfo.getUseCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.fileInfoMap.size());

            IndexPersistenceMgr.RefFileInfo fiRef = indexPersistenceMgr.fileInfoMap.get(lid);
            assertNotNull(fiRef);
            assertEquals(2, fiRef.getCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testGetFileInfoWriteBeforeRead() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = getPersistenceManager(1);

            FileInfo writeFileInfo = indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(1, writeFileInfo.getUseCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.fileInfoMap.size());

            IndexPersistenceMgr.RefFileInfo fiRef = indexPersistenceMgr.fileInfoMap.get(lid);
            assertNotNull(fiRef);
            assertEquals(2, fiRef.getCount());

            FileInfo readFileInfo = indexPersistenceMgr.getFileInfo(lid, null);
            assertEquals(2, readFileInfo.getUseCount());
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(1, indexPersistenceMgr.fileInfoMap.size());

            fiRef = indexPersistenceMgr.fileInfoMap.get(lid);
            assertNotNull(fiRef);
            assertEquals(2, fiRef.getCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }

    @Test(timeout = 60000)
    public void testReadFileInfoCacheEviction() throws Exception {
        IndexPersistenceMgr indexPersistenceMgr = null;
        try {
            indexPersistenceMgr = getPersistenceManager(1);

            for (int i = 0; i < 3; i++) {
                indexPersistenceMgr.getFileInfo(lid+i, masterKey);
            }

            indexPersistenceMgr.getFileInfo(lid, masterKey);
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(2, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(2, indexPersistenceMgr.fileInfoMap.size());

            // trigger file info eviction on read file info cache
            for (int i = 1; i <= 2; i++) {
                indexPersistenceMgr.getFileInfo(lid + i, null);
            }
            assertEquals(1, indexPersistenceMgr.writeFileInfoCache.size());
            assertEquals(2, indexPersistenceMgr.readFileInfoCache.size());
            assertEquals(3, indexPersistenceMgr.fileInfoMap.size());

            IndexPersistenceMgr.RefFileInfo fiRef = indexPersistenceMgr.fileInfoMap.get(lid);
            assertNotNull(fiRef);
            assertEquals(1, fiRef.getCount());
            fiRef = indexPersistenceMgr.fileInfoMap.get(lid+1);
            assertNotNull(fiRef);
            assertEquals(1, fiRef.getCount());
            fiRef = indexPersistenceMgr.fileInfoMap.get(lid+2);
            assertNotNull(fiRef);
            assertEquals(1, fiRef.getCount());
        } finally {
            if (null != indexPersistenceMgr) {
                indexPersistenceMgr.close();
            }
        }
    }
}
