package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
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
        txnDir = File.createTempFile("index-persistence-mgr", "txn");
        txnDir.delete();
        txnDir.mkdir();
        ledgerDir = File.createTempFile("index-persistence-mgr", "ledger");
        ledgerDir.delete();
        ledgerDir.mkdir();
        // create current dir
        new File(txnDir, Bookie.CURRENT_DIR).mkdir();
        new File(ledgerDir, Bookie.CURRENT_DIR).mkdir();

        conf = new ServerConfiguration();
        conf.setZkServers(null);
        conf.setJournalDirName(txnDir.getPath());
        conf.setLedgerDirNames(new String[] { ledgerDir.getPath() });

        ledgerDirsManager = new LatchableLedgerDirsManager(conf, conf.getLedgerDirs());

        ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(conf, null);
        activeLedgerManager = ledgerManagerFactory.newActiveLedgerManager();

        indexPersistenceMgr = new IndexPersistenceMgr(
                conf.getPageSize(), conf.getPageSize() / LedgerEntryPage.getIndexEntrySize(),
                conf, activeLedgerManager, ledgerDirsManager);
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
                conf, activeLedgerManager, ledgerDirsManager);
        assertEquals("write", new String(newMgr.getFileInfo(lid, "fake".getBytes()).getMasterKey(), UTF_8));
        assertEquals("write", new String(newMgr.getFileInfo(lid, null).getMasterKey(), UTF_8));
        newMgr.close();
    }
}
