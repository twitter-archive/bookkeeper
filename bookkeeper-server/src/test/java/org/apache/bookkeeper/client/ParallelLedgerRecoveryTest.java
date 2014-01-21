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

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.meta.TimedLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelLedgerRecoveryTest extends BookKeeperClusterTestCase {

    static final Logger LOG = LoggerFactory.getLogger(ParallelLedgerRecoveryTest.class);

    static class TestLedgerManager implements LedgerManager {

        final LedgerManager lm;
        volatile CountDownLatch waitLatch = null;
        final ExecutorService executorService;

        TestLedgerManager(LedgerManager lm) {
            this.lm = lm;
            this.executorService = Executors.newSingleThreadExecutor();
        }

        void setLatch(CountDownLatch waitLatch) {
            this.waitLatch = waitLatch;
        }

        @Override
        public void createLedger(LedgerMetadata metadata, GenericCallback<Long> cb) {
            lm.createLedger(metadata, cb);
        }

        @Override
        public void deleteLedger(long ledgerId, GenericCallback<Void> cb) {
            lm.deleteLedger(ledgerId, cb);
        }

        @Override
        public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {
            lm.readLedgerMetadata(ledgerId, readCb);
        }

        @Override
        public void writeLedgerMetadata(final long ledgerId, final LedgerMetadata metadata,
                                        final GenericCallback<Void> cb) {
            final CountDownLatch cdl = waitLatch;
            if (null != cdl) {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cdl.await();
                        } catch (InterruptedException e) {
                            LOG.error("Interrupted on waiting latch : ", e);
                        }
                        lm.writeLedgerMetadata(ledgerId, metadata, cb);
                    }
                });
            } else {
                lm.writeLedgerMetadata(ledgerId, metadata, cb);
            }
        }

        @Override
        public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            lm.registerLedgerMetadataListener(ledgerId, listener);
        }

        @Override
        public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
            lm.unregisterLedgerMetadataListener(ledgerId, listener);
        }

        @Override
        public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context,
                                        int successRc, int failureRc) {
            lm.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
        }

        @Override
        public void close() throws IOException {
            lm.close();
            executorService.shutdown();
        }
    }

    static class TestLedgerManagerFactory extends HierarchicalLedgerManagerFactory {
        @Override
        public LedgerManager newLedgerManager() {
            return new TestLedgerManager(super.newLedgerManager());
        }
    }

    final DigestType digestType;

    public ParallelLedgerRecoveryTest() {
        super(3);
        baseConf.setLedgerManagerFactoryClass(TestLedgerManagerFactory.class);
        baseClientConf.setLedgerManagerFactoryClass(TestLedgerManagerFactory.class);
        baseClientConf.setReadTimeout(60000);
        baseClientConf.setReadEntryTimeout(60000);
        baseClientConf.setAddEntryTimeout(60000);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testRecoverBeforeWriteMetadata1() throws Exception {
        rereadDuringRecovery(true, 1, false, false);
    }

    @Test(timeout = 60000)
    public void testRecoverBeforeWriteMetadata2() throws Exception {
        rereadDuringRecovery(true, 3, false, false);
    }

    @Test(timeout = 60000)
    public void testRecoverBeforeWriteMetadata3() throws Exception {
        rereadDuringRecovery(false, 1, false, false);
    }

    @Test(timeout = 60000)
    public void testRecoverBeforeWriteMetadata4() throws Exception {
        rereadDuringRecovery(false, 3, false, false);
    }

    @Test(timeout = 60000)
    public void testRereadDuringRecovery1() throws Exception {
        rereadDuringRecovery(true, 1, true, false);
    }

    @Test(timeout = 60000)
    public void testRereadDuringRecovery2() throws Exception {
        rereadDuringRecovery(true, 3, true, false);
    }

    @Test(timeout = 60000)
    public void testRereadDuringRecovery3() throws Exception {
        rereadDuringRecovery(false, 1, true, false);
    }

    @Test(timeout = 60000)
    public void testRereadDuringRecovery4() throws Exception {
        rereadDuringRecovery(false, 3, true, false);
    }

    @Test(timeout = 60000)
    public void testConcurrentRecovery1() throws Exception {
        rereadDuringRecovery(true, 1, true, false);
    }

    @Test(timeout = 60000)
    public void testConcurrentRecovery2() throws Exception {
        rereadDuringRecovery(true, 3, true, false);
    }

    @Test(timeout = 60000)
    public void testConcurrentRecovery3() throws Exception {
        rereadDuringRecovery(false, 1, true, false);
    }

    @Test(timeout = 60000)
    public void testConcurrentRecovery4() throws Exception {
        rereadDuringRecovery(false, 3, true, false);
    }

    private void rereadDuringRecovery(boolean parallelRead, int batchSize,
                                      boolean updateMetadata, boolean close) throws Exception {
        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setEnableParallelRecoveryRead(parallelRead);
        newConf.setRecoveryReadBatchSize(batchSize);
        BookKeeper newBk = new BookKeeper(newConf);

        TestLedgerManager tlm =
                (TestLedgerManager) (((TimedLedgerManager) newBk.getLedgerManager()).getUnderlying());

        final LedgerHandle lh = newBk.createLedger(numBookies, 2, 2, digestType, "".getBytes());
        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        sleepBookie(lh.getLedgerMetadata().currentEnsemble.get(0), latch1);
        sleepBookie(lh.getLedgerMetadata().currentEnsemble.get(1), latch2);

        int numEntries = (numBookies * 3) + 1;
        final AtomicInteger numPendingAdds = new AtomicInteger(numEntries);
        final CountDownLatch addDone = new CountDownLatch(1);
        for (int i = 0; i < numEntries; i++) {
            lh.asyncAddEntry(("" + i).getBytes(), new org.apache.bookkeeper.client.AsyncCallback.AddCallback() {
                @Override
                public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
                    if (BKException.Code.OK != rc) {
                        addDone.countDown();
                        return;
                    }
                    if (numPendingAdds.decrementAndGet() == 0) {
                        addDone.countDown();
                    }
                }
            }, null);
        }
        latch1.countDown();
        latch2.countDown();
        addDone.await(10, TimeUnit.SECONDS);
        assertEquals(0, numPendingAdds.get());

        LOG.info("Added {} entries to ledger {}.", numEntries, lh.getId());

        long ledgerLenth = lh.getLength();

        LedgerHandle recoverLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, recoverLh.getLastAddPushed());
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, recoverLh.getLastAddConfirmed());
        assertEquals(0, recoverLh.getLength());

        LOG.info("OpenLedgerNoRecovery {}.", lh.getId());

        final CountDownLatch metadataLatch = new CountDownLatch(1);

        tlm.setLatch(metadataLatch);

        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        recoverLh.recover(new GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                LOG.info("Recovering ledger {} completed : {}.", lh.getId(), rc);
                success.set(BKException.Code.OK == rc);
                recoverLatch.countDown();
            }
        });

        while ((numEntries - 1) != recoverLh.getLastAddPushed()) {
            Thread.sleep(1000);
        }

        assertEquals(numEntries - 1, recoverLh.getLastAddPushed());
        assertEquals(ledgerLenth, recoverLh.getLength());
        assertFalse(recoverLh.getLedgerMetadata().isClosed());

        // clear the metadata latch
        tlm.setLatch(null);

        if (updateMetadata) {
            if (close) {
                LOG.info("OpenLedger {} to close.", lh.getId());
                LedgerHandle newRecoverLh = newBk.openLedger(lh.getId(), digestType, "".getBytes());
                newRecoverLh.close();
            } else {
                LOG.info("OpenLedgerNoRecovery {} again.", lh.getId());
                LedgerHandle newRecoverLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
                assertEquals(BookieProtocol.INVALID_ENTRY_ID, newRecoverLh.getLastAddPushed());
                assertEquals(BookieProtocol.INVALID_ENTRY_ID, newRecoverLh.getLastAddConfirmed());
                // mark the ledger as in recovery to update version.
                newRecoverLh.getLedgerMetadata().markLedgerInRecovery();
                final CountDownLatch updateLatch = new CountDownLatch(1);
                final AtomicInteger updateResult = new AtomicInteger(0x12345);
                newRecoverLh.writeLedgerConfig(new GenericCallback<Void>() {
                    @Override
                    public void operationComplete(int rc, Void result) {
                        updateResult.set(rc);
                        updateLatch.countDown();
                    }
                });
                updateLatch.await();
                assertEquals(BKException.Code.OK, updateResult.get());
                newRecoverLh.close();
                LOG.info("Updated ledger manager {}.", newRecoverLh.getLedgerMetadata());
            }
        }

        // resume metadata operation on recoverLh
        metadataLatch.countDown();

        LOG.info("Resume metadata update.");

        // wait until recover completed
        recoverLatch.await(20, TimeUnit.SECONDS);
        assertTrue(success.get());
        assertEquals(numEntries - 1, recoverLh.getLastAddPushed());
        assertEquals(numEntries - 1, recoverLh.getLastAddConfirmed());
        assertEquals(ledgerLenth, recoverLh.getLength());
        assertTrue(recoverLh.getLedgerMetadata().isClosed());

        Enumeration<LedgerEntry> enumeration = recoverLh.readEntries(0, numEntries - 1);
        int numReads = 0;
        while (enumeration.hasMoreElements()) {
            LedgerEntry entry = enumeration.nextElement();
            assertEquals((long) numReads, entry.getEntryId());
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        recoverLh.close();
        newBk.close();
    }

}
