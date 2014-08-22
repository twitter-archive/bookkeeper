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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;

/**
 * This unit test tests ledger recovery.
 */
public class LedgerRecoveryTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(LedgerRecoveryTest.class);

    DigestType digestType;

    public LedgerRecoveryTest() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    private void testInternal(int numEntries) throws Exception {
        /*
         * Create ledger.
         */
        LedgerHandle beforelh = null;
        beforelh = bkc.createLedger(digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        long length = (long) (numEntries * tmp.length());

        /*
         * Try to open ledger.
         */
        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());

        /*
         * Check if has recovered properly.
         */
        assertTrue("Has not recovered correctly: " + afterlh.getLastAddConfirmed(),
                   afterlh.getLastAddConfirmed() == numEntries - 1);
        assertTrue("Has not set the length correctly: " + afterlh.getLength() + ", " + length,
                   afterlh.getLength() == length);
    }

    @Test
    public void testLedgerRecovery() throws Exception {
        testInternal(100);
    }

    @Test
    public void testEmptyLedgerRecoveryOne() throws Exception {
        testInternal(1);
    }

    @Test
    public void testEmptyLedgerRecovery() throws Exception {
        testInternal(0);
    }

    @Test
    public void testLedgerRecoveryWithWrongPassword() throws Exception {
        // Create a ledger
        byte[] ledgerPassword = "aaaa".getBytes();
        LedgerHandle lh = bkc.createLedger(digestType, ledgerPassword);
        // bkc.initMessageDigest("SHA1");
        long ledgerId = lh.getId();
        LOG.info("Ledger ID: " + lh.getId());
        String tmp = "BookKeeper is cool!";
        int numEntries = 30;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(tmp.getBytes());
        }

        // Using wrong password
        ledgerPassword = "bbbb".getBytes();
        try {
            lh = bkc.openLedger(ledgerId, digestType, ledgerPassword);
            fail("Opening ledger with wrong password should fail");
        } catch (BKException e) {
            // should failed
        }
    }

    @Test
    public void testLedgerRecoveryWithNotEnoughBookies() throws Exception {
        int numEntries = 3;

        // Create a ledger
        LedgerHandle beforelh = null;
        beforelh = bkc.createLedger(3, 3, digestType, "".getBytes());

        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        // shutdown first bookie server
        bs.get(0).shutdown();
        bs.remove(0);

        /*
         * Try to open ledger.
         */
        try {
            bkc.openLedger(beforelh.getId(), digestType, "".getBytes());
            fail("should not reach here!");
        } catch (Exception e) {
            // should thrown recovery exception
        }

        // start a new bookie server
        startNewBookie();

        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());

        /*
         * Check if has recovered properly.
         */
        assertEquals(numEntries - 1, afterlh.getLastAddConfirmed());
    }

    @Test
    public void testLedgerRecoveryWithSlowBookie() throws Exception {
        for (int i = 0; i < 3; i++) {
            LOG.info("TestLedgerRecoveryWithAckQuorum @ slow bookie {}", i);
            ledgerRecoveryWithSlowBookie(3, 3, 2, 1, i);
        }
    }

    private void ledgerRecoveryWithSlowBookie(int ensembleSize, int writeQuorumSize,
        int ackQuorumSize, int numEntries, int slowBookieIdx) throws Exception {

        // Create a ledger
        LedgerHandle beforelh = null;
        beforelh = bkc.createLedger(ensembleSize, writeQuorumSize, ackQuorumSize,
                                    digestType, "".getBytes());

        // kill first bookie server to start a fake one to simulate a slow bookie
        // and failed to add entry on crash
        // until write succeed
        InetSocketAddress host = beforelh.getLedgerMetadata().currentEnsemble.get(slowBookieIdx);
        ServerConfiguration conf = killBookie(host);

        Bookie fakeBookie = new Bookie(conf) {
            @Override
            public void addEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                // drop request to simulate a slow and failed bookie
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, fakeBookie));

        // avoid not-enough-bookies case
        startNewBookie();

        // write would still succeed with 2 bookies ack
        String tmp = "BookKeeper is cool!";
        for (int i = 0; i < numEntries; i++) {
            beforelh.addEntry(tmp.getBytes());
        }

        conf = killBookie(host);
        bsConfs.add(conf);
        // the bookie goes normally
        bs.add(startBookie(conf));

        /*
         * Try to open ledger.
         */
        LedgerHandle afterlh = bkc.openLedger(beforelh.getId(), digestType, "".getBytes());

        /*
         * Check if has recovered properly.
         */
        assertEquals(numEntries - 1, afterlh.getLastAddConfirmed());
    }

    /**
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-355}
     * A recovery during a rolling restart shouldn't affect the ability
     * to recovery the ledger later.
     * We have a ledger on ensemble B1,B2,B3.
     * The sequence of events is
     * 1. B1 brought down for maintenance
     * 2. Ledger recovery started
     * 3. B2 answers read last confirmed.
     * 4. B1 replaced in ensemble by B4
     * 5. Write to B4 fails for some reason
     * 6. B1 comes back up.
     * 7. B2 goes down for maintenance.
     * 8. Ledger recovery starts (ledger is now unavailable)
     */
    @Test(timeout=60000)
    public void testLedgerRecoveryWithRollingRestart() throws Exception {
        LedgerHandle lhbefore = bkc.createLedger(numBookies, 2, digestType, "".getBytes());
        for (int i = 0; i < (numBookies*3)+1; i++) {
            lhbefore.addEntry("data".getBytes());
        }

        // Add a dead bookie to the cluster
        ServerConfiguration conf = newServerConfiguration();
        Bookie deadBookie1 = new Bookie(conf) {
            @Override
            public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                // drop request to simulate a slow and failed bookie
                throw new IOException("Couldn't write for some reason");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, deadBookie1));

        // kill first bookie server
        InetSocketAddress bookie1 = lhbefore.getLedgerMetadata().currentEnsemble.get(0);
        ServerConfiguration conf1 = killBookie(bookie1);

        // Try to recover and fence the ledger after killing one bookie in the
        // ensemble in the ensemble, and another bookie is available in zk, but not writtable
        try {
            bkc.openLedger(lhbefore.getId(), digestType, "".getBytes());
            fail("Shouldn't be able to open ledger, there should be entries missing");
        } catch (BKException.BKLedgerRecoveryException e) {
            // expected
        }

        // restart the first server, kill the second
        bsConfs.add(conf1);
        bs.add(startBookie(conf1));
        InetSocketAddress bookie2 = lhbefore.getLedgerMetadata().currentEnsemble.get(1);
        ServerConfiguration conf2 = killBookie(bookie2);

        // using async, because this could trigger an assertion
        final AtomicInteger returnCode = new AtomicInteger(0);
        final CountDownLatch openLatch = new CountDownLatch(1);
        bkc.asyncOpenLedger(lhbefore.getId(), digestType, "".getBytes(),
                            new AsyncCallback.OpenCallback() {
                                public void openComplete(int rc, LedgerHandle lh, Object ctx) {
                                    returnCode.set(rc);
                                    openLatch.countDown();
                                    if (rc == BKException.Code.OK) {
                                        try {
                                            lh.close();
                                        } catch (Exception e) {
                                            LOG.error("Exception closing ledger handle", e);
                                        }
                                    }
                                }
                            }, null);
        assertTrue("Open call should have completed", openLatch.await(5, TimeUnit.SECONDS));
        assertFalse("Open should not have succeeded", returnCode.get() == BKException.Code.OK);

        bsConfs.add(conf2);
        bs.add(startBookie(conf2));

        LedgerHandle lhafter = bkc.openLedger(lhbefore.getId(), digestType,
                "".getBytes());
        assertEquals("Fenced ledger should have correct lastAddConfirmed",
                     lhbefore.getLastAddConfirmed(), lhafter.getLastAddConfirmed());
    }

    /**
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-355}
     * Verify that if a recovery happens with 1 replica missing, and it's replaced
     * with a faulty bookie, it doesn't break future recovery from happening.
     * 1. Ledger is created with quorum size as 2, and entries are written
     * 2. Now first bookie is in the ensemble is brought down.
     * 3. Another client fence and trying to recover the same ledger
     * 4. During this time ensemble change will happen
     *    and new bookie will be added. But this bookie is not able to write.
     * 5. This recovery will fail.
     * 7. A new non-faulty bookie comes up
     * 8. Another client trying to recover the same ledger.
     */
    @Test(timeout=60000)
    public void testBookieFailureDuringRecovery() throws Exception {
        LedgerHandle lhbefore = bkc.createLedger(numBookies, 2, digestType, "".getBytes());
        for (int i = 0; i < (numBookies*3)+1; i++) {
            lhbefore.addEntry("data".getBytes());
        }

        // Add a dead bookie to the cluster
        ServerConfiguration conf = newServerConfiguration();
        Bookie deadBookie1 = new Bookie(conf) {
            @Override
            public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                // drop request to simulate a slow and failed bookie
                throw new IOException("Couldn't write for some reason");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, deadBookie1));

        // kill first bookie server
        InetSocketAddress bookie1 = lhbefore.getLedgerMetadata().currentEnsemble.get(0);
        ServerConfiguration conf1 = killBookie(bookie1);

        // Try to recover and fence the ledger after killing one bookie in the
        // ensemble in the ensemble, and another bookie is available in zk but not writtable
        try {
            bkc.openLedger(lhbefore.getId(), digestType, "".getBytes());
            fail("Shouldn't be able to open ledger, there should be entries missing");
        } catch (BKException.BKLedgerRecoveryException e) {
            // expected
        }

        // start a new good server
        startNewBookie();

        LedgerHandle lhafter = bkc.openLedger(lhbefore.getId(), digestType,
                "".getBytes());
        assertEquals("Fenced ledger should have correct lastAddConfirmed",
                     lhbefore.getLastAddConfirmed(), lhafter.getLastAddConfirmed());
    }

    /**
     * Verify that it doesn't break the recovery when changing ensemble in
     * recovery add.
     */
    @Test(timeout = 60000)
    public void testEnsembleChangeDuringRecovery() throws Exception {
        LedgerHandle lh = bkc.createLedger(numBookies, 2, 2, digestType, "".getBytes());
        int numEntries = (numBookies * 3) + 1;
        final AtomicInteger numPendingAdds = new AtomicInteger(numEntries);
        final CountDownLatch addDone = new CountDownLatch(1);
        for (int i = 0; i < numEntries; i++) {
            lh.asyncAddEntry("data".getBytes(), new AddCallback() {

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
        addDone.await(10, TimeUnit.SECONDS);
        if (numPendingAdds.get() > 0) {
            fail("Failed to add " + numEntries + " to ledger handle " + lh.getId());
        }
        // kill first 2 bookies to replace bookies
        InetSocketAddress bookie1 = lh.getLedgerMetadata().currentEnsemble.get(0);
        ServerConfiguration conf1 = killBookie(bookie1);
        InetSocketAddress bookie2 = lh.getLedgerMetadata().currentEnsemble.get(1);
        ServerConfiguration conf2 = killBookie(bookie2);

        // replace these two bookies
        startDeadBookie(conf1);
        startDeadBookie(conf2);
        // kick in two brand new bookies
        startNewBookie();
        startNewBookie();

        // two dead bookies are put in the ensemble which would cause ensemble
        // change
        LedgerHandle recoveredLh = bkc.openLedger(lh.getId(), digestType, "".getBytes());
        assertEquals("Fenced ledger should have correct lastAddConfirmed", lh.getLastAddConfirmed(),
                recoveredLh.getLastAddConfirmed());
    }

    private void startDeadBookie(ServerConfiguration conf) throws Exception {
        Bookie rBookie = new Bookie(conf) {
            @Override
            public void recoveryAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx, byte[] masterKey)
                    throws IOException, BookieException {
                // drop request to simulate a dead bookie
                throw new IOException("Couldn't write entries for some reason");
            }
        };
        bsConfs.add(conf);
        bs.add(startBookie(conf, rBookie));
    }

    @Test(timeout = 60000)
    public void testBatchRecoverySize3() throws Exception {
        batchRecovery(3);
    }

    @Test(timeout = 60000)
    public void testBatchRecoverySize13() throws Exception {
        batchRecovery(13);
    }

    private void batchRecovery(int batchSize) throws Exception {
        ClientConfiguration newConf = new ClientConfiguration()
                .setReadTimeout(60000).setAddEntryTimeout(60000)
                .setRecoveryReadBatchSize(batchSize);

        newConf.setZkServers(zkUtil.getZooKeeperConnectString());
        BookKeeper newBk = new BookKeeper(newConf);

        LedgerHandle lh = newBk.createLedger(numBookies, 2, 2, digestType, "".getBytes());

        CountDownLatch latch1 = new CountDownLatch(1);
        CountDownLatch latch2 = new CountDownLatch(1);
        sleepBookie(lh.getLedgerMetadata().currentEnsemble.get(0), latch1);
        sleepBookie(lh.getLedgerMetadata().currentEnsemble.get(1), latch2);

        int numEntries = (numBookies * 3) + 1;
        final AtomicInteger numPendingAdds = new AtomicInteger(numEntries);
        final CountDownLatch addDone = new CountDownLatch(1);
        for (int i = 0; i < numEntries; i++) {
            lh.asyncAddEntry(("" + i).getBytes(), new AddCallback() {
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

        LedgerHandle recoverLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        assertEquals(BookieProtocol.INVALID_ENTRY_ID, recoverLh.getLastAddConfirmed());

        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        LedgerRecoveryOp recoveryOp = new LedgerRecoveryOp(recoverLh, new BookkeeperInternalCallbacks.GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                success.set(BKException.Code.OK == rc);
                recoverLatch.countDown();
            }
        }).parallelRead(true).readBatchSize(newConf.getRecoveryReadBatchSize()).setCouldClose(true);
        recoveryOp.initiate();
        recoverLatch.await(10, TimeUnit.SECONDS);
        assertTrue(success.get());
        assertEquals(numEntries, recoveryOp.readCount.get());
        assertEquals(numEntries, recoveryOp.writeCount.get());

        Enumeration<LedgerEntry> enumeration = recoverLh.readEntries(0, numEntries - 1);

        int numReads = 0;
        while (enumeration.hasMoreElements()) {
            LedgerEntry entry = enumeration.nextElement();
            assertEquals((long) numReads, entry.getEntryId());
            assertEquals(numReads, Integer.parseInt(new String(entry.getEntry())));
            ++numReads;
        }
        assertEquals(numEntries, numReads);

        newBk.close();
    }

    static class FakeSlowBookie extends Bookie {

        final AtomicBoolean shouldFailRead;
        final long expectedEntryId;
        boolean alreadyFailRead = false;

        public FakeSlowBookie(ServerConfiguration conf, long expectedEntryId, AtomicBoolean shouldFailRead)
                throws IOException, KeeperException, InterruptedException, BookieException {
            super(conf);
            this.expectedEntryId = expectedEntryId;
            this.shouldFailRead = shouldFailRead;
        }

        @Override
        public ByteBuffer readEntry(long ledgerId, long entryId)
                throws IOException, NoLedgerException {
            if (shouldFailRead.get() && !alreadyFailRead && expectedEntryId == entryId) {
                alreadyFailRead = true;
                throw new IOException("Read Exception on entry " + entryId);
            }
            return super.readEntry(ledgerId, entryId);
        }
    }

    @Test(timeout = 60000)
    public void testRecoveryOnBookieHandleNotAvailable() throws Exception {
        byte[] passwd = "recovery-on-bookie-handle-not-available".getBytes(UTF_8);

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setEnableParallelRecoveryRead(true);
        newConf.setRecoveryReadBatchSize(5);
        BookKeeper newBk = new BookKeeper(newConf);

        final AtomicBoolean shouldFailRead = new AtomicBoolean(true);

        // start additional 3 bookies
        stopAllBookies();
        for (int i = 0; i < 5; i++) {
            ServerConfiguration conf = newServerConfiguration();
            Bookie b = new FakeSlowBookie(conf, 10L, shouldFailRead);
            bs.add(startBookie(conf, b));
            bsConfs.add(conf);
        }

        // create a ledger to write some data.
        LedgerHandle lh = newBk.createLedger(5, 5, 3, digestType, passwd);
        for (int i = 0; i < 10; i++) {
            lh.addEntry(("" + i).getBytes(UTF_8));
        }

        // simulate ledger write failure on concurrent writes
        long lac = lh.getLastAddConfirmed();
        long length = lh.getLength();
        for (long entryId = 10L; entryId < 15L; entryId++) {
            byte[] data = ("" + entryId).getBytes(UTF_8);
            length += data.length;
            ChannelBuffer toSend =
                    lh.macManager.computeDigestAndPackageForSending(
                            entryId, lac, length, data, 0, data.length);
            int bid = (int) (entryId % 5);
            final CountDownLatch addLatch = new CountDownLatch(1);
            final AtomicBoolean addSuccess = new AtomicBoolean(false);
            LOG.info("Add entry {} with lac = {}", entryId, lac);
            lh.bk.bookieClient.addEntry(lh.metadata.currentEnsemble.get(bid), lh.getId(), lh.ledgerKey, entryId, toSend,
                    new WriteCallback() {
                        @Override
                        public void writeComplete(int rc, long ledgerId, long entryId, InetSocketAddress addr, Object ctx) {
                            addSuccess.set(BKException.Code.OK == rc);
                            addLatch.countDown();
                        }
                    }, 0, BookieProtocol.FLAG_NONE);
            addLatch.await();
            assertTrue("Add entry " + entryId + " should succeed", addSuccess.get());
        }

        final LedgerHandle recoverLh =
                newBk.openLedgerNoRecovery(lh.getId(), digestType, passwd);

        final CountDownLatch listenerLatch = new CountDownLatch(1);
        // listener on recover procedure to ensure the recovery reads are completed.
        final BookkeeperInternalCallbacks.ReadEntryListener recoverListener = new BookkeeperInternalCallbacks.ReadEntryListener() {
            @Override
            public void onEntryComplete(int rc, LedgerHandle lh, LedgerEntry entry, Object ctx) {
                if (entry.getEntryId() >= 14L) {
                    listenerLatch.countDown();
                }
            }
        };

        LOG.info("Recover the ledger when bookie are temporarily unavailable");

        final CountDownLatch recoverLatch = new CountDownLatch(1);
        final AtomicInteger recoverRc = new AtomicInteger(-12345);
        recoverLh.recover(new BookkeeperInternalCallbacks.GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                recoverRc.set(rc);
                recoverLatch.countDown();
            }
        }, recoverListener);
        recoverLatch.await();
        assertEquals("Recovery should fail due to one bookie isn't available",
                     BKException.Code.ReadException, recoverRc.get());

        // wait for all outstanding recovery reads to be completed
        listenerLatch.await();

        shouldFailRead.set(false);

        LOG.info("Recover the ledger when all bookies are available");

        final LedgerHandle newRecoverLh =
                newBk.openLedger(lh.getId(), digestType, passwd);

        killBookie(lh.getLedgerMetadata().currentEnsemble.get(0));

        Enumeration<LedgerEntry> entries = newRecoverLh.readEntries(10L, 10L);
        int readCount = 0;
        long expectedEid = 10L;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(expectedEid, entry.getEntryId());
            byte[] data = entry.getEntry();
            assertEquals(expectedEid, Long.parseLong(new String(data, UTF_8)));
            ++expectedEid;
            ++readCount;
        }
        assertEquals(1, readCount);
    }
}
