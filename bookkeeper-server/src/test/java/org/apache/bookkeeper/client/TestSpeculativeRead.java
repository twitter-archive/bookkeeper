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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BaseTestCase;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing;
 *
 */
public class TestSpeculativeRead extends BaseTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestSpeculativeRead.class);

    DigestType digestType;
    byte[] passwd = "specPW".getBytes();

    public TestSpeculativeRead(DigestType digestType) {
        super(10);
        this.digestType = digestType;
    }

    long getLedgerToRead(int ensemble, int quorum) throws Exception {
        byte[] data = "Data for test".getBytes();
        LedgerHandle l = bkc.createLedger(ensemble, quorum, digestType, passwd);
        for (int i = 0; i < 10; i++) {
            l.addEntry(data);
        }
        l.close();

        return l.getId();
    }

    LedgerHandle getLedgerToWrite(int ensemble, int writeQuorum, int ackQuorum) throws Exception {
        byte[] data = "Data for test".getBytes();
        LedgerHandle l = bkc.createLedger(ensemble, writeQuorum, ackQuorum, digestType, passwd);
        for (int i = 0; i < 10; i++) {
            l.addEntry(data);
        }

        return l;
    }


    BookKeeper createClient(int specTimeout) throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setSpeculativeReadTimeout(specTimeout)
            .setReadTimeout(30000);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        return new BookKeeper(conf);
    }

    BookKeeper createClientForReadLAC(int specTimeout) throws Exception {
        ClientConfiguration conf = new ClientConfiguration()
            .setFirstSpeculativeReadLACTimeout(specTimeout)
            .setReadTimeout(30000);
        conf.setZkServers(zkUtil.getZooKeeperConnectString());
        return new BookKeeper(conf);
    }

    class LatchCallback implements ReadCallback, ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback, AsyncCallback.ReadLastConfirmedAndEntryCallback {
        CountDownLatch l = new CountDownLatch(1);
        boolean success = false;
        long startMillis = System.currentTimeMillis();
        long endMillis = Long.MAX_VALUE;

        @Override
        public void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry) {
            endMillis = System.currentTimeMillis();
            LOG.debug("Got response {} {}", rc, getDuration());
            success = rc == BKException.Code.OK;
            l.countDown();
        }

        public void readComplete(int rc,
                                 LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq,
                                 Object ctx) {
            endMillis = System.currentTimeMillis();
            LOG.debug("Got response {} {}", rc, getDuration());
            success = rc == BKException.Code.OK;
            l.countDown();
        }

        long getDuration() {
            return endMillis - startMillis;
        }

        void expectSuccess(int milliseconds) throws Exception {
            assertTrue(l.await(milliseconds, TimeUnit.MILLISECONDS));
            assertTrue(success);
        }

        void expectFail(int milliseconds) throws Exception {
            assertTrue(l.await(milliseconds, TimeUnit.MILLISECONDS));
            assertFalse(success);
        }

        void expectTimeout(int milliseconds) throws Exception {
            assertFalse(l.await(milliseconds, TimeUnit.MILLISECONDS));
        }

        @Override
        public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
            endMillis = System.currentTimeMillis();
            LOG.debug("Got response {} {}", rc, getDuration());
            success = rc == BKException.Code.OK;
            l.countDown();
        }
    }

    /**
     * Test basic speculative functionality.
     * - Create 2 clients with read timeout disabled, one with spec
     *   read enabled, the other not.
     * - create ledger
     * - sleep second bookie in ensemble
     * - read first entry, both should find on first bookie.
     * - read second bookie, spec client should find on bookie three,
     *   non spec client should hang.
     */
    @Test
    public void testSpeculativeRead() throws Exception {
        long id = getLedgerToRead(3,2);
        int timeout = 400;
        BookKeeper bknospec = createClient(0); // disabled
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle lnospec = bknospec.openLedger(id, digestType, passwd);
        LedgerHandle lspec = bkspec.openLedger(id, digestType, passwd);

        // sleep second bookie
        CountDownLatch sleepLatch = new CountDownLatch(1);
        BookieSocketAddress second = lnospec.getLedgerMetadata().getEnsembles().get(0L).get(1);
        sleepBookie(second, sleepLatch);

        try {
            // read first entry, both go to first bookie, should be fine
            LatchCallback nospeccb = new LatchCallback();
            LatchCallback speccb = new LatchCallback();
            lnospec.asyncReadEntries(0, 0, nospeccb, null);
            lspec.asyncReadEntries(0, 0, speccb, null);
            nospeccb.expectSuccess(timeout);
            speccb.expectSuccess(timeout);

            // read second entry, both look for second book, spec read client
            // tries third bookie, nonspec client hangs as read timeout is very long.
            nospeccb = new LatchCallback();
            speccb = new LatchCallback();
            lnospec.asyncReadEntries(1, 1, nospeccb, null);
            lspec.asyncReadEntries(1, 1, speccb, null);
            speccb.expectSuccess(2 * timeout);
            nospeccb.expectTimeout(2 * timeout);
        } finally {
            sleepLatch.countDown();
            lspec.close();
            lnospec.close();
            bkspec.close();
            bknospec.close();
        }
    }

    /**
     * Test that if more than one replica is down, we can still read, as long as the quorum
     * size is larger than the number of down replicas.
     */
    @Test
    public void testSpeculativeReadMultipleReplicasDown() throws Exception {
        long id = getLedgerToRead(5,5);
        int timeout = 400;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookie 1, 2 & 4
        CountDownLatch sleepLatch = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(1), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(2), sleepLatch);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(4), sleepLatch);

        try {
            // read first entry, should complete faster than timeout
            // as bookie 0 has the entry
            LatchCallback firstReadComplete = new LatchCallback();
            l.asyncReadEntries(0, 0, firstReadComplete, null);
            firstReadComplete.expectSuccess(timeout/2);

            // second should have to hit two timeouts (bookie 1 & 2)
            // bookie 3 has the entry
            LatchCallback secondReadComplete = new LatchCallback();
            l.asyncReadEntries(1, 1, secondReadComplete, null);
            secondReadComplete.expectTimeout(timeout);
            secondReadComplete.expectSuccess(timeout*2);
            LOG.info("Timeout {} latch1 duration {}", timeout, secondReadComplete.getDuration());
            assertTrue("should have taken longer than two timeouts, but less than 3",
                secondReadComplete.getDuration() >= timeout*2
                       && secondReadComplete.getDuration() < timeout*3);

            // third should have to hit one timeouts (bookie 2)
            // bookie 3 has the entry
            LatchCallback thirdReadComplete = new LatchCallback();
            l.asyncReadEntries(2, 2, thirdReadComplete, null);
            thirdReadComplete.expectTimeout(timeout/2);
            thirdReadComplete.expectSuccess(timeout);
            LOG.info("Timeout {} latch2 duration {}", timeout, thirdReadComplete.getDuration());
            assertTrue("should have taken longer than one timeout, but less than 2",
                thirdReadComplete.getDuration() >= timeout
                       && thirdReadComplete.getDuration() < timeout*2);

            // fourth should have no timeout
            // bookie 3 has the entry
            LatchCallback fourthReadComplete = new LatchCallback();
            l.asyncReadEntries(3, 3, fourthReadComplete, null);
            fourthReadComplete.expectSuccess(timeout/2);

            // fifth should hit one timeout, (bookie 4)
            // bookie 0 has the entry
            LatchCallback fifthReadComplete = new LatchCallback();
            l.asyncReadEntries(4, 4, fifthReadComplete, null);
            fifthReadComplete.expectTimeout(timeout/2);
            fifthReadComplete.expectSuccess(timeout);
            LOG.info("Timeout {} latch4 duration {}", timeout, fifthReadComplete.getDuration());
            assertTrue("should have taken longer than one timeout, but less than 2",
                fifthReadComplete.getDuration() >= timeout
                       && fifthReadComplete.getDuration() < timeout*2);

        } finally {
            sleepLatch.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Test that if after a speculative read is kicked off, the original read completes
     * nothing bad happens.
     */
    @Test
    public void testSpeculativeReadFirstReadCompleteIsOk() throws Exception {
        long id = getLedgerToRead(2,2);
        int timeout = 400;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        // sleep bookies
        CountDownLatch sleepLatch0 = new CountDownLatch(1);
        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(0), sleepLatch0);
        sleepBookie(l.getLedgerMetadata().getEnsembles().get(0L).get(1), sleepLatch1);

        try {
            // read goes to first bookie, spec read timeout occurs,
            // goes to second
            LatchCallback firstReadComplete = new LatchCallback();
            l.asyncReadEntries(0, 0, firstReadComplete, null);
            firstReadComplete.expectTimeout(timeout);

            // wake up first bookie
            sleepLatch0.countDown();
            firstReadComplete.expectSuccess(timeout/2);

            sleepLatch1.countDown();

            // check we can read next entry without issue
            LatchCallback secondReadComplete = new LatchCallback();
            l.asyncReadEntries(1, 1, secondReadComplete, null);
            secondReadComplete.expectSuccess(timeout/2);

        } finally {
            sleepLatch0.countDown();
            sleepLatch1.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Unit test for the speculative read scheduling method
     */
    @Test
    public void testSpeculativeReadScheduling() throws Exception {
        long id = getLedgerToRead(3,2);
        int timeout = 400;
        BookKeeper bkspec = createClient(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        ArrayList<BookieSocketAddress> ensemble = l.getLedgerMetadata().getEnsembles().get(0L);
        BitSet allHosts = new BitSet(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            allHosts.set(i, true);
        }
        BitSet noHost = new BitSet(ensemble.size());
        BitSet secondHostOnly = new BitSet(ensemble.size());
        secondHostOnly.set(1, true);
        PendingReadOp.LedgerEntryRequest req0 = null, req2 = null, req4 = null;
        try {
            LatchCallback readComplete = new LatchCallback();
            PendingReadOp op = new PendingReadOp(l, bkspec.scheduler,
                                                 0, 5, readComplete, null);

            // if we've already heard from all hosts,
            // we only send the initial read
            req0 = op.new SequenceReadRequest(ensemble, l.getId(), 0);
            assertTrue("Should have sent to first",
                       req0.maybeSendSpeculativeRead(allHosts).equals(ensemble.get(0)));
            assertNull("Should not have sent another",
                       req0.maybeSendSpeculativeRead(allHosts));

            // if we have heard from some hosts, but not one we have sent to
            // send again
            req2 = op.new SequenceReadRequest(ensemble, l.getId(), 2);
            assertTrue("Should have sent to third",
                       req2.maybeSendSpeculativeRead(noHost).equals(ensemble.get(2)));
            assertTrue("Should have sent to first",
                       req2.maybeSendSpeculativeRead(secondHostOnly).equals(ensemble.get(0)));

            // if we have heard from some hosts, which includes one we sent to
            // do not read again
            req4 = op.new SequenceReadRequest(ensemble, l.getId(), 4);
            assertTrue("Should have sent to second",
                       req4.maybeSendSpeculativeRead(noHost).equals(ensemble.get(1)));
            assertNull("Should not have sent another",
                       req4.maybeSendSpeculativeRead(secondHostOnly));
        } finally {
            for (PendingReadOp.LedgerEntryRequest req
                     : new PendingReadOp.LedgerEntryRequest[] { req0, req2, req4 }) {
                if (req != null) {
                    int i = 0;
                    while (!req.isComplete()) {
                        if (i++ > 10) {
                            break; // wait for up to 10 seconds
                        }
                        Thread.sleep(1000);
                    }
                    assertTrue("Request should be done", req0.isComplete());
                }
            }

            l.close();
            bkspec.close();
        }
    }

    /**
     * Test basic speculative functionality.
     * - Create 2 clients with read timeout disabled, one with spec
     *   read enabled, the other not.
     * - create ledger
     * - sleep second bookie in ensemble
     * - read first entry, both should find on first bookie.
     * - read second bookie, spec client should find on bookie three,
     *   non spec client should hang.
     */
    @Test
    public void testSpeculativeReadLAC() throws Exception {
        LedgerHandle lh = getLedgerToWrite(3, 3, 2);
        int timeOut = 400;
        BookKeeper bknospec = createClientForReadLAC(0); // disabled
        BookKeeper bkspec = createClientForReadLAC(timeOut);

        LedgerHandle lnospec = bknospec.openLedgerNoRecovery(lh.getId(), digestType, passwd);
        LedgerHandle lspec = bkspec.openLedgerNoRecovery(lh.getId(), digestType, passwd);

        lh.addEntry("Data for test".getBytes());

        // sleep second bookie
        CountDownLatch sleepLatch = new CountDownLatch(1);
        long entryId = lnospec.getLastAddConfirmed() + 1;
        sleepBookie(lnospec.getLedgerMetadata().getEnsemble(entryId).get(
            lnospec.distributionSchedule.getWriteSet(entryId).get(0))
            , sleepLatch);

        try {
            // read last confirmed
            LatchCallback nospeccb = new LatchCallback();
            LatchCallback speccb = new LatchCallback();
            lnospec.asyncReadLastConfirmedAndEntry(lnospec.getLastAddConfirmed() + 1, 10000, false, nospeccb, null);
            lspec.asyncReadLastConfirmedAndEntry(lnospec.getLastAddConfirmed() + 1, 10000, false, speccb, null);
            speccb.expectSuccess(2 * timeOut);
            nospeccb.expectTimeout(2 * timeOut);
        } finally {
            sleepLatch.countDown();
            lspec.close();
            lnospec.close();
            bkspec.close();
            bknospec.close();
        }
    }

    /**
     * Test that if one replica is down, we can still read, as long as the quorum
     * size is larger than the number of down replicas.
     */
    @Test
    public void testSpeculativeReadLACOneReplicaDown() throws Exception {
        LedgerHandle lh = getLedgerToWrite(5, 5, 3);
        int timeout = 400;
        BookKeeper bkspec = createClientForReadLAC(timeout);

        LedgerHandle l = bkspec.openLedgerNoRecovery(lh.getId(), digestType, passwd);

        lh.addEntry("Data for test".getBytes());

        // sleep bookie 1, 2 & 4
        CountDownLatch sleepLatch = new CountDownLatch(1);
        long entryId = l.getLastAddConfirmed() + 1;
        sleepBookie(lh.getLedgerMetadata().getEnsemble(entryId).get(
            lh.distributionSchedule.getWriteSet(entryId).get(0))
            , sleepLatch);

        try {
            // second should have to hit two timeouts (bookie 1 & 2)
            // bookie 3 has the entry
            LatchCallback readComplete = new LatchCallback();
            l.asyncReadLastConfirmedAndEntry(entryId, 10000, false, readComplete, null);
            readComplete.expectTimeout(timeout);
            readComplete.expectSuccess(timeout*2);
            LOG.info("Timeout {} latch1 duration {}", timeout, readComplete.getDuration());
            assertTrue("should have taken longer than two timeouts, but less than 3",
                readComplete.getDuration() >= timeout
                    && readComplete.getDuration() < timeout*2);
        } finally {
            sleepLatch.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Test that if more than one replica is down, we can still read, as long as the quorum
     * size is larger than the number of down replicas.
     */
    @Test
    public void testSpeculativeReadLACMultipleReplicasDown() throws Exception {
        LedgerHandle lh = getLedgerToWrite(5, 5, 3);
        int timeout = 200;
        BookKeeper bkspec = createClientForReadLAC(timeout);

        LedgerHandle l = bkspec.openLedgerNoRecovery(lh.getId(), digestType, passwd);

        lh.addEntry("Data for test".getBytes());

        // sleep bookie 1, 2 & 4
        CountDownLatch sleepLatch = new CountDownLatch(1);
        long entryId = l.getLastAddConfirmed() + 1;
        sleepBookie(lh.getLedgerMetadata().getEnsemble(entryId).get(
            lh.distributionSchedule.getWriteSet(entryId).get(0))
            , sleepLatch);
        sleepBookie(lh.getLedgerMetadata().getEnsemble(entryId).get(
            lh.distributionSchedule.getWriteSet(entryId).get(1))
            , sleepLatch);

        try {
            // second should have to hit two timeouts (bookie 0 & 1)
            // bookie 2 has the entry
            LatchCallback readComplete = new LatchCallback();
            l.asyncReadLastConfirmedAndEntry(entryId, 10000, false, readComplete, null);
            readComplete.expectTimeout(timeout);
            readComplete.expectTimeout(timeout*2);
            readComplete.expectSuccess(timeout*2);
            LOG.info("Timeout {} latch1 duration {}", timeout, readComplete.getDuration());
            assertTrue("should have taken longer than two timeouts, but less than 3",
                readComplete.getDuration() >= timeout*3
                    && readComplete.getDuration() < timeout*4);
        } finally {
            sleepLatch.countDown();
            l.close();
            bkspec.close();
        }
    }


    /**
     * Test that if after a speculative read is kicked off, the original read completes
     * nothing bad happens.
     */
    @Test
    public void testSpeculativeReadLACFirstReadCompleteIsOk() throws Exception {
        LedgerHandle lh = getLedgerToWrite(2, 2, 2);
        int timeout = 400;
        BookKeeper bkspec = createClientForReadLAC(timeout);

        LedgerHandle l = bkspec.openLedgerNoRecovery(lh.getId(), digestType, passwd);
        long nextEntryId = l.getLastAddConfirmed()+1;
        lh.addEntry("Data for test".getBytes());

        // sleep bookies
        CountDownLatch sleepLatch0 = new CountDownLatch(1);
        CountDownLatch sleepLatch1 = new CountDownLatch(1);
        long entryId = l.getLastAddConfirmed() + 1;
        sleepBookie(lh.getLedgerMetadata().getEnsemble(entryId).get(
            lh.distributionSchedule.getWriteSet(entryId).get(0))
            , sleepLatch0);
        sleepBookie(lh.getLedgerMetadata().getEnsemble(entryId).get(
            lh.distributionSchedule.getWriteSet(entryId).get(1))
            , sleepLatch1);

        try {
            // read goes to first bookie, spec read timeout occurs,
            // goes to second
            LatchCallback firstReadComplete = new LatchCallback();
            l.asyncReadLastConfirmedAndEntry(nextEntryId++, 10000, false, firstReadComplete, null);
            firstReadComplete.expectTimeout(timeout);

            // wake up first bookie
            sleepLatch0.countDown();
            firstReadComplete.expectSuccess(timeout/2);

            sleepLatch1.countDown();

            lh.addEntry("Data for test".getBytes());

            // check we can read next entry without issue
            LatchCallback secondReadComplete = new LatchCallback();
            l.asyncReadLastConfirmedAndEntry(nextEntryId++, 10000, false, secondReadComplete, null);
            secondReadComplete.expectSuccess(timeout/2);

        } finally {
            sleepLatch0.countDown();
            sleepLatch1.countDown();
            l.close();
            bkspec.close();
        }
    }

    /**
     * Unit test for the speculative read scheduling method
     */
    @Test
    public void testSpeculativeReadLastEntryAndOpScheduling() throws Exception {
        int timeout = 400;
        long id = getLedgerToRead(3,2);
        BookKeeper bkspec = createClientForReadLAC(timeout);

        LedgerHandle l = bkspec.openLedger(id, digestType, passwd);

        ArrayList<BookieSocketAddress> ensemble = l.getLedgerMetadata().getEnsembles().get(0L);
        BitSet allHosts = new BitSet(ensemble.size());
        for (int i = 0; i < ensemble.size(); i++) {
            allHosts.set(i, true);
        }
        BitSet noHost = new BitSet(ensemble.size());
        BitSet secondHostOnly = new BitSet(ensemble.size());
        secondHostOnly.set(1, true);
        ReadLastConfirmedAndEntryOp.ReadLACAndEntryRequest req0 = null, req2 = null, req4 = null;
        try {
            LatchCallback firstReadComplete = new LatchCallback();
            ReadLastConfirmedAndEntryOp op = new ReadLastConfirmedAndEntryOp(l, firstReadComplete,
                5, 500, bkspec.scheduler);

            // if we've already heard from all hosts,
            // we only send the initial read
            req0 = op.new SequenceReadRequest(ensemble, l.getId(), 0);
            assertTrue("Should have sent to first",
                req0.maybeSendSpeculativeRead(allHosts).equals(ensemble.get(0)));
            assertNull("Should not have sent another",
                req0.maybeSendSpeculativeRead(allHosts));

            // if we have heard from some hosts, but not one we have sent to
            // send again
            req2 = op.new SequenceReadRequest(ensemble, l.getId(), 2);
            assertTrue("Should have sent to third",
                req2.maybeSendSpeculativeRead(noHost).equals(ensemble.get(2)));
            assertTrue("Should have sent to first",
                req2.maybeSendSpeculativeRead(secondHostOnly).equals(ensemble.get(0)));

            // if we have heard from some hosts, which includes one we sent to
            // do not read again
            req4 = op.new SequenceReadRequest(ensemble, l.getId(), 4);
            assertTrue("Should have sent to second",
                req4.maybeSendSpeculativeRead(noHost).equals(ensemble.get(1)));
            assertNull("Should not have sent another",
                req4.maybeSendSpeculativeRead(secondHostOnly));
        } finally {
            l.close();
            bkspec.close();
        }
    }
}
