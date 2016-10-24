package org.apache.bookkeeper.client;

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

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * This class tests the bookie recovery admin functionality.
 */
public class BookieRecoveryTest extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(BookieRecoveryTest.class);

    // Object used for synchronizing async method calls
    class SyncObject {
        boolean value;

        public SyncObject() {
            value = false;
        }
    }

    // Object used for implementing the Bookie RecoverCallback for this jUnit
    // test. This verifies that the operation completed successfully.
    class BookieRecoverCallback implements RecoverCallback {
        boolean success = false;
        @Override
        public void recoverComplete(int rc, Object ctx) {
            LOG.info("Recovered bookie operation completed with rc: " + rc);
            success = rc == BKException.Code.OK;
            SyncObject sync = (SyncObject) ctx;
            synchronized (sync) {
                sync.value = true;
                sync.notify();
            }
        }
    }

    // Objects to use for this jUnit test.
    DigestType digestType;
    String ledgerManagerFactory;
    SyncObject sync;
    BookieRecoverCallback bookieRecoverCb;
    BookKeeperAdmin bkAdmin;

    // Constructor
    public BookieRecoveryTest() {
        super(3);

        this.digestType = DigestType.CRC32;
        this.ledgerManagerFactory = "org.apache.bookkeeper.meta.HierarchicalLedgerManagerFactory";
        LOG.info("Using ledger manager " + ledgerManagerFactory);
        // set ledger manager
        baseConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        baseClientConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        // Set up the configuration properties needed.
        baseClientConf.setBookieRecoveryDigestType(digestType);
        baseClientConf.setBookieRecoveryPasswd("".getBytes());
        super.setUp();

        sync = new SyncObject();
        bookieRecoverCb = new BookieRecoverCallback();
        ClientConfiguration adminConf = new ClientConfiguration(baseClientConf);
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        bkAdmin = new BookKeeperAdmin(adminConf);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        // Release any resources used by the BookKeeperTools instance.
        bkAdmin.close();
        super.tearDown();
    }

    /**
     * Helper method to create a number of ledgers
     *
     * @param numLedgers
     *            Number of ledgers to create
     * @return List of LedgerHandles for each of the ledgers created
     */
    private List<LedgerHandle> createLedgers(int numLedgers)
            throws BKException, IOException, InterruptedException
    {
        return createLedgers(numLedgers, 3, 2);
    }

    /**
     * Helper method to create a number of ledgers
     *
     * @param numLedgers
     *            Number of ledgers to create
     * @param ensemble Ensemble size for ledgers
     * @param quorum Quorum size for ledgers
     * @return List of LedgerHandles for each of the ledgers created
     */
    private List<LedgerHandle> createLedgers(int numLedgers, int ensemble, int quorum)
            throws BKException, IOException,
        InterruptedException {
        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < numLedgers; i++) {
            lhs.add(bkc.createLedger(ensemble, quorum,
                                     digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        return lhs;
    }

    private List<LedgerHandle> openLedgers(List<LedgerHandle> oldLhs)
            throws Exception {
        List<LedgerHandle> newLhs = new ArrayList<LedgerHandle>();
        for (LedgerHandle oldLh : oldLhs) {
            newLhs.add(bkc.openLedger(oldLh.getId(), digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        return newLhs;
    }

    /**
     * Helper method to write dummy ledger entries to all of the ledgers passed.
     *
     * @param numEntries
     *            Number of ledger entries to write for each ledger
     * @param startEntryId
     *            The first entry Id we're expecting to write for each ledger
     * @param lhs
     *            List of LedgerHandles for all ledgers to write entries to
     * @throws BKException
     * @throws InterruptedException
     */
    private void writeEntriestoLedgers(int numEntries, long startEntryId,
                                       List<LedgerHandle> lhs)
        throws BKException, InterruptedException {
        for (LedgerHandle lh : lhs) {
            for (int i = 0; i < numEntries; i++) {
                lh.addEntry(("LedgerId: " + lh.getId() + ", EntryId: " + (startEntryId + i)).getBytes());
            }
        }
    }

    private void closeLedgers(List<LedgerHandle> lhs) throws BKException, InterruptedException {
        for (LedgerHandle lh : lhs) {
            lh.close();
        }
    }

    /**
     * Helper method to verify that we can read the recovered ledger entries.
     *
     * @param oldLhs
     *            Old Ledger Handles
     * @param untilEntryId
     *            End Entry Id to read
     * @throws BKException
     * @throws InterruptedException
     */
    private void verifyRecoveredLedgers(List<LedgerHandle> oldLhs, long untilEntryId) throws Exception {
        // Get a set of LedgerHandles for all of the ledgers to verify
        List<LedgerHandle> lhs = new ArrayList<LedgerHandle>();
        for (int i = 0; i < oldLhs.size(); i++) {
            lhs.add(bkc.openLedger(oldLhs.get(i).getId(), digestType, baseClientConf.getBookieRecoveryPasswd()));
        }
        // Read the ledger entries to verify that they are all present and
        // correct in the new bookie.
        for (LedgerHandle lh : lhs) {
            verifyFullyReplicated(lh, untilEntryId);
        }
    }

    /**
     * This tests the bookie recovery functionality with ensemble changes.
     * We'll verify that:
     * - bookie recovery should not affect ensemble change.
     * - ensemble change should not erase changes made by recovery.
     *
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-667}
     */
    @Test(timeout = 60000)
    public void testMetadataConflictWithRecovery() throws Exception {
        metadataConflictWithRecovery(bkc);
    }

    @Test(timeout = 60000)
    public void testMetadataConflictWhenDelayingEnsembleChange() throws Exception {
        ClientConfiguration newConf = new ClientConfiguration(baseClientConf);
        newConf.setZkServers(zkUtil.getZooKeeperConnectString());
        newConf.setDelayEnsembleChange(true);
        BookKeeper newBkc = new BookKeeper(newConf);
        try {
            metadataConflictWithRecovery(newBkc);
        } finally {
            newBkc.close();
        }
    }

    void metadataConflictWithRecovery(BookKeeper bkc) throws Exception {
        int numEntries = 10;
        byte[] data = "testMetadataConflictWithRecovery".getBytes();

        LedgerHandle lh = bkc.createLedger(2, 2, digestType, baseClientConf.getBookieRecoveryPasswd());
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        BookieSocketAddress bookieToKill = lh.getLedgerMetadata().getEnsemble(numEntries - 1).get(1);
        killBookie(bookieToKill);
        startNewBookie();
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        bkAdmin.recoverBookieData(bookieToKill, null);
        // fail another bookie to cause ensemble change again
        bookieToKill = lh.getLedgerMetadata().getEnsemble(2 * numEntries - 1).get(1);
        ServerConfiguration confOfKilledBookie = killBookie(bookieToKill);
        startNewBookie();
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(data);
        }
        // start the killed bookie again
        bsConfs.add(confOfKilledBookie);
        bs.add(startBookie(confOfKilledBookie));
        // all ensembles should be fully replicated since it is recovered
        assertTrue("Not fully replicated", verifyFullyReplicated(lh, 3 * numEntries));
        lh.close();
    }

    @Test(timeout = 60000)
    public void testBookieRecoveryFirstEnsemble() throws Exception {
        testBookieRecoveryWholeEnsemble(false);
    }

    @Test(timeout = 60000)
    public void testBookieRecoveryLastEnsemble() throws Exception {
        testBookieRecoveryWholeEnsemble(true);
    }

    private void testBookieRecoveryWholeEnsemble(boolean testOnLastEnsemble) throws Exception {
        // Create the ledgers
        List<LedgerHandle> lhs = createLedgers(3, 3, 3);

        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        for (ServerConfiguration conf : bsConfs) {
            int port = conf.getBookiePort();
            BookieSocketAddress bookie =
                    new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), port);
            bookiesSrc.add(bookie);
        }
        Set<ServerConfiguration> confsKilled = new HashSet<ServerConfiguration>();
        if (!testOnLastEnsemble) {
            for (BookieSocketAddress bookie : bookiesSrc) {
                LOG.info("Killed bookie {}", bookie);
                confsKilled.add(killBookie(bookie));
            }

            // Startup three new bookies servers
            for (int i = 0; i < 3; i++) {
                startNewBookie();
            }

            // Write some more entries to roll new ensemble
            writeEntriestoLedgers(numMsgs, 10, lhs);

            // Startup the killed servers
            for (ServerConfiguration conf : confsKilled) {
                bs.add(startBookie(conf));
                LOG.info("Started bookie at port {}", conf.getBookiePort());
            }
        } else {
            // Startup three new bookies servers
            for (int i = 0; i < 3; i++) {
                startNewBookie();
            }
        }

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the whole ensemble (" + bookiesSrc
                + ", testOnLastEnsemble = " + testOnLastEnsemble
                + ") and replicate it to a random available one");
        bkAdmin.recoverBookieData(bookiesSrc);

        // Verify the recovered ledger metadata
        verifyLedgerMetadata(lhs, bookiesSrc);
        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 2 * numMsgs - 1);
    }

    /**
     * This tests the asynchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a new one to
     * replace it, and then recovering the ledger entries from the killed bookie
     * onto the new one. We'll verify that the entries stored on the killed
     * bookie are properly copied over and restored onto the new one.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBookieRecoveryToSpecificBookie() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        int initialPort = bsConfs.get(0).getBookiePort();
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup a new bookie server
        int newBookiePort = startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        BookieSocketAddress bookieSrc = new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        BookieSocketAddress bookieDest = new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), newBookiePort);
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc + ") and replicate it to the new one ("
                 + bookieDest + ")");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.asyncRecoverBookieData(bookieSrc, bookieDest, bookieRecoverCb, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
            assertTrue(bookieRecoverCb.success);
        }

        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        bookiesSrc.add(bookieSrc);
        // Verify the recovered ledger metadata
        verifyLedgerMetadata(lhs, bookiesSrc);
        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 2 * numMsgs - 1);
    }

    /**
     * This tests the synchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a new one to
     * replace it, and then recovering the ledger entries from the killed bookie
     * onto the new one. We'll verify that the entries stored on the killed
     * bookie are properly copied over and restored onto the new one.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSyncBookieRecoveryToSpecificBookie() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        int initialPort = bsConfs.get(0).getBookiePort();
        bs.get(0).shutdown();
        bs.remove(0);

        // Startup a new bookie server
        int newBookiePort = startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the sync recover bookie method.
        BookieSocketAddress bookieSrc = new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        BookieSocketAddress bookieDest = new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), newBookiePort);
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc + ") and replicate it to the new one ("
                 + bookieDest + ")");
        bkAdmin.recoverBookieData(bookieSrc, bookieDest);

        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        bookiesSrc.add(bookieSrc);
        // Verify the recovered ledger metadata
        verifyLedgerMetadata(lhs, bookiesSrc);
        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 2 * numMsgs - 1);
    }

    /**
     * This tests the asynchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a few new
     * bookies, and then recovering the ledger entries from the killed bookie
     * onto random available bookie servers. We'll verify that the entries
     * stored on the killed bookie are properly copied over and restored onto
     * the other bookies.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testAsyncBookieRecoveryToRandomBookies1() throws Exception {
        testBookieRecoveryToRandomBookies(true, 1);
    }

    @Test(timeout = 60000)
    public void testAsyncBookieRecoveryToRandomBookies2() throws Exception {
        testBookieRecoveryToRandomBookies(true, 2);
    }

    /**
     * This tests the synchronous bookie recovery functionality by writing
     * entries into 3 bookies, killing one bookie, starting up a few new
     * bookies, and then recovering the ledger entries from the killed bookie
     * onto random available bookie servers. We'll verify that the entries
     * stored on the killed bookie are properly copied over and restored onto
     * the other bookies.
     *
     * @throws Exception
     */
    @Test(timeout = 60000)
    public void testSyncBookieRecoveryToRandomBookies1() throws Exception {
        testBookieRecoveryToRandomBookies(false, 1);
    }

    @Test(timeout = 60000)
    public void testSyncBookieRecoveryToRandomBookies2() throws Exception {
        testBookieRecoveryToRandomBookies(false, 2);
    }

    private void testBookieRecoveryToRandomBookies(boolean async, int numBookiesToKill) throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, 3, 3);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown {} bookies.", numBookiesToKill);

        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        for (int i = 0; i < numBookiesToKill; i++) {
            int portToKill = bsConfs.get(0).getBookiePort();
            bs.get(0).shutdown();
            bs.remove(0);
            BookieSocketAddress bookieToKill =
                    new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), portToKill);
            bookiesSrc.add(bookieToKill);
        }

        // Startup three new bookie servers
        for (int i = 0; i < 3; i++) {
            startNewBookie();
        }

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, 10, lhs);

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookiesSrc
                 + ") and replicate it to a random available one");
        if (async) {
            // Initiate the sync object
            sync.value = false;
            bkAdmin.asyncRecoverBookieData(bookiesSrc, bookieRecoverCb, sync);

            // Wait for the async method to complete.
            synchronized (sync) {
                while (sync.value == false) {
                    sync.wait();
                }
                assertTrue(bookieRecoverCb.success);
            }
        } else {
            bkAdmin.recoverBookieData(bookiesSrc);
        }

        // Verify the recovered ledger metadata
        verifyLedgerMetadata(lhs, bookiesSrc);
        // Verify the recovered ledger entries are okay.
        verifyRecoveredLedgers(lhs, 2 * numMsgs - 1);
    }

    private static class ReplicationVerificationCallback implements ReadEntryCallback {
        final CountDownLatch latch;
        final AtomicLong numSuccess;

        ReplicationVerificationCallback(int numRequests) {
            latch = new CountDownLatch(numRequests);
            numSuccess = new AtomicLong(0);
        }

        @Override
        public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
            if (LOG.isDebugEnabled()) {
                BookieSocketAddress addr = (BookieSocketAddress)ctx;
                LOG.debug("Got " + rc + " for ledger " + ledgerId + " entry " + entryId + " from " + ctx);
            }
            if (rc == BKException.Code.OK) {
                numSuccess.incrementAndGet();
            }
            latch.countDown();
        }

        long await() throws InterruptedException {
            if (latch.await(60, TimeUnit.SECONDS) == false) {
                LOG.warn("Didn't get all responses in verification");
                return 0;
            } else {
                return numSuccess.get();
            }
        }
    }

    private void verifyLedgerMetadata(List<LedgerHandle> lhs, Set<BookieSocketAddress> bookiesReplaced) throws Exception {
        for (LedgerHandle lh : lhs) {
            verifyLedgerMetadata(lh, bookiesReplaced);
        }
    }

    private void verifyLedgerMetadata(LedgerHandle lh, Set<BookieSocketAddress> bookiesReplaced) throws Exception {
        LedgerMetadata md = getLedgerMetadata(lh);
        boolean containReplacedBookies = false;
        for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : md.getEnsembles().entrySet()) {
            Set<BookieSocketAddress> uniqueBookies = new HashSet<BookieSocketAddress>();
            uniqueBookies.addAll(e.getValue());
            assertEquals("Duplicated bookies found in " + e.getValue(),
                    e.getValue().size(), uniqueBookies.size());
            for (BookieSocketAddress addr : e.getValue()) {
                if (bookiesReplaced.contains(addr)) {
                    containReplacedBookies = true;
                }
            }
        }
        if (containReplacedBookies) {
            LOG.error("Ledger {} still contains replaced bookies {} : {}",
                    new Object[]{lh.getId(), bookiesReplaced, md});
        }
        assertFalse("Should not contain replaced bookies : " + bookiesReplaced + " in " + md, containReplacedBookies);
    }

    private boolean verifyFullyReplicated(LedgerHandle lh, long untilEntry) throws Exception {
        LedgerMetadata md = getLedgerMetadata(lh);

        Map<Long, ArrayList<BookieSocketAddress>> ensembles = md.getEnsembles();

        HashMap<Long, Long> ranges = new HashMap<Long, Long>();
        ArrayList<Long> keyList = Collections.list(
                Collections.enumeration(ensembles.keySet()));
        Collections.sort(keyList);
        for (int i = 0; i < keyList.size() - 1; i++) {
            ranges.put(keyList.get(i), keyList.get(i+1));
        }
        ranges.put(keyList.get(keyList.size()-1), untilEntry);

        for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : ensembles.entrySet()) {
            int quorum = md.getAckQuorumSize();
            long startEntryId = e.getKey();
            long endEntryId = ranges.get(startEntryId);
            long expectedSuccess = quorum*(endEntryId-startEntryId);
            int numRequests = e.getValue().size()*((int)(endEntryId-startEntryId));

            ReplicationVerificationCallback cb = new ReplicationVerificationCallback(numRequests);
            for (long i = startEntryId; i < endEntryId; i++) {
                for (BookieSocketAddress addr : e.getValue()) {
                    bkc.bookieClient.readEntry(addr, lh.getId(), i, cb, addr);
                }
            }

            long numSuccess = cb.await();
            if (numSuccess < expectedSuccess) {
                LOG.warn("Fragment not fully replicated ledgerId = " + lh.getId()
                         + " startEntryId = " + startEntryId
                         + " endEntryId = " + endEntryId
                         + " expectedSuccess = " + expectedSuccess
                         + " gotSuccess = " + numSuccess);
                return false;
            }
        }
        return true;
    }

    // Object used for synchronizing async method calls
    class SyncLedgerMetaObject {
        boolean value;
        int rc;
        LedgerMetadata meta;

        public SyncLedgerMetaObject() {
            value = false;
            meta = null;
        }
    }

    private LedgerMetadata getLedgerMetadata(LedgerHandle lh) throws Exception {
        final SyncLedgerMetaObject syncObj = new SyncLedgerMetaObject();
        bkc.getLedgerManager().readLedgerMetadata(lh.getId(), new GenericCallback<LedgerMetadata>() {

            @Override
            public void operationComplete(int rc, LedgerMetadata result) {
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.meta = result;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }

        });

        synchronized (syncObj) {
            while (syncObj.value == false) {
                syncObj.wait();
            }
        }
        assertEquals(BKException.Code.OK, syncObj.rc);
        return syncObj.meta;
    }

    private boolean findDupesInEnsembles(List<LedgerHandle> lhs) throws Exception {
        long numDupes = 0;
        for (LedgerHandle lh : lhs) {
            LedgerMetadata md = getLedgerMetadata(lh);
            for (Map.Entry<Long, ArrayList<BookieSocketAddress>> e : md.getEnsembles().entrySet()) {
                HashSet<BookieSocketAddress> set = new HashSet<BookieSocketAddress>();
                long fragment = e.getKey();

                for (BookieSocketAddress addr : e.getValue()) {
                    if (set.contains(addr)) {
                        LOG.error("Dupe " + addr + " found in ensemble for fragment " + fragment
                                + " of ledger " + lh.getId() + " : " + e.getValue());
                        numDupes++;
                    }
                    set.add(addr);
                }
            }
        }
        return numDupes > 0;
    }

    /**
     * Test recoverying the closed ledgers when the failed bookie server is in the last ensemble
     */
    @Test
    public void testBookieRecoveryOnClosedLedgers() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        closeLedgers(lhs);

        // Shutdown last bookie server in last ensemble
        ArrayList<BookieSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        BookieSocketAddress bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        BookieSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
               + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill, bookieDest);
        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
            try {
                lh.close();
            } catch (BKException.BKLedgerClosedException e) {
                // those ledger handle already closed.
            }
        }
    }

    @Test
    public void testBookieRecoveryOnOpenedLedgers() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        ArrayList<BookieSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        BookieSocketAddress bookieToKill = lastEnsemble.get(lastEnsemble.size() - 1);
        killBookie(bookieToKill);

        // start a new bookie
        startNewBookie();

        BookieSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieToKill
               + ") and replicate it to a random available one");

        bkAdmin.recoverBookieData(bookieToKill, bookieDest);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
        }

        try {
            // we can't write entries
            writeEntriestoLedgers(numMsgs, 0, lhs);
            fail("should not reach here");
        } catch (Exception e) {
        }
    }

    @Test
    public void testBookieRecoveryOnInRecoveryLedger() throws Exception {
        int numMsgs = 10;
        // Create the ledgers
        int numLedgers = 1;
        List<LedgerHandle> lhs = createLedgers(numLedgers, 2, 2);

        // Write the entries for the ledgers with dummy values
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        ArrayList<BookieSocketAddress> lastEnsemble = lhs.get(0).getLedgerMetadata().getEnsembles()
                                                       .entrySet().iterator().next().getValue();
        // removed bookie
        BookieSocketAddress bookieToKill = lastEnsemble.get(0);
        killBookie(bookieToKill);
        // temp failure
        BookieSocketAddress bookieToKill2 = lastEnsemble.get(1);
        ServerConfiguration conf2 = killBookie(bookieToKill2);

        // start a new bookie
        startNewBookie();

        // open these ledgers
        for (LedgerHandle oldLh : lhs) {
            try {
                bkc.openLedger(oldLh.getId(), digestType, baseClientConf.getBookieRecoveryPasswd());
                fail("Should have thrown exception");
            } catch (Exception e) {
            }
        }

        try {
            bkAdmin.recoverBookieData(bookieToKill, null);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }

        // restart failed bookie
        bs.add(startBookie(conf2));
        bsConfs.add(conf2);

        // recover them
        bkAdmin.recoverBookieData(bookieToKill, null);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs));
        }

        // open ledgers to read metadata
        List<LedgerHandle> newLhs = openLedgers(lhs);
        for (LedgerHandle newLh : newLhs) {
            // first ensemble should contains bookieToKill2 and not contain bookieToKill
            Map.Entry<Long, ArrayList<BookieSocketAddress>> entry =
                newLh.getLedgerMetadata().getEnsembles().entrySet().iterator().next();
            assertFalse(entry.getValue().contains(bookieToKill));
            assertTrue(entry.getValue().contains(bookieToKill2));
        }

    }

    @Test
    public void testAsyncBookieRecoveryToRandomBookiesNotEnoughBookies() throws Exception {
        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        int initialPort = bsConfs.get(0).getBookiePort();
        bs.get(0).shutdown();
        bs.remove(0);

        // Call the async recover bookie method.
        BookieSocketAddress bookieSrc = new BookieSocketAddress(InetAddress.getLocalHost().getHostAddress(), initialPort);
        BookieSocketAddress bookieDest = null;
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                 + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        try {
            bkAdmin.recoverBookieData(bookieSrc, null);
            fail("Should have thrown exception");
        } catch (BKException.BKLedgerRecoveryException bke) {
            // correct behaviour
        }
    }

    @Test
    public void testSyncBookieRecoveryToRandomBookiesCheckForDupes() throws Exception {
        Random r = new Random();

        // Create the ledgers
        int numLedgers = 3;
        List<LedgerHandle> lhs = createLedgers(numLedgers, numBookies, 2);

        // Write the entries for the ledgers with dummy values.
        int numMsgs = 10;
        writeEntriestoLedgers(numMsgs, 0, lhs);

        // Shutdown the first bookie server
        LOG.info("Finished writing all ledger entries so shutdown one of the bookies.");
        int removeIndex = r.nextInt(bs.size());
        BookieSocketAddress bookieSrc = bs.get(removeIndex).getLocalAddress();
        bs.get(removeIndex).shutdown();
        bs.remove(removeIndex);

        // Startup new bookie server
        startNewBookie();

        // Write some more entries for the ledgers so a new ensemble will be
        // created for them.
        writeEntriestoLedgers(numMsgs, numMsgs, lhs);

        // Call the async recover bookie method.
        LOG.info("Now recover the data on the killed bookie (" + bookieSrc
                 + ") and replicate it to a random available one");
        // Initiate the sync object
        sync.value = false;
        bkAdmin.recoverBookieData(bookieSrc, null);

        assertFalse("Dupes exist in ensembles", findDupesInEnsembles(lhs));

        // Write some more entries to ensure fencing hasn't broken stuff
        writeEntriestoLedgers(numMsgs, numMsgs*2, lhs);

        for (LedgerHandle lh : lhs) {
            assertTrue("Not fully replicated", verifyFullyReplicated(lh, numMsgs*3));
            lh.close();
        }
    }

    @Test
    public void recoverWithoutPasswordInConf() throws Exception {
        byte[] passwdCorrect = "AAAAAA".getBytes();
        byte[] passwdBad = "BBBBBB".getBytes();
        DigestType digestCorrect = digestType;
        DigestType digestBad = (digestType == DigestType.MAC) ? DigestType.CRC32 : DigestType.MAC;

        LedgerHandle lh = bkc.createLedger(3, 2, digestCorrect, passwdCorrect);
        long ledgerId = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("foobar".getBytes());
        }
        lh.close();

        BookieSocketAddress bookieSrc = bs.get(0).getLocalAddress();
        bs.get(0).shutdown();
        bs.remove(0);
        startNewBookie();

        // Check that entries are missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with bad password in conf
        // This is fine, because it only falls back to the configured
        // password if the password info is missing from the metadata
        ClientConfiguration adminConf = new ClientConfiguration();
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        adminConf.setBookieRecoveryDigestType(digestCorrect);
        adminConf.setBookieRecoveryPasswd(passwdBad);

        BookKeeperAdmin bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc, null);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should be back to fully replication", verifyFullyReplicated(lh, 100));
        lh.close();

        bookieSrc = bs.get(0).getLocalAddress();
        bs.get(0).shutdown();
        bs.remove(0);
        startNewBookie();

        // Check that entries are missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with no password in conf
        adminConf = new ClientConfiguration();
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());

        bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc, null);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should be back to fully replication", verifyFullyReplicated(lh, 100));
        lh.close();
    }

    /**
     * Test that when we try to recover a ledger which doesn't have
     * the password stored in the configuration, we don't succeed
     */
    @Test
    public void ensurePasswordUsedForOldLedgers() throws Exception {
        // stop all bookies
        // and wipe the ledger layout so we can use an old client
        zkUtil.getZooKeeperClient().delete("/ledgers/LAYOUT", -1);

        byte[] passwdCorrect = "AAAAAA".getBytes();
        byte[] passwdBad = "BBBBBB".getBytes();
        DigestType digestCorrect = digestType;
        DigestType digestBad = digestCorrect == DigestType.MAC ? DigestType.CRC32 : DigestType.MAC;

        org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType digestCorrect410
            = org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper.DigestType.valueOf(digestType.toString());

        org.apache.bk_v4_1_0.bookkeeper.conf.ClientConfiguration c
            = new org.apache.bk_v4_1_0.bookkeeper.conf.ClientConfiguration();
        c.setZkServers(zkUtil.getZooKeeperConnectString())
            .setLedgerManagerType(
                    ledgerManagerFactory.equals("org.apache.bookkeeper.meta.FlatLedgerManagerFactory") ?
                    "flat" : "hierarchical");

        // create client to set up layout, close it, restart bookies, and open a new client.
        // the new client is necessary to ensure that it has all the restarted bookies in the
        // its available bookie list
        org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper bkc41
            = new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(c);
        bkc41.close();
        restartBookies();
        bkc41 = new org.apache.bk_v4_1_0.bookkeeper.client.BookKeeper(c);

        org.apache.bk_v4_1_0.bookkeeper.client.LedgerHandle lh41
            = bkc41.createLedger(3, 2, digestCorrect410, passwdCorrect);
        long ledgerId = lh41.getId();
        for (int i = 0; i < 100; i++) {
            lh41.addEntry("foobar".getBytes());
        }
        lh41.close();
        bkc41.close();

        // Startup a new bookie server
        int newBookiePort = startNewBookie();
        int removeIndex = 0;
        BookieSocketAddress bookieSrc = bs.get(removeIndex).getLocalAddress();
        bs.get(removeIndex).shutdown();
        bs.remove(removeIndex);

        // Check that entries are missing
        LedgerHandle lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        // Try to recover with bad password in conf
        // if the digest type is MAC
        // for CRC32, the password is only checked
        // when adding new entries, which recovery will
        // never do
        ClientConfiguration adminConf;
        BookKeeperAdmin bka;
        if (digestCorrect == DigestType.MAC) {
            adminConf = new ClientConfiguration();
            adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
            adminConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
            adminConf.setBookieRecoveryDigestType(digestCorrect);
            adminConf.setBookieRecoveryPasswd(passwdBad);

            bka = new BookKeeperAdmin(adminConf);
            try {
                bka.recoverBookieData(bookieSrc, null);
                fail("Shouldn't be able to recover with wrong password");
            } catch (BKException bke) {
                // correct behaviour
            } finally {
                bka.close();
            }
        }

        // Try to recover with bad digest in conf
        adminConf = new ClientConfiguration();
        adminConf.setZkServers(zkUtil.getZooKeeperConnectString());
        adminConf.setLedgerManagerFactoryClassName(ledgerManagerFactory);
        adminConf.setBookieRecoveryDigestType(digestBad);
        adminConf.setBookieRecoveryPasswd(passwdCorrect);

        bka = new BookKeeperAdmin(adminConf);
        try {
            bka.recoverBookieData(bookieSrc, null);
            fail("Shouldn't be able to recover with wrong digest");
        } catch (BKException bke) {
            // correct behaviour
        } finally {
            bka.close();
        }

        // Check that entries are still missing
        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertFalse("Should be entries missing", verifyFullyReplicated(lh, 100));
        lh.close();

        adminConf.setBookieRecoveryDigestType(digestCorrect);
        adminConf.setBookieRecoveryPasswd(passwdCorrect);

        bka = new BookKeeperAdmin(adminConf);
        bka.recoverBookieData(bookieSrc, null);
        bka.close();

        lh = bkc.openLedgerNoRecovery(ledgerId, digestCorrect, passwdCorrect);
        assertTrue("Should have recovered everything", verifyFullyReplicated(lh, 100));
        lh.close();
    }
}
