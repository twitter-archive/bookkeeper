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
package org.apache.bookkeeper.replication;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CountDownLatch;
import java.util.HashMap;
import java.util.List;
import java.util.LinkedList;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieAccessor;
import org.apache.bookkeeper.bookie.LedgerCacheImpl;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This test verifies that the period check on the auditor
 * will pick up on missing data in the client
 */
public class AuditorPeriodicCheckTest extends BookKeeperClusterTestCase {
    private final static Logger LOG = LoggerFactory
            .getLogger(AuditorPeriodicCheckTest.class);

    private HashMap<String, AuditorElector> auditorElectors = new HashMap<String, AuditorElector>();
    private List<ZooKeeper> zkClients = new LinkedList<ZooKeeper>();

    private final static int CHECK_INTERVAL = 1; // run every second

    public AuditorPeriodicCheckTest() {
        super(3);
        baseConf.setPageLimit(1); // to make it easy to push ledger out of cache
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = new ServerConfiguration(bsConfs.get(i));
            conf.setAuditorPeriodicCheckInterval(CHECK_INTERVAL);

            String addr = bs.get(i).getLocalAddress().toString();

            ZooKeeper zk = ZooKeeperClient.createConnectedZooKeeper(
                    zkUtil.getZooKeeperConnectString(), 10000);
            zkClients.add(zk);

            AuditorElector auditorElector = new AuditorElector(addr,
                                                               conf, zk);
            auditorElectors.put(addr, auditorElector);
            auditorElector.start();
            LOG.debug("Starting Auditor Elector");
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }
        for (ZooKeeper zk : zkClients) {
            zk.close();
        }
        zkClients.clear();
        super.tearDown();
    }

    /**
     * test that the period checker will detect corruptions in
     * the bookie index files
     */
    @Test(timeout=30000)
    public void testIndexCorruption() throws Exception {
        LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bsConfs.get(0), zkc);
        LedgerUnderreplicationManager underReplicationManager = mFactory.newLedgerUnderreplicationManager();

        LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        long ledgerToCorrupt = lh.getId();
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        // push ledgerToCorrupt out of page cache (bookie is configured to only use 1 page)
        lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
        for (int i = 0; i < 100; i++) {
            lh.addEntry("testdata".getBytes());
        }
        lh.close();

        BookieAccessor.forceFlush(bs.get(0).getBookie());

        File ledgerDir = bsConfs.get(0).getLedgerDirs()[0];
        ledgerDir = Bookie.getCurrentDirectory(ledgerDir);

        // corrupt of entryLogs
        File index = new File(ledgerDir, LedgerCacheImpl.getLedgerName(ledgerToCorrupt));
        LOG.info("file to corrupt{}" , index);
        ByteBuffer junk = ByteBuffer.allocate(1024*1024);
        FileOutputStream out = new FileOutputStream(index);
        out.getChannel().write(junk);
        out.close();

        long underReplicatedLedger = -1;
        for (int i = 0; i < 10; i++) {
            underReplicatedLedger = underReplicationManager.pollLedgerToRereplicate();
            if (underReplicatedLedger != -1) {
                break;
            }
            Thread.sleep(CHECK_INTERVAL * 1000);
        }
        assertEquals("Ledger should be under replicated", ledgerToCorrupt, underReplicatedLedger);
        underReplicationManager.close();
    }

    /**
     * Test that the period check will succeed if a ledger is deleted midway
     */
    @Test(timeout=60000)
    public void testPeriodicCheckWhenLedgerDeleted() throws Exception {
        for (AuditorElector e : auditorElectors.values()) {
            e.shutdown();
        }

        final int numLedgers = 100;
        List<Long> ids = new LinkedList<Long>();
        for (int i = 0; i < numLedgers; i++) {
            LedgerHandle lh = bkc.createLedger(3, 3, DigestType.CRC32, "passwd".getBytes());
            ids.add(lh.getId());
            for (int j = 0; j < 10; j++) {
                lh.addEntry("testdata".getBytes());
            }
            lh.close();
        }
        final Auditor auditor = new Auditor(
                Bookie.getBookieAddress(bsConfs.get(0)).toString(),
                bsConfs.get(0), zkc);
        final AtomicBoolean exceptionCaught = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
                public void run() {
                    try {
                        latch.countDown();
                        for (int i = 0; i < numLedgers; i++) {
                            auditor.checkAllLedgers();
                        }
                    } catch (Exception e) {
                        LOG.error("Caught exception while checking all ledgers", e);
                        exceptionCaught.set(true);
                    }
                }
            };
        t.start();
        latch.await();
        for (Long id : ids) {
            bkc.deleteLedger(id);
        }
        t.join();
        assertFalse("Shouldn't have thrown exception", exceptionCaught.get());
    }
}
