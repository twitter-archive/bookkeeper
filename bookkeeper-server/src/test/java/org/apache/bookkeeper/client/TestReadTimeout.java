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

import org.junit.*;
import java.net.InetSocketAddress;
import java.util.Enumeration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;

import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This unit test tests ledger fencing;
 *
 */
public class TestReadTimeout extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestReadTimeout.class);

    DigestType digestType;

    public TestReadTimeout() {
        // 10 bookies were started.
        super(10);
        this.digestType = DigestType.CRC32;
    }

    @Override
    public void setUp() throws Exception {
        // Ensure that the timeout task doesn't interfere with other tests.
        // Read timeout is 5 seconds
        baseClientConf.setReadTimeout(5);
        super.setUp();
    }

    @Test
    public void testReadTimeout() throws Exception {
        final AtomicBoolean completed = new AtomicBoolean(false);

        LedgerHandle writelh = bkc.createLedger(3,3,digestType, "testPasswd".getBytes());
        String tmp = "Foobar";

        final int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            writelh.addEntry(tmp.getBytes());
        }

        Set<InetSocketAddress> beforeSet = new HashSet<InetSocketAddress>();
        for (InetSocketAddress addr : writelh.getLedgerMetadata().getEnsemble(numEntries)) {
            beforeSet.add(addr);
        }

        final InetSocketAddress bookieToSleep
            = writelh.getLedgerMetadata().getEnsemble(numEntries).get(0);
        int sleeptime = baseClientConf.getReadTimeout()*3;
        CountDownLatch latch = sleepBookie(bookieToSleep, sleeptime);
        latch.await();

        writelh.asyncAddEntry(tmp.getBytes(),
                new AddCallback() {
                    public void addComplete(int rc, LedgerHandle lh,
                                            long entryId, Object ctx) {
                        completed.set(true);
                    }
                }, null);
        Thread.sleep((baseClientConf.getReadTimeout()*3)*1000);
        Assert.assertTrue("Write request did not finish", completed.get());

        Set<InetSocketAddress> afterSet = new HashSet<InetSocketAddress>();
        for (InetSocketAddress addr : writelh.getLedgerMetadata().getEnsemble(numEntries+1)) {
            afterSet.add(addr);
        }
        beforeSet.removeAll(afterSet);
        Assert.assertTrue("Bookie set should not match", beforeSet.size() != 0);
    }

    private boolean testTimeoutTask(long timeoutTaskMillis, int readTimeoutSecInit, int bookieSleepTime,
                                 int readTimeoutSecAfter, long threadSleepMillis, boolean shouldFail) throws Exception {

        baseClientConf.setTimeoutTaskIntervalMillis(timeoutTaskMillis);
        baseClientConf.setReadTimeout(readTimeoutSecInit);

        final AtomicBoolean done = new AtomicBoolean(false);

        LedgerHandle lh = bkc.createLedger(3, 3, digestType, "testPasswd".getBytes());
        String temp = "Foobar";
        lh.addEntry(temp.getBytes());
        // The before set will be the next striped ensemble. We've added only one entry.
        Set<InetSocketAddress> beforeSet = new HashSet<InetSocketAddress>(lh.getLedgerMetadata().getEnsemble(1));
        InetSocketAddress sleepingBookie = beforeSet.iterator().next();
        CountDownLatch latch = sleepBookie(sleepingBookie, bookieSleepTime);
        latch.await();

        baseClientConf.setReadTimeout(readTimeoutSecAfter);
        lh.asyncAddEntry(temp.getBytes(), new AddCallback() {
            @Override
            public void addComplete(int i, LedgerHandle ledgerHandle, long l, Object o) {
                done.set(true);
            }
        }, null);
        Thread.sleep(threadSleepMillis);
        if (done.get() == shouldFail) {
            return false;
        }
        if (!shouldFail && lh.getLedgerMetadata().getEnsemble(1).contains(sleepingBookie)) {
            return false;
        }
        return true;
    }

    /**
     * Initial timeout task interval is 3 seconds. Read timeout is 8 seconds. So the PerChannelBookieClients will
     * have channels with a ReadTimeoutHandler configured for 8 seconds. We write an entry and then put a bookie
     * for that entry to sleep for 10 seconds. We wake up after 4 seconds and because the timeout task should have triggered
     * and changed the ensemble to another set of bookies, our add operation should have completed.
     * @throws Exception
     */
    @Test
    public void testTimeoutTaskSuccess() throws Exception {
        assertTrue("Did not timeout and add entry.", testTimeoutTask(3000L, 8, 10, 1, 5000L, false));
    }

    /**
     * The timeout task should not trigger in this case and neither should the read timeout. The add would not have
     * succeeded.
     * @throws Exception
     */
    @Test
    public void testTimeoutTaskFail() throws Exception {
        assertTrue("Timed out and added entry when we shouldn't have", testTimeoutTask(8000L, 8, 10, 1, 3000L, true));
    }
}
