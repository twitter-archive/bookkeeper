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

import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.jboss.netty.buffer.ChannelBuffer;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class TestAddEntryQuorumTimeout extends BookKeeperClusterTestCase implements AddCallback {

    final static Logger logger = LoggerFactory.getLogger(TestAddEntryQuorumTimeout.class);

    final DigestType digestType;
    final byte[] testPasswd = "".getBytes();

    public TestAddEntryQuorumTimeout() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    private static class SyncObj {
        volatile int counter;
        volatile int rc;
        public SyncObj() {
            counter = 0;
        }
    }

    @Override
    public void addComplete(int rc, LedgerHandle lh, long entryId, Object ctx) {
        SyncObj x = (SyncObj) ctx;
        synchronized (x) {
            x.rc = rc;
            x.counter++;
            x.notify();
        }
    }

    LedgerHandle getTestLedger() throws Exception {
        ClientConfiguration conf = new ClientConfiguration();
        conf.copy(baseClientConf);
        conf.setAddEntryTimeout(10);
        conf.setAddEntryQuorumTimeout(1);
        BookKeeperTestClient bkc = new BookKeeperTestClient(conf);
        return bkc.createLedger(3, 3, 3, digestType, testPasswd);
    }

    @Test(timeout = 60000)
    public void testBasicTimeout() throws Exception {
        LedgerHandle lh = getTestLedger();
        List<InetSocketAddress> curEns = lh.getLedgerMetadata().currentEnsemble;
        byte[] data = "foobar".getBytes();
        lh.addEntry(data);
        sleepBookie(curEns.get(0), 5);
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKAddEntryQuorumTimeoutException ex) {
        }
    }

    @Test(timeout = 60000)
    public void testTimeoutWithPendingOps() throws Exception {
        LedgerHandle lh = getTestLedger();
        List<InetSocketAddress> curEns = lh.getLedgerMetadata().currentEnsemble;
        byte[] data = "foobar".getBytes();

        SyncObj syncObj1 = new SyncObj();
        SyncObj syncObj2 = new SyncObj();
        SyncObj syncObj3 = new SyncObj();

        lh.addEntry(data);
        sleepBookie(curEns.get(0), 5).await();
        lh.asyncAddEntry(data, this, syncObj1);
        lh.asyncAddEntry(data, this, syncObj2);
        lh.asyncAddEntry(data, this, syncObj3);

        synchronized (syncObj1) {
            while (syncObj1.counter < 1) {
                logger.debug("Entries counter = " + syncObj1.counter);
                syncObj1.wait();
            }
            assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj1.rc);
        }
        assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj2.rc);
        assertEquals(BKException.Code.AddEntryQuorumTimeoutException, syncObj3.rc);
    }

    @Test(timeout = 60000)
    public void testLedgerClosedAfterTimeout() throws Exception {
        LedgerHandle lh = getTestLedger();
        List<InetSocketAddress> curEns = lh.getLedgerMetadata().currentEnsemble;
        byte[] data = "foobar".getBytes();
        CountDownLatch b0latch = sleepBookie(curEns.get(0), 5);
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKAddEntryQuorumTimeoutException ex) {
        }
        b0latch.await();
        try {
            lh.addEntry(data);
            fail("should have thrown");
        } catch (BKException.BKLedgerClosedException ex) {
        }
    }
}
