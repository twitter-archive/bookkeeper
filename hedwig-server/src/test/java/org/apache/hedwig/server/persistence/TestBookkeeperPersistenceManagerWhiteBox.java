/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.server.persistence;

import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import junit.framework.TestCase;

import org.apache.bookkeeper.client.BookKeeper;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.apache.hedwig.util.Either;

import com.google.protobuf.ByteString;
import org.apache.hedwig.HelperMethods;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.meta.MetadataManagerFactory;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.TrivialOwnAllTopicManager;
import org.apache.hedwig.util.ConcurrencyUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestBookkeeperPersistenceManagerWhiteBox extends TestCase {

    protected static Logger logger =
        LoggerFactory.getLogger(TestBookkeeperPersistenceManagerWhiteBox.class);

    BookKeeperTestBase bktb;
    private final int numBookies = 3;
    BookkeeperPersistenceManager bkpm;
    MetadataManagerFactory mm;
    ServerConfiguration conf;
    ScheduledExecutorService scheduler;
    TopicManager tm;
    ByteString topic = ByteString.copyFromUtf8("topic0");

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();
        bktb = new BookKeeperTestBase(numBookies);
        bktb.setUp();

        conf = new ServerConfiguration();
        scheduler = Executors.newScheduledThreadPool(1);
        tm = new TrivialOwnAllTopicManager(conf, scheduler);

        mm = MetadataManagerFactory.newMetadataManagerFactory(conf, bktb.getZooKeeperClient());

        bkpm = new BookkeeperPersistenceManager(bktb.bk, bktb.readBk, mm, tm, conf, scheduler);
    }

    @Override
    @After
    protected void tearDown() throws Exception {
        mm.shutdown();
        bktb.tearDown();
        super.tearDown();
    }

    @Test
    public void testEmptyDirtyLedger() throws Exception {

        StubCallback<Void> stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        // now abandon, and try another time, the prev ledger should be dirty

        bkpm = new BookkeeperPersistenceManager(new BookKeeper(bktb.getZkHostPort()),new BookKeeper(bktb.getZkHostPort()), mm, tm,
                                                conf, scheduler);
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(0, bkpm.topicInfos.get(topic).ledgerRanges.size());
    }

    @Test
    public void testNonEmptyDirtyLedger() throws Exception {

        Random r = new Random();
        int NUM_MESSAGES_TO_TEST = 100;
        int SIZE_OF_MESSAGES_TO_TEST = 100;
        int index = 0;
        int numPrevLedgers = 0;
        List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST,
                                 SIZE_OF_MESSAGES_TO_TEST);

        while (index < messages.size()) {

            StubCallback<Void> stubCallback = new StubCallback<Void>();
            bkpm.acquiredTopic(topic, stubCallback, null);
            assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
            assertEquals(numPrevLedgers, bkpm.topicInfos.get(topic).ledgerRanges.size());

            StubCallback<PubSubProtocol.MessageSeqId> persistCallback = new StubCallback<PubSubProtocol.MessageSeqId>();
            bkpm.persistMessage(new PersistRequest(topic, messages.get(index), persistCallback, null));
            assertEquals(index + 1, ConcurrencyUtils.take(persistCallback.queue).left().getLocalComponent());

            index++;
            // once in every 10 times, give up ledger
            if (r.nextInt(10) == 9) {
                // should not release topic when the message is last message
                // otherwise when we call scan, bookkeeper persistence manager doesn't own the topic
                if (index < messages.size()) {
                    // Make the bkpm lose its memory
                    bkpm.topicInfos.clear();
                    numPrevLedgers++;
                }
            }
        }

        // Lets scan now
        StubScanCallback scanCallback = new StubScanCallback();
        bkpm.scanMessages(new RangeScanRequest(topic, 1, NUM_MESSAGES_TO_TEST, Long.MAX_VALUE, scanCallback, null));
        for (int i = 0; i < messages.size(); i++) {
            Message scannedMessage = ConcurrencyUtils.take(scanCallback.queue).left();
            assertTrue(messages.get(i).getBody().equals(scannedMessage.getBody()));
            assertEquals(i + 1, scannedMessage.getMsgId().getLocalComponent());
        }
        assertTrue(StubScanCallback.END_MESSAGE == ConcurrencyUtils.take(scanCallback.queue).left());

    }

    static final long maxEntriesPerLedger = 10;

    class ChangeLedgerServerConfiguration extends ServerConfiguration {
        @Override
        public long getMaxEntriesPerLedger() {
            return maxEntriesPerLedger;
        }
    }

    @Test
    public void testSyncChangeLedgers() throws Exception {
        int NUM_MESSAGES_TO_TEST = 101;
        int SIZE_OF_MESSAGES_TO_TEST = 100;
        int index = 0;
        List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST,
                                 SIZE_OF_MESSAGES_TO_TEST);

        bkpm = new BookkeeperPersistenceManager(bktb.bk, bktb.readBk, mm, tm,
                                                new ChangeLedgerServerConfiguration(), scheduler);

        // acquire the topic
        StubCallback<Void> stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(0, bkpm.topicInfos.get(topic).ledgerRanges.size());

        while (index < messages.size()) {
            logger.debug("Persist message {}", (index + 1));
            StubCallback<MessageSeqId> persistCallback = new StubCallback<MessageSeqId>();
            bkpm.persistMessage(new PersistRequest(topic, messages.get(index), persistCallback, null));
            assertEquals(index + 1, ConcurrencyUtils.take(persistCallback.queue).left().getLocalComponent());

            index++;
            if (index % maxEntriesPerLedger == 1) {
                assertEquals(index / maxEntriesPerLedger, bkpm.topicInfos.get(topic).ledgerRanges.size());
            }
        }
        assertEquals(NUM_MESSAGES_TO_TEST / maxEntriesPerLedger, bkpm.topicInfos.get(topic).ledgerRanges.size());

        // Lets scan now
        StubScanCallback scanCallback = new StubScanCallback();
        bkpm.scanMessages(new RangeScanRequest(topic, 1, NUM_MESSAGES_TO_TEST, Long.MAX_VALUE, scanCallback, null));
        for (int i = 0; i < messages.size(); i++) {
            Message scannedMessage = ConcurrencyUtils.take(scanCallback.queue).left();
            assertTrue(messages.get(i).getBody().equals(scannedMessage.getBody()));
            assertEquals(i + 1, scannedMessage.getMsgId().getLocalComponent());
        }
        assertTrue(StubScanCallback.END_MESSAGE == ConcurrencyUtils.take(scanCallback.queue).left());

        // Make the bkpm lose its memory
        bkpm.topicInfos.clear();

        // acquire the topic again
        stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(NUM_MESSAGES_TO_TEST / maxEntriesPerLedger + 1, bkpm.topicInfos.get(topic).ledgerRanges.size());
    }

    class OrderCheckingCallback extends StubCallback<MessageSeqId> {
        long curMsgId;
        int numMessages;
        int numProcessed;
        int numSuccess;
        int numFailed;

        OrderCheckingCallback(long startMsgId, int numMessages) {
            this.curMsgId = startMsgId;
            this.numMessages = numMessages;
            numProcessed = numSuccess = numFailed = 0;
        }

        @Override
        public void operationFailed(Object ctx, final PubSubException exception) {
            synchronized (this) {
                ++numFailed;
                ++numProcessed;
                if (numProcessed == numMessages) {
                    MessageSeqId.Builder seqIdBuilder =
                        MessageSeqId.newBuilder().setLocalComponent(curMsgId);
                    super.operationFinished(ctx, seqIdBuilder.build());
                }
            }
        }

        @Override
        public void operationFinished(Object ctx, final MessageSeqId seqId) {
            synchronized(this) {
                long msgId = seqId.getLocalComponent();
                if (msgId == curMsgId) {
                    ++curMsgId;
                }
                ++numSuccess;
                ++numProcessed;
                if (numProcessed == numMessages) {
                    MessageSeqId.Builder seqIdBuilder =
                        MessageSeqId.newBuilder().setLocalComponent(curMsgId);
                    super.operationFinished(ctx, seqIdBuilder.build());
                }
            }
        }
    }

    @Test
    public void testAsyncChangeLedgers() throws Exception {
        int NUM_MESSAGES_TO_TEST = 101;
        int SIZE_OF_MESSAGES_TO_TEST = 100;
        List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST,
                                 SIZE_OF_MESSAGES_TO_TEST);

        bkpm = new BookkeeperPersistenceManager(bktb.bk, bktb.readBk, mm, tm,
                                                new ChangeLedgerServerConfiguration(), scheduler);

        // acquire the topic
        StubCallback<Void> stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(0, bkpm.topicInfos.get(topic).ledgerRanges.size());

        OrderCheckingCallback persistCallback =
            new OrderCheckingCallback(1, NUM_MESSAGES_TO_TEST);
        for (Message message : messages) {
            bkpm.persistMessage(new PersistRequest(topic, message, persistCallback, null));
        }
        assertEquals(NUM_MESSAGES_TO_TEST + 1,
                     ConcurrencyUtils.take(persistCallback.queue).left().getLocalComponent());
        assertEquals(NUM_MESSAGES_TO_TEST, persistCallback.numSuccess);
        assertEquals(0, persistCallback.numFailed);
        assertEquals(NUM_MESSAGES_TO_TEST / maxEntriesPerLedger,
                     bkpm.topicInfos.get(topic).ledgerRanges.size());

        // ensure the bkpm has the topic before scanning
        stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);

        // Lets scan now
        StubScanCallback scanCallback = new StubScanCallback();
        bkpm.scanMessages(new RangeScanRequest(topic, 1, NUM_MESSAGES_TO_TEST, Long.MAX_VALUE, scanCallback, null));
        for (int i = 0; i < messages.size(); i++) {
            Either<Message,Exception> e = ConcurrencyUtils.take(scanCallback.queue);
            Message scannedMessage = e.left();
            if (scannedMessage == null) {
                throw e.right();
            }

            assertTrue(messages.get(i).getBody().equals(scannedMessage.getBody()));
            assertEquals(i + 1, scannedMessage.getMsgId().getLocalComponent());
        }
        assertTrue(StubScanCallback.END_MESSAGE == ConcurrencyUtils.take(scanCallback.queue).left());

        // Make the bkpm lose its memory
        bkpm.topicInfos.clear();

        // acquire the topic again
        stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(NUM_MESSAGES_TO_TEST / maxEntriesPerLedger + 1,
                     bkpm.topicInfos.get(topic).ledgerRanges.size());
    }

    class ChangeLedgerCallback extends OrderCheckingCallback {
        boolean tearDown = false;
        ChangeLedgerCallback(long startMsgId, int numMessages) {
            super(startMsgId, numMessages);
        }

        @Override
        public void operationFinished(Object ctx, final MessageSeqId msgId) {
            super.operationFinished(ctx, msgId);
            // shutdown bookie server when changing ledger
            // so following requests should fail
            if (msgId.getLocalComponent() >= maxEntriesPerLedger && !tearDown) {
                try {
                    bktb.tearDownOneBookieServer();
                    bktb.tearDownOneBookieServer();
                } catch (Exception e) {
                    logger.error("Failed to tear down bookie server.");
                }
                tearDown = true;
            }
        }
    }

    @Test
    public void testChangeLedgerFailure() throws Exception {
        int NUM_MESSAGES_TO_TEST = 101;
        int SIZE_OF_MESSAGES_TO_TEST = 100;
        List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES_TO_TEST,
                                 SIZE_OF_MESSAGES_TO_TEST);

        bkpm = new BookkeeperPersistenceManager(bktb.bk, bktb.readBk, mm, tm,
                                                new ChangeLedgerServerConfiguration(), scheduler);

        // acquire the topic
        StubCallback<Void> stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(0, bkpm.topicInfos.get(topic).ledgerRanges.size());

        ChangeLedgerCallback persistCallback =
            new ChangeLedgerCallback(1, NUM_MESSAGES_TO_TEST);
        for (Message message : messages) {
            bkpm.persistMessage(new PersistRequest(topic, message, persistCallback, null));
        }
        assertEquals(maxEntriesPerLedger + 1,
                     ConcurrencyUtils.take(persistCallback.queue).left().getLocalComponent());
        assertEquals(maxEntriesPerLedger, persistCallback.numSuccess);
        assertEquals(NUM_MESSAGES_TO_TEST - maxEntriesPerLedger, persistCallback.numFailed);
    }

    @Test
    public void testSeparateReads() throws Exception {
        int NUM_MESSAGE = 15;
        int SIZE_MESSAGE = 100;
        bkpm = new BookkeeperPersistenceManager(bktb.bk, bktb.readBk, mm, tm,
                new ServerConfiguration() {
                    @Override
                    public long getMaxEntriesPerLedger() {
                        return 10;
                    }
                }, scheduler);
        List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGE, SIZE_MESSAGE);
        StubCallback<Void> stubCallback = new StubCallback<Void>();
        bkpm.acquiredTopic(topic, stubCallback, null);
        assertNull(ConcurrencyUtils.take(stubCallback.queue).right());
        assertEquals(0, bkpm.topicInfos.get(topic).ledgerRanges.size());

        // Write 15 messages. The ledger should be changed after writing the first 10.
        // The reads for the first 10 entries should later go to the read only handle.
        long index = 0;
        for (Message message : messages) {
            index++;
            StubCallback<MessageSeqId> persistCallback = new StubCallback<MessageSeqId>();
            bkpm.persistMessage(new PersistRequest(topic, message, persistCallback, null));
            assertEquals(index, ConcurrencyUtils.take(persistCallback.queue).left().getLocalComponent());
        }

        StubScanCallback scanCallback = new StubScanCallback();
        // A scan for all messages should work now.
        bkpm.scanMessages(new RangeScanRequest(topic, 1, NUM_MESSAGE, Long.MAX_VALUE, scanCallback, null));
        for (int i = 0; i < NUM_MESSAGE; i++) {
            assertEquals(i+1, ConcurrencyUtils.take(scanCallback.queue).left().getMsgId().getLocalComponent());
        }
        // Stub scan callback will enqueue an empty message when the scan finishes.
        assertEquals(0, ConcurrencyUtils.take(scanCallback.queue).left().getMsgId().getLocalComponent());
        // Close the read-write handle
        bkpm.bk.close();
        // A read for messages 1 to 10 should not result in an error because it goes through the read only
        // handle.
        int NUM_SCAN = 10;
        bkpm.scanMessages(new RangeScanRequest(topic, 1, NUM_SCAN, Long.MAX_VALUE, scanCallback, null));
        for (int i = 0; i < NUM_SCAN; i++) {
            assertEquals(i+1, ConcurrencyUtils.take(scanCallback.queue).left().getMsgId().getLocalComponent());
        }
        // Stub scan callback will enqueue an empty message when the scan finishes.
        assertEquals(0, ConcurrencyUtils.take(scanCallback.queue).left().getMsgId().getLocalComponent());
        // Scanning message #11 should result in an error because the read-write handle is closed.
        try {
            bkpm.scanMessages(new RangeScanRequest(topic, 11, 1, Long.MAX_VALUE, scanCallback, null));
            // Should not reach here as the handle is closed and should throw a RejectedExecutionException
            // on trying to scan a message
            fail("scanMessages succeeded although read-write client is closed.");
        } catch (RejectedExecutionException e) {
            // Expected
        }
    }
}
