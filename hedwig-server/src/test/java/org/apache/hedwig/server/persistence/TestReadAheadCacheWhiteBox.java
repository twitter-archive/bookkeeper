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

import static org.junit.Assert.*;

import java.util.List;

import org.apache.hedwig.protocol.PubSubProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.protobuf.ByteString;
import org.apache.hedwig.HelperMethods;
import org.apache.hedwig.StubCallback;
import org.apache.hedwig.StubScanCallback;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.ConcurrencyUtils;

public class TestReadAheadCacheWhiteBox {
    ByteString topic = ByteString.copyFromUtf8("testTopic");
    final static int NUM_MESSAGES = 10;
    final static int MSG_SIZE = 50;
    List<Message> messages = HelperMethods.getRandomPublishedMessages(NUM_MESSAGES, MSG_SIZE);
    StubPersistenceManager stubPersistenceManager;
    ReadAheadCache cacheBasedPersistenceManager;
    MyServerConfiguration myConf = new MyServerConfiguration();

    class MyReadAheadCache extends ReadAheadCache {
        public MyReadAheadCache(PersistenceManagerWithRangeScan persistenceManger, ServerConfiguration cfg) {
            super(persistenceManger, cfg);
        }

        @Override
        protected void enqueueWithoutFailure(ByteString topic, final CacheRequest obj) {
            // make it perform in the same thread
            obj.safeRun();
        }
    }

    class MyServerConfiguration extends ServerConfiguration {

        // Note these are set up, so that the size limit will be reached before
        // the count limit
        int readAheadCount = NUM_MESSAGES / 2;
        long readAheadSize = (long) (MSG_SIZE * 2.5);
        long maxCacheSize = Long.MAX_VALUE;

        int getReadAheadMaxCountOfMessages() {
            return (int)Math.ceil(myConf.readAheadSize / (MSG_SIZE + 0.0));
        }

        @Override
        public int getReadAheadCount() {
            return readAheadCount;
        }

        @Override
        public long getReadAheadSizeBytes() {
            return readAheadSize;
        }

        @Override
        public long getMaximumCacheSize() {
            return maxCacheSize;
        }

        @Override
        public int getNumReadAheadCacheThreads() {
            return 1;
        }
    }

    @Before
    public void setUp() throws Exception {
        stubPersistenceManager = new StubPersistenceManager();
        cacheBasedPersistenceManager = new MyReadAheadCache(stubPersistenceManager, myConf).start();
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testPersistMessage() throws Exception {
        StubCallback<PubSubProtocol.MessageSeqId> callback = new StubCallback<PubSubProtocol.MessageSeqId>();
        PersistRequest request = new PersistRequest(topic, messages.get(0), callback, null);

        stubPersistenceManager.failure = true;
        cacheBasedPersistenceManager.persistMessage(request);
        assertNotNull(ConcurrencyUtils.take(callback.queue).right());

        CacheKey key = new CacheKey(topic, cacheBasedPersistenceManager.getCurrentSeqIdForTopic(topic)
                                    .getLocalComponent());
        assertFalse(null != cacheBasedPersistenceManager.cache.getIfPresent(key));

        stubPersistenceManager.failure = false;
        persistMessage(messages.get(0));
    }

    private void persistMessage(Message msg) throws Exception {
        StubCallback<PubSubProtocol.MessageSeqId> callback = new StubCallback<PubSubProtocol.MessageSeqId>();
        PersistRequest request = new PersistRequest(topic, msg, callback, null);
        cacheBasedPersistenceManager.persistMessage(request);
        assertNotNull(ConcurrencyUtils.take(callback.queue).left());
        CacheKey key = new CacheKey(topic, cacheBasedPersistenceManager.getCurrentSeqIdForTopic(topic)
                                    .getLocalComponent());
        CacheValue cacheValue = cacheBasedPersistenceManager.cache.getIfPresent(key);

        assertNotNull(cacheValue);
        assertFalse(cacheValue.isStub());
        assertTrue(HelperMethods.areEqual(cacheValue.getMessage(), msg));
    }

    @Test
    public void testScanSingleMessage() throws Exception {
        StubScanCallback callback = new StubScanCallback();
        ScanRequest request = new ScanRequest(topic, 1, callback, null);
        stubPersistenceManager.failure = true;

        cacheBasedPersistenceManager.scanSingleMessage(request);
        assertTrue(callback.isFailed());
        assertTrue(0 == cacheBasedPersistenceManager.cache.size());

        stubPersistenceManager.failure = false;
        cacheBasedPersistenceManager.scanSingleMessage(request);
        assertTrue(myConf.readAheadCount == cacheBasedPersistenceManager.cache.size());

        persistMessage(messages.get(0));
        assertTrue(callback.isSuccess());
    }

    @Test
    public void testDoReadAhead() throws Exception {
        StubScanCallback callback = new StubScanCallback();
        ScanRequest request = new ScanRequest(topic, 1, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);

        assertEquals(myConf.readAheadCount, cacheBasedPersistenceManager.cache.size());

        request = new ScanRequest(topic, myConf.readAheadCount / 2 - 1, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);
        assertEquals(myConf.readAheadCount, cacheBasedPersistenceManager.cache.size());

        request = new ScanRequest(topic, myConf.readAheadCount / 2 + 2, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);
        assertEquals((myConf.readAheadCount + (myConf.readAheadCount + 1) /2), cacheBasedPersistenceManager.cache.size());
    }

    @Test
    public void testReadAheadSizeLimit() throws Exception {
        for (Message m : messages) {
            persistMessage(m);
        }
        cacheBasedPersistenceManager.removeAllMessagesFromCache();
        StubScanCallback callback = new StubScanCallback();
        ScanRequest request = new ScanRequest(topic, 1, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);

        assertTrue(callback.isSuccess());
        assertEquals(myConf.getReadAheadMaxCountOfMessages(),
                cacheBasedPersistenceManager.cache.size());
    }

    @Test
    public void testDoReadAheadStartingFrom() throws Exception {
        persistMessage(messages.get(0));
        int readAheadCount = 5;
        int start = 1;
        StubScanCallback callback = new StubScanCallback();
        RangeScanRequest readAheadRequest = cacheBasedPersistenceManager.doReadAhead(topic, start,
                readAheadCount, callback, null);
        assertNull(readAheadRequest);

        int end = 100;
        ScanRequest request = new ScanRequest(topic, end, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);

        int pos = 98;
        readAheadRequest = cacheBasedPersistenceManager.doReadAhead(topic, pos, readAheadCount, callback, null);
        assertEquals(readAheadRequest.messageLimit, end - pos);

        end = 200;
        request = new ScanRequest(topic, end, callback, null);
        cacheBasedPersistenceManager.scanSingleMessage(request);

        // too far back
        pos = 150;
        readAheadRequest = cacheBasedPersistenceManager.doReadAhead(topic, pos, readAheadCount, callback, null);
        assertEquals(readAheadRequest.messageLimit, readAheadCount);
    }

    @Test
    public void testAddMessageToCache() {
        CacheKey key = new CacheKey(topic, 1);
        cacheBasedPersistenceManager.addMessageToCache(key, messages.get(0));
        assertEquals(1, cacheBasedPersistenceManager.cache.size());
        assertEquals(MSG_SIZE, cacheBasedPersistenceManager.presentCacheSize.get());

        CacheValue value = cacheBasedPersistenceManager.cache.getIfPresent(key);
        assertTrue(value != null);
    }

    @Test
    public void testRemoveMessageFromCache() throws Exception {
        CacheKey key = new CacheKey(topic, 1);
        cacheBasedPersistenceManager.addMessageToCache(key, messages.get(0));
        cacheBasedPersistenceManager.removeMessageFromCache(key);
        assertTrue(cacheBasedPersistenceManager.cache.size() == 0);
    }

    @Test
    public void testCollectOldCacheEntries() throws Exception {
        int n = 2;
        myConf.maxCacheSize = n * MSG_SIZE;
        PersistenceManagerWithRangeScan persistenceManager = new StubPersistenceManager();
        ReadAheadCache cache = new MyReadAheadCache(persistenceManager, myConf).start();

        int i = 1;
        for (Message m : messages) {
            CacheKey key = new CacheKey(topic, i);
            cache.addMessageToCache(key, m);
            i++;
        }

        assertEquals(n, cache.cache.size());
    }
}
