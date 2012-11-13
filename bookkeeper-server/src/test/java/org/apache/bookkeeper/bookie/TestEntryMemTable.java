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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.bookie;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.HashSet;

import org.junit.Test;
import org.junit.Before;
import java.nio.ByteBuffer;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.conf.ServerConfiguration;

public class TestEntryMemTable implements CacheCallback, SkipListFlusher {
    private static Logger Logger = LoggerFactory.getLogger(Journal.class);
    private EntryMemTable memTable;
    private final Random random = new Random();

    @Before
    public void setUp() throws Exception {
        this.memTable = new EntryMemTable(new ServerConfiguration());
    }

    /**
     * Basic put/get
     * @throws IOException
     * */
    @Test
    public void testBasicOps() throws IOException {
        long ledgerId = 1;
        long entryId = 1;
        byte[] data = new byte[10];
        random.nextBytes(data);
        ByteBuffer buf = ByteBuffer.wrap(data);
        memTable.addEntry(ledgerId, entryId, buf, this);
        buf.rewind();
        EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
        assertTrue(kv.getLedgerId() == ledgerId);
        assertTrue(kv.getEntryId() == entryId);
        assertTrue(kv.getValueAsByteBuffer().equals(buf));
        memTable.flush(this, false);
    }

    /**
     * Process notification that cache size limit reached
     */
    @Override
    public void onSizeLimitReached() throws IOException {
        // No-op
    }

    public void process(long ledgerId, long entryId, ByteBuffer entry)
            throws IOException {
        // No-op
    }

    /**
     * Test read/write across snapshot
     * @throws IOException
     */
    @Test
    public void testScanAcrossSnapshot() throws IOException {
        byte[] data = new byte[10];
        List<EntryKeyValue> keyValues = new ArrayList<EntryKeyValue>();
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 3; ledgerId++) {
                random.nextBytes(data);
                memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this);
                keyValues.add(memTable.getEntry(ledgerId, entryId));
                if (random.nextInt(16) == 0) {
                    memTable.snapshot();
                }
            }
        }

        for (EntryKeyValue kv : keyValues) {
            assertTrue(memTable.getEntry(kv.getLedgerId(), kv.getEntryId()).equals(kv));
        }
        memTable.flush(this, true);
    }

    private class KVFLusher implements SkipListFlusher {
        final HashSet<EntryKeyValue> keyValues;

        KVFLusher(final HashSet<EntryKeyValue> keyValues) {
            this.keyValues = keyValues;
        }

        @Override
        public void process(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
            assertTrue(ledgerId + ":" + entryId + " is duplicate in store!",
                    keyValues.add(new EntryKeyValue(ledgerId, entryId, entry.array())));
        }
    }

    /**
     * Test snapshot/flush interaction
     * @throws IOException
     */
    @Test
    public void testFlushSnapshot() throws IOException {
        HashSet<EntryKeyValue> keyValues = new HashSet<EntryKeyValue>();
        HashSet<EntryKeyValue> flushedKVs = new HashSet<EntryKeyValue>();
        KVFLusher flusher = new KVFLusher(flushedKVs);

        byte[] data = new byte[10];
        for (long entryId = 1; entryId < 100; entryId++) {
            for (long ledgerId = 1; ledgerId < 100; ledgerId++) {
                random.nextBytes(data);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in mem-table!",
                        memTable.addEntry(ledgerId, entryId, ByteBuffer.wrap(data), this) != 0);
                assertTrue(ledgerId + ":" + entryId + " is duplicate in hash-set!",
                        keyValues.add(memTable.getEntry(ledgerId, entryId)));
                if (random.nextInt(16) == 0) {
                    if (memTable.snapshot()) {
                        if (random.nextInt(2) == 0) {
                            memTable.flush(flusher, false);
                        }
                    }
                }
            }
        }

        memTable.flush(flusher, true);
        for (EntryKeyValue kv : keyValues) {
            assertTrue("kv " + kv.toString() + " was not flushed!", flushedKVs.contains(kv));
        }
    }
}

