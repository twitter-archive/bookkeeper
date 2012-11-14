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
package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

public class SkipListLedgerStorage extends InterleavedLedgerStorage implements LedgerStorage, CacheCallback, SkipListFlusher {
    private final static Logger LOG = LoggerFactory.getLogger(SkipListLedgerStorage.class);

    private EntryMemTable memTable;
    private final ScheduledExecutorService scheduler;
    private final CacheCallback cacheCallback;

    private static class DaemonThreadFactory implements ThreadFactory {
        private ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        public Thread newThread(Runnable r) {
            Thread thread = defaultThreadFactory.newThread(r);
            thread.setDaemon(true);
            return thread;
        }
    }

    public SkipListLedgerStorage(ServerConfiguration conf, ActiveLedgerManager activeLedgerManager, final CacheCallback cb)
            throws IOException {
        super(conf, activeLedgerManager);
        this.memTable = new EntryMemTable(conf);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
        this.cacheCallback = cb;
    }

    @Override
    public void shutdown() throws InterruptedException {
        // Wait for any jobs currently scheduled to be completed and then shut down.
        scheduler.shutdown();
        super.shutdown();
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // If it exists in the skip list, return. Else query the ledger cache.
        // TODO: check skiplist
        return super.ledgerExists(ledgerId);
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        // TODO: Move this to the function calling addEntry
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.WRITE_BYTES)
                .add(entry.remaining());
        memTable.addEntry(ledgerId, entryId, entry, this);
        return entryId;
    }

    /**
     * Get the last entry id for a particular ledger.
     * @param ledgerId
     * @return
     */
    private ByteBuffer getLastEntryId(long ledgerId) throws IOException {
        EntryKeyValue kv = memTable.getLastEntry(ledgerId);
        if (null != kv) {
            return kv.getValueAsByteBuffer();
        }
        // If it doesn't exist in the skip list, then fallback to the ledger cache+index.
        return super.getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            return getLastEntryId(ledgerId);
        }
        EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
        if (null != kv) {
            return kv.getValueAsByteBuffer();
        }
        // If it doesn't exist in the skip list, then query the cache+index.
        return super.getEntry(ledgerId, entryId);
    }
}
