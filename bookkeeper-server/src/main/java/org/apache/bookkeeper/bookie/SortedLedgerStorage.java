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
import org.apache.bookkeeper.util.DaemonThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SortedLedgerStorage extends InterleavedLedgerStorage
        implements LedgerStorage, CacheCallback, SkipListFlusher {
    private final static Logger LOG = LoggerFactory.getLogger(SortedLedgerStorage.class);

    private EntryMemTable memTable;
    private final ScheduledExecutorService scheduler;
    private final CacheCallback cacheCallback;

    public SortedLedgerStorage(ServerConfiguration conf, ActiveLedgerManager activeLedgerManager,
                               LedgerDirsManager ledgerDirsManager, final CacheCallback cb,
                               final CheckpointProgress progress) throws IOException {
        super(conf, activeLedgerManager, ledgerDirsManager);
        this.memTable = new EntryMemTable(conf, progress);
        this.scheduler = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
        this.cacheCallback = cb;
    }

    @Override
    public void shutdown() throws InterruptedException {
        // Wait for any jobs currently scheduled to be completed and then shut down.
        scheduler.shutdown();
        if (!scheduler.awaitTermination(3, TimeUnit.SECONDS)) {
            scheduler.shutdownNow();
        }
        super.shutdown();
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        // Done this way because checking the skip list is an O(logN) operation compared to
        // the O(1) for the ledgerCache.
        if (!super.ledgerExists(ledgerId)) {
            EntryKeyValue kv = memTable.getLastEntry(ledgerId);
            if (null == kv) {
                return super.ledgerExists(ledgerId);
            }
        }
        return true;
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
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
        ByteBuffer buffToRet = getEntryImpl(ledgerId, entryId);
        if (null == buffToRet) {
            EntryKeyValue kv = memTable.getEntry(ledgerId, entryId);
            if (null == kv) {
                // The entry might have been flushed since we last checked, so query the ledger cache again.
                // If the entry truly doesn't exist, then this will throw a NoEntryException
                buffToRet = super.getEntry(ledgerId, entryId);
            } else {
                buffToRet = kv.getValueAsByteBuffer();
            }
        }
        // buffToRet will not be null when we reach here.
        return buffToRet;
    }

    @Override
    public boolean isFlushRequired() {
        return !memTable.isEmpty() || super.isFlushRequired();
    }

    private void flushInternal(final LogMark logMark) throws IOException {
        memTable.flush(this, logMark);
        super.flush();
    }

    @Override
    public void flush(final LogMark logMark) throws IOException {
        flushInternal(logMark);
    }

    // SkipListFlusher functions.
    @Override
    public void process(long ledgerId, long entryId, ByteBuffer buffer) throws IOException {
        processEntry(ledgerId, entryId, buffer);
    }

    @Override
    synchronized public void flush() throws IOException {
        flushInternal(LogMark.MAX_VALUE);
    }

    // CacheCallback functions.
    @Override
    public void onSizeLimitReached() throws IOException {
        scheduler.submit(new Runnable() {
            @Override
            public void run() {
            try {
                if (memTable.flush(SortedLedgerStorage.this) != 0) {
                    cacheCallback.onSizeLimitReached();
                }
            } catch (IOException e) {
                LOG.error("IOException thrown while flushing skip list cache.", e);
            }
            }
        });
    }
}
