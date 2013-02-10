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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.bookkeeper.bookie.CheckpointProgress.CheckPoint;
import org.apache.bookkeeper.conf.ServerConfiguration;

/**
 * The EntryMemTable holds in-memory representation to the entries not-yet flushed.
 * When asked to flush, current EntrySkipList is moved to snapshot and is cleared.
 * We continue to serve edits out of new EntrySkipList and backing snapshot until
 * flusher reports in that the flush succeeded. At that point we let the snapshot go.
 */
public class EntryMemTable {
    private static Logger Logger = LoggerFactory.getLogger(Journal.class);

    /**
     * Entry skip list
     */
    static class EntrySkipList extends ConcurrentSkipListMap<EntryKey, EntryKeyValue> {
        final CheckPoint cp;
        static final EntrySkipList EMPTY_VALUE = new EntrySkipList(null) {
            @Override
            public boolean isEmpty() {
                return true;
            }
        };

        EntrySkipList(final CheckPoint cp) {
            super(EntryKey.COMPARATOR);
            this.cp = cp;
        }

        @Override
        public EntryKeyValue put(EntryKey k, EntryKeyValue v) {
            return putIfAbsent(k, v);
        }

        @Override
        public EntryKeyValue putIfAbsent(EntryKey k, EntryKeyValue v) {
            assert k.equals(v);
            return super.putIfAbsent(v, v);
        }

        @Override
        public boolean equals(Object o) {
            return this == o;
        }
    }

    volatile EntrySkipList kvmap;

    // Snapshot of EntryMemTable.  Made for flusher.
    volatile EntrySkipList snapshot;

    final ServerConfiguration conf;
    final CheckpointProgress progress;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    // Used to track own data size
    final AtomicLong size;

    final long skipListSizeLimit;

    SkipListArena allocator;

    private EntrySkipList newSkipList() {
        return new EntrySkipList(progress.requestCheckpoint());
    }

    /**
    * Constructor.
    * @param conf Server configuration
    */
    public EntryMemTable(final ServerConfiguration conf, final CheckpointProgress progress) {
        this.progress = progress;
        this.kvmap = newSkipList();
        this.snapshot = EntrySkipList.EMPTY_VALUE;
        this.conf = conf;
        this.size = new AtomicLong(0);
        this.allocator = new SkipListArena(conf);
        // skip list size limit
        this.skipListSizeLimit = conf.getSkipListSizeLimit();
    }

    void dump() {
        for (EntryKey key: this.kvmap.keySet()) {
            Logger.info(key.toString());
        }
        for (EntryKey key: this.snapshot.keySet()) {
            Logger.info(key.toString());
        }
    }

    CheckPoint snapshot() throws IOException {
        return snapshot(CheckPoint.MAX);
    }

    /**
     * Snapshot current EntryMemTable. if given <i>oldCp</i> is older than current checkpoint,
     * we don't do any snapshot. If snapshot happened, we return the checkpoint of the snapshot.
     *
     * @param oldCp
     *          checkpoint
     * @return checkpoint of the snapshot, null means no snapshot
     * @throws IOException
     */
    CheckPoint snapshot(CheckPoint oldCp) throws IOException {
        CheckPoint cp = null;
        // No-op if snapshot currently has entries
        if (this.snapshot.isEmpty()) {
            final long startTimeMillis = MathUtils.now();
            this.lock.writeLock().lock();
            try {
                if (this.snapshot.isEmpty() && !this.kvmap.isEmpty()
                        && this.kvmap.cp.compareTo(oldCp) < 0) {
                    this.snapshot = this.kvmap;
                    this.kvmap = newSkipList();
                    // get the checkpoint of the memtable.
                    cp = this.kvmap.cp;
                    // Reset heap to not include any keys
                    this.size.set(0);
                    // Reset allocator so we get a fresh buffer for the new EntryMemTable
                    this.allocator = new SkipListArena(conf);
                }
            } finally {
                this.lock.writeLock().unlock();
            }

            long latencyMillis = MathUtils.now() - startTimeMillis;
            if (null != cp) {
                ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                        .SKIP_LIST_SNAPSHOT).registerSuccessfulEvent(latencyMillis);
            } else {
                ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                        .SKIP_LIST_SNAPSHOT).registerFailedEvent(latencyMillis);
            }
        }
        return cp;
    }

    /**
     * Flush snapshot and clear it.
     * Only this function change non-empty this.snapshot.
     */
    long flush(final SkipListFlusher flusher) throws IOException {
        long size = 0;
        if (!this.snapshot.isEmpty()) {
            final long startTimeMillis = MathUtils.now();
            synchronized (this) {
                EntrySkipList keyValues = this.snapshot;
                if (!keyValues.isEmpty()) {
                    for (EntryKey key : keyValues.keySet()) {
                        EntryKeyValue kv = (EntryKeyValue)key;
                        size += kv.getLength();
                        flusher.process(kv.getLedgerId(), kv.getEntryId(), kv.getValueAsByteBuffer());
                    }
                    ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                            BookkeeperServerSimpleStatType.SKIP_LIST_FLUSH_BYTES).add(size);
                    clearSnapshot(keyValues);
                }
            }

            if (size > 0) {
                ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                        .SKIP_LIST_FLUSH).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
            } else {
                ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                        .SKIP_LIST_FLUSH).registerFailedEvent(MathUtils.now() - startTimeMillis);
            }
        }

        return size;
    }

    /**
     * Flush memtable until checkpoint.
     *
     * @param checkpoint
     *          all data before this checkpoint need to be flushed.
     */
    public long flush(SkipListFlusher flusher, CheckPoint checkpoint) throws IOException {
        long size = flush(flusher);
        if (null != snapshot(checkpoint)) {
            size += flush(flusher);
        }
        return size;
    }

    /**
     * The passed snapshot was successfully persisted; it can be let go.
     * @param keyValues The snapshot to clean out.
     * @see {@link #snapshot()}
     */
    private void clearSnapshot(final EntrySkipList keyValues) {
        // Caller makes sure that keyValues not empty
        assert !keyValues.isEmpty();
        final long startTimeMillis = MathUtils.now();
        this.lock.writeLock().lock();
        try {
            // create a new snapshot and let the old one go.
            assert this.snapshot == keyValues;
            this.snapshot = EntrySkipList.EMPTY_VALUE;
        } finally {
            this.lock.writeLock().unlock();
        }
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_CLEAR_SNAPSHOT).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
    }

    /**
     * Throttling writer w/ 1 ms delay
     */
    private void throttleWriters() {
        final long startTimeMillis = MathUtils.now();
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_THROTTLING).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
    }

    /**
    * Write an update
    * @param entry
    * @return approximate size of the passed key and value.
    */
    public long addEntry(long ledgerId, long entryId, final ByteBuffer entry, final CacheCallback cb)
            throws IOException {
        long size = 0;
        final long startTimeMillis = MathUtils.now();
        if (isSizeLimitReached()) {
            CheckPoint cp = snapshot();
            if (null != cp) {
                cb.onSizeLimitReached(cp);
            } else {
                throttleWriters();
            }
        }

        this.lock.readLock().lock();
        try {
            EntryKeyValue toAdd = cloneWithAllocator(ledgerId, entryId, entry);
            size = internalAdd(toAdd);
        } finally {
            this.lock.readLock().unlock();
        }

        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_PUT_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
        return size;
    }

    /**
    * Internal version of add() that doesn't clone KVs with the
    * allocator, and doesn't take the lock.
    *
    * Callers should ensure they already have the read lock taken
    */
    private long internalAdd(final EntryKeyValue toAdd) throws IOException {
        long sizeChange = 0;
        if (kvmap.putIfAbsent(toAdd, toAdd) == null) {
            sizeChange = toAdd.getLength();
            size.addAndGet(sizeChange);
        }
        return sizeChange;
    }

    private EntryKeyValue newEntry(long ledgerId, long entryId, final ByteBuffer entry) {
        byte[] buf;
        int offset = 0;
        int length = entry.remaining();

        if (entry.hasArray()) {
            buf = entry.array();
            offset = entry.arrayOffset();
        }
        else {
            buf = new byte[length];
            entry.get(buf);
        }
        return new EntryKeyValue(ledgerId, entryId, buf, offset, length);
    }

    private EntryKeyValue cloneWithAllocator(long ledgerId, long entryId, final ByteBuffer entry) {
        int len = entry.remaining();
        SkipListArena.MemorySlice alloc = allocator.allocateBytes(len);
        if (alloc == null) {
            // The allocation was too large, allocator decided
            // not to do anything with it.
            return newEntry(ledgerId, entryId, entry);
        }

        assert alloc != null && alloc.getData() != null;
        entry.get(alloc.getData(), alloc.getOffset(), len);
        return new EntryKeyValue(ledgerId, entryId, alloc.getData(), alloc.getOffset(), len);
    }

    /**
     * Find the entry with given key
     * @param ledgerId
     * @param entryId
     * @return the entry kv or null if none found.
     */
    public EntryKeyValue getEntry(long ledgerId, long entryId) throws IOException {
        EntryKey key = new EntryKey(ledgerId, entryId);
        EntryKeyValue value = null;
        long startTimeMillis = MathUtils.now();
        this.lock.readLock().lock();
        try {
            value = this.kvmap.get(key);
            if (value == null) {
                value = this.snapshot.get(key);
            }
        } finally {
            this.lock.readLock().unlock();
        }
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_GET_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);

        return value;
    }

    /**
     * Find the last entry with the given ledger key
     * @param ledgerId
     * @return the entry kv or null if none found.
     */
    public EntryKeyValue getLastEntry(long ledgerId) throws IOException {
        EntryKey result = null;
        EntryKey key = new EntryKey(ledgerId, Long.MAX_VALUE);
        long startTimeMillis = MathUtils.now();
        this.lock.readLock().lock();
        try {
            result = this.kvmap.floorKey(key);
            if (result == null || result.getLedgerId() != ledgerId) {
                result = this.snapshot.floorKey(key);
            }
        } finally {
            this.lock.readLock().unlock();
        }
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_GET_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);

        if (result == null || result.getLedgerId() != ledgerId) {
            return null;
        }
        return (EntryKeyValue)result;
    }

    /**
     * Check if the entire heap usage for this EntryMemTable exceeds limit
     */
    boolean isSizeLimitReached() {
        return size.get() >= skipListSizeLimit;
    }

    /**
     * Check if there is data in the mem-table
     * @return
     */
    boolean isEmpty() {
        return size.get() == 0 && snapshot.isEmpty();
    }
}
