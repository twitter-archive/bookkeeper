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
        EntrySkipList() {
            super(EntryKey.COMPARATOR);
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
    }

    volatile EntrySkipList kvmap;

    // Snapshot of EntryMemTable.  Made for flusher.
    volatile EntrySkipList snapshot;

    final ServerConfiguration conf;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    final KeyComparator comparator = EntryKey.COMPARATOR;

    // Used to track own data size
    final AtomicLong size;

    final long skipListSizeLimit;

    SkipListArena allocator;

    private EntrySkipList newSkipList() {
        return new EntrySkipList();
    }

    /**
    * Constructor.
    * @param conf Server configuration
    */
    public EntryMemTable(final ServerConfiguration conf) {
        this.kvmap = newSkipList();
        this.snapshot = newSkipList();
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

    /**
    * Creates a snapshot of the current EntryMemTable.
    * Snapshot must be cleared by call to {@link #clearSnapshot(EntrySkipList)}
    */
    boolean snapshot() throws IOException {
        boolean success = false;
        // No-op if snapshot currently has entries
        if (this.snapshot.isEmpty()) {
            this.lock.writeLock().lock();
            try {
                if (this.snapshot.isEmpty()) {
                    if (!this.kvmap.isEmpty()) {
                        this.snapshot = this.kvmap;
                        this.kvmap = newSkipList();
                        // Reset heap to not include any keys
                        this.size.set(0);
                        // Reset allocator so we get a fresh buffer for the new EntryMemTable
                        this.allocator = new SkipListArena(conf);
                        success = true;
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        }
        return success;
    }

    /**
     * Flush snapshot and clear it
     * @param force all data to be flushed (incl' current)
     */
    synchronized public long flush(final SkipListFlusher flusher, boolean force)
            throws IOException {
        // No lock is required as only this function change non-empty this.snapshot
        EntrySkipList keyValues = this.snapshot;
        long size = 0;
        if (!keyValues.isEmpty()) {
            for (EntryKey key : keyValues.keySet()) {
                EntryKeyValue kv = (EntryKeyValue)key;
                size += kv.getLength();
                flusher.process(kv.getLedgerId(), kv.getEntryId(), kv.getValueAsByteBuffer());
            }
            Logger.info("skip list flushed " + size + " bytes");
            clearSnapshot(keyValues);
        }

        if (force) {
            if (snapshot()) {
                size += flush(flusher, false);
            }
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
        this.lock.writeLock().lock();
        try {
            // create a new snapshot and let the old one go.
            assert this.snapshot == keyValues;
            this.snapshot = newSkipList();
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    /**
    * Write an update
    * @param entry
    * @return approximate size of the passed key and value.
    */
    public long addEntry(long ledgerId, long entryId, final ByteBuffer entry, final CacheCallback cb)
            throws IOException {
        if (isSizeLimitReached()) {
            if (snapshot()) {
                cb.onSizeLimitReached();
            } else {
                // Throttling writer w/ 1 ms delay
                try {
                    Thread.sleep(1);
                } catch (Exception exception) {
                }
            }
        }

        this.lock.readLock().lock();
        try {
            EntryKeyValue toAdd = cloneWithAllocator(ledgerId, entryId, entry);
            return internalAdd(toAdd);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    /**
    * Internal version of add() that doesn't clone KVs with the
    * allocator, and doesn't take the lock.
    *
    * Callers should ensure they already have the read lock taken
    */
    private long internalAdd(final EntryKeyValue toAdd) throws IOException {
        long sizeChange = 0;
        long startTimeMillis = MathUtils.now();
        if (kvmap.putIfAbsent(toAdd, toAdd) == null) {
            sizeChange = toAdd.getLength();
            size.addAndGet(sizeChange);
        }
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
            .SKIP_LIST_PUT_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
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
        this.lock.readLock().lock();
        try {
            long startTimeMillis = MathUtils.now();
            value = this.kvmap.get(key);
            if (value == null) {
                value = this.snapshot.get(key);
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_GET_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
            }
        } finally {
            this.lock.readLock().unlock();
        }

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
        this.lock.readLock().lock();
        try {
            long startTimeMillis = MathUtils.now();
            result = this.kvmap.floorKey(key);
            if (result == null || result.getLedgerId() != ledgerId) {
                result = this.snapshot.floorKey(key);
            }
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .SKIP_LIST_GET_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
        } finally {
            this.lock.readLock().unlock();
        }

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
}
