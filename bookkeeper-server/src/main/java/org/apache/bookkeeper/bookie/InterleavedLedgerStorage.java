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

import java.nio.ByteBuffer;
import java.io.IOException;

import org.apache.bookkeeper.jmx.BKMBeanInfo;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;

import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerOp;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
class InterleavedLedgerStorage implements LedgerStorage {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    private EntryLogger entryLogger;
    private LedgerCache ledgerCache;
    private EntryMemTable memTable;
    private final CacheCallback cacheCallback;

    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;

    final private boolean isSkipListEnabled;

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    InterleavedLedgerStorage(ServerConfiguration conf, ActiveLedgerManager activeLedgerManager,
                             final CacheCallback cb) throws IOException {
        entryLogger = new EntryLogger(conf);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgerManager);
        memTable = new EntryMemTable(conf);
        cacheCallback = cb;
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger,
                activeLedgerManager, new EntryLogCompactionScanner());
        isSkipListEnabled = conf.getSkipListUsageEnabled();
    }

    @Override
    public void start() {
        gcThread.start();
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        gcThread.shutdown();
        entryLogger.shutdown();
        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
    }

    @Override
    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        ledgerCache.setMasterKey(ledgerId, masterKey);
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        return ledgerCache.readMasterKey(ledgerId);
    }

    @Override
    public boolean ledgerExists(long ledgerId) throws IOException {
        return ledgerCache.ledgerExists(ledgerId);
    }

    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();

        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.WRITE_BYTES)
                .add(entry.remaining());

        if (isSkipListEnabled) {
            memTable.add(ledgerId, entryId, entry, cacheCallback);
        }
        else {
            processEntry(ledgerId, entryId, entry);
        }

        return entryId;
    }

    @Override
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        EntryKeyValue kv = null;

        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            kv = memTable.getEntry(ledgerId, entryId);
            if (kv != null) {
                return kv.getAsByteBuffer();
            }
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        long startTimeMillis = MathUtils.now();
        long offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .STORAGE_GET_OFFSET).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);

        if (offset == 0) {
            kv = memTable.getEntry(ledgerId, entryId);
            if (kv != null) {
                return kv.getAsByteBuffer();
            }
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }

        startTimeMillis = MathUtils.now();
        byte[] retBytes = entryLogger.readEntry(ledgerId, entryId, offset);
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .STORAGE_GET_ENTRY).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.READ_BYTES)
                .add(retBytes.length);
        return ByteBuffer.wrap(retBytes);
    }

    @Override
    public void prepare(boolean force) throws IOException {
        if (isSkipListEnabled) {
            memTable.flush(this, force);
        }
    }

    @Override
    public boolean isFlushRequired() {
        return somethingWritten;
    };

    @Override
    public void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }
        somethingWritten = false;
        boolean flushFailed = false;

        try {
            ledgerCache.flushLedger(true);
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }

        try {
            entryLogger.flush();
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    @Override
    public BKMBeanInfo getJMXBean() {
        return ledgerCache.getJMXBean();
    }

    /**
     * Scanner used to do entry log compaction
     */
    class EntryLogCompactionScanner implements EntryLogger.EntryLogScanner {
        @Override
        public boolean accept(long ledgerId) {
            // bookie has no knowledge about which ledger is deleted
            // so just accept all ledgers that aren't invalid.
            return ledgerId != EntryLogger.INVALID_LID;
        }

        @Override
        public void process(long ledgerId, long offset, ByteBuffer buffer)
            throws IOException {
            buffer.getLong();   // Skip ledger id
            long entryId = buffer.getLong();
            buffer.rewind();
            processEntry(ledgerId, entryId, buffer);
        }
    }

    synchronized private void processEntry(long ledgerId, long entryId, ByteBuffer entry)
            throws IOException {
        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);

        somethingWritten = true;
    }

    @Override
    public void process(long ledgerId, long entryId, ByteBuffer buffer) throws IOException {
        processEntry(ledgerId, entryId, buffer);
    }
}
