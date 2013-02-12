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
import org.apache.bookkeeper.bookie.CheckpointProgress.CheckPoint;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;

import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
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
class InterleavedLedgerStorage implements LedgerStorage, EntryLogListener {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    protected EntryLogger entryLogger;
    protected LedgerCache ledgerCache;
    private final CheckpointProgress checkPointer;
    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    public InterleavedLedgerStorage(ServerConfiguration conf, ActiveLedgerManager activeLedgerManager,
                                    LedgerDirsManager ledgerDirsManager,
                                    LedgerDirsManager indexDirsManager, CheckpointProgress checkPointer)
                                            throws IOException {
        this.checkPointer = checkPointer;
        entryLogger = new EntryLogger(conf, ledgerDirsManager, this);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgerManager, indexDirsManager);
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger, this,
                activeLedgerManager, new EntryLogCompactionScanner());
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
    public boolean setFenced(long ledgerId) throws IOException {
        return ledgerCache.setFenced(ledgerId);
    }

    @Override
    public boolean isFenced(long ledgerId) throws IOException {
        return ledgerCache.isFenced(ledgerId);
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
        // TODO: Move this to the function calling addEntry
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.WRITE_BYTES)
                .add(entry.remaining());
        processEntry(ledgerId, entryId, entry);
        return entryId;
    }

    protected ByteBuffer getEntryImpl(long ledgerId, long entryId) throws IOException {
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        long startTimeMillis = MathUtils.now();
        long offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(BookkeeperServerOp
                .STORAGE_GET_OFFSET).registerSuccessfulEvent(MathUtils.now() - startTimeMillis);

        if (offset == 0) {
            return null;
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
    public ByteBuffer getEntry(long ledgerId, long entryId) throws IOException {
        ByteBuffer buffToRet = getEntryImpl(ledgerId, entryId);
        if (null == buffToRet) {
            throw new Bookie.NoEntryException(ledgerId, entryId);
        }
        return buffToRet;
    }

    private void flushOptional(boolean force, boolean isCheckPointFlush)
            throws IOException {
        boolean flushFailed = false;
        try {
            ledgerCache.flushLedger(force);
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger cache", ioe);
            flushFailed = true;
        }

        try {
            // if it is just a checkpoint flush, we just flush rotated entry log files
            // in entry logger.
            if (isCheckPointFlush) {
                entryLogger.checkpoint();
            } else {
                entryLogger.flush();
            }
        } catch (LedgerDirsManager.NoWritableLedgerDirException e) {
            throw e;
        } catch (IOException ioe) {
            LOG.error("Exception flushing Ledger", ioe);
            flushFailed = true;
        }
        if (flushFailed) {
            throw new IOException("Flushing to storage failed, check logs");
        }
    }

    @Override
    public void checkpoint(CheckPoint checkpoint) throws IOException {
        // Ignore checkpoint, since for interleaved ledger storage, we need just checkpoint unflushed.
        //
        // we don't need to check somethingwritten since checkpoint
        // is scheduled when rotate an entry logger file. and we could
        // not set somethingWritten to false after checkpoint, since
        // current entry logger file isn't flushed yet.
        flushOptional(true, true);
    }

    @Override
    synchronized public void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }
        somethingWritten = false;
        flushOptional(true, false);
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
            addEntry(buffer);
        }
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
        processEntry(ledgerId, entryId, entry, true);
    }

    synchronized protected void processEntry(long ledgerId, long entryId, ByteBuffer entry, boolean rollLog)
            throws IOException {
        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(entry, rollLog);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);

        somethingWritten = true;
    }

    @Override
    public void onRotateEntryLog() {
        // for interleaved ledger storage, we request a checkpoint when rotating a entry log file.
        if (null != checkPointer) {
            CheckPoint checkpoint = checkPointer.requestCheckpoint();
            checkPointer.startCheckpoint(checkpoint);
        }
    }
}
