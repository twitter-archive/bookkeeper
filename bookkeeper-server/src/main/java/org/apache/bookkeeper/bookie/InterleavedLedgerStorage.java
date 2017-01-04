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

import java.io.File;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.Observable;
import java.util.Observer;

import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

/**
 * Interleave ledger storage
 * This ledger storage implementation stores all entries in a single
 * file and maintains an index file for each ledger.
 */
class InterleavedLedgerStorage implements LedgerStorage, EntryLogListener {
    final static Logger LOG = LoggerFactory.getLogger(InterleavedLedgerStorage.class);

    // Hold the last checkpoint
    protected static class CheckpointHolder {
        Checkpoint lastCheckpoint = Checkpoint.MAX;

        protected synchronized void setNextCheckpoint(Checkpoint cp) {
            if (Checkpoint.MAX.equals(lastCheckpoint) || lastCheckpoint.compareTo(cp) < 0) {
                lastCheckpoint = cp;
            }
        }

        protected synchronized void clearLastCheckpoint(Checkpoint done) {
            if (0 == lastCheckpoint.compareTo(done)) {
                lastCheckpoint = Checkpoint.MAX;
            }
        }

        protected synchronized Checkpoint getLastCheckpoint() {
            return lastCheckpoint;
        }
    }

    protected EntryLogger entryLogger;
    protected LedgerCache ledgerCache;
    private final CheckpointSource checkpointSource;
    protected final CheckpointHolder checkpointHolder = new CheckpointHolder();
    // This is the thread that garbage collects the entry logs that do not
    // contain any active ledgers in them; and compacts the entry logs that
    // has lower remaining percentage to reclaim disk space.
    final GarbageCollectorThread gcThread;
    final LedgerDirsManager ledgerDirsManager;
    final LedgerDirsManager indexDirsManager;

    // this indicates that a write has happened since the last flush
    private volatile boolean somethingWritten = false;

    // Stats
    final OpStatsLogger getOffsetStats;
    final OpStatsLogger getEntryStats;

    public InterleavedLedgerStorage(ServerConfiguration conf,
                                    ActiveLedgerManager activeLedgerManager,
                                    LedgerDirsManager ledgerDirsManager,
                                    LedgerDirsManager indexDirsManager,
                                    CheckpointSource checkpointSource,
                                    StatsLogger statsLogger)
            throws IOException {
        this.checkpointSource = checkpointSource;
        entryLogger = new EntryLogger(conf, ledgerDirsManager, this, statsLogger);
        ledgerCache = new LedgerCacheImpl(conf, activeLedgerManager, indexDirsManager, statsLogger);
        gcThread = new GarbageCollectorThread(conf, ledgerCache, entryLogger, this,
                activeLedgerManager, statsLogger);
        this.ledgerDirsManager = ledgerDirsManager;
        this.indexDirsManager = indexDirsManager;
        // Stats
        this.getEntryStats = statsLogger.getOpStatsLogger(STORAGE_GET_ENTRY);
        this.getOffsetStats = statsLogger.getOpStatsLogger(STORAGE_GET_OFFSET);
    }

    @Override
    public void registerListener(LedgerStorageListener listener) {
        ledgerCache.registerListener(listener);
    }

    @Override
    public void reclaimDiskSpace() throws IOException {
        gcThread.gc();
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFailed(File disk) {
                // do nothing.
            }

            @Override
            public void diskAlmostFull(File disk) {
                // when the disk space reaches the warn threshold, trigger force gc
                // and enable minor compaction but disable major compaction
                gcThread.enableForceGC(true, false);
            }

            @Override
            public void diskFull(File disk) {
                // when the disk space reaches the critical threshold, trigger force gc
                // but disable all compaction
                gcThread.enableForceGC(true, true);
            }

            @Override
            public void allDisksFull() {
                // when all the disk space reach the critical threshold, trigger force gc
                // but disable compaction
                gcThread.enableForceGC(true, true);
            }

            @Override
            public void fatalError() {
                // do nothing.
            }

            @Override
            public void diskWritable(File disk) {
                // we have enough space now, disable force gc.
                gcThread.disableForceGC();
            }

            @Override
            public void diskJustWritable(File disk) {
                // if a disk is just writable, disable major compaction, enable minor compaction
                gcThread.enableForceGC(true, false);
            }
        };
    }

    @Override
    public void start() {
        gcThread.start();
        // register listener after gc thread is started.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        if (indexDirsManager != ledgerDirsManager) {
            indexDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        }
    }

    @Override
    public void shutdown() throws InterruptedException {
        // shut down gc thread, which depends on zookeeper client
        // also compaction will write entries again to entry log file
        LOG.info("Shutting down GC thread");
        gcThread.shutdown();
        LOG.info("Shutting down entry logger");
        entryLogger.shutdown();
        try {
            ledgerCache.close();
        } catch (IOException e) {
            LOG.error("Error while closing the ledger cache", e);
        }
        LOG.info("Complete shutting down Ledger Storage");
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
    public long getLastAddConfirmed(long ledgerId) throws IOException {
        Long lac = ledgerCache.getLastAddConfirmed(ledgerId);
        if (lac == null) {
            ByteBuffer bb = getEntry(ledgerId, BookieProtocol.LAST_ADD_CONFIRMED);
            if (null == bb) {
                return BookieProtocol.INVALID_ENTRY_ID;
            } else {
                bb.getLong(); // ledger id
                bb.getLong(); // entry id
                lac = bb.getLong();
                lac = ledgerCache.updateLastAddConfirmed(ledgerId, lac);
            }
        }
        return lac;
    }

    public Observable waitForLastAddConfirmedUpdate(long ledgerId, long previoisLAC, Observer observer) throws IOException {
        return ledgerCache.waitForLastAddConfirmedUpdate(ledgerId, previoisLAC, observer);
    }


    @Override
    public long addEntry(ByteBuffer entry) throws IOException {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        long lac = entry.getLong();
        entry.rewind();
        processEntry(ledgerId, entryId, entry);
        // after adding the entry, update the lac
        // TODO: do we need to get last add confirmed first to ensure lac is loaded?
        // ledgerCache.getLastAddConfirmed(ledgerId);
        ledgerCache.updateLastAddConfirmed(ledgerId, lac);
        return entryId;
    }

    protected ByteBuffer getEntryImpl(long ledgerId, long entryId) throws IOException {
        /*
         * If entryId is BookieProtocol.LAST_ADD_CONFIRMED, then return the last written.
         */
        if (entryId == BookieProtocol.LAST_ADD_CONFIRMED) {
            entryId = ledgerCache.getLastEntry(ledgerId);
        }

        long startTimeNanos = MathUtils.nowInNano();
        long offset = ledgerCache.getEntryOffset(ledgerId, entryId);
        getOffsetStats.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));

        if (offset == 0) {
            return null;
        }
        startTimeNanos = MathUtils.nowInNano();
        byte[] retBytes = entryLogger.readEntry(ledgerId, entryId, offset);
        getEntryStats.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
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
    public Checkpoint checkpoint(Checkpoint checkpoint) throws IOException {
        Checkpoint lastCheckpoint = checkpointHolder.getLastCheckpoint();
        // if checkpoint is less than last checkpoint, we don't need to do checkpoint again.
        if (lastCheckpoint.compareTo(checkpoint) > 0) {
            return lastCheckpoint;
        }
        // we don't need to check somethingwritten since checkpoint
        // is scheduled when rotate an entry logger file. and we could
        // not set somethingWritten to false after checkpoint, since
        // current entry logger file isn't flushed yet.
        flushOptional(true, true);
        // after the ledger storage finished checkpointing, try to clear the done checkpoint

        checkpointHolder.clearLastCheckpoint(lastCheckpoint);
        return lastCheckpoint;
    }

    @Override
    synchronized public void flush() throws IOException {
        if (!somethingWritten) {
            return;
        }
        somethingWritten = false;
        flushOptional(true, false);
    }

    protected void processEntry(long ledgerId, long entryId, ByteBuffer entry) throws IOException {
        processEntry(ledgerId, entryId, entry, true);
    }

    synchronized protected void processEntry(long ledgerId, long entryId, ByteBuffer entry, boolean rollLog)
            throws IOException {
        /*
         * Touch dirty flag
         */
        somethingWritten = true;

        /*
         * Log the entry
         */
        long pos = entryLogger.addEntry(ledgerId, entry, rollLog);

        /*
         * Set offset of entry id to be the current ledger position
         */
        ledgerCache.putEntryOffset(ledgerId, entryId, pos);
    }

    @Override
    public void onRotateEntryLog() {
        // for interleaved ledger storage, we request a checkpoint when rotating a entry log file.
        // the checkpoint represent the point that all the entries added before this point are already
        // in ledger storage and ready to be synced to disk.
        if (null != checkpointSource) {
            checkpointHolder.setNextCheckpoint(checkpointSource.newCheckpoint());
        }
    }
}
