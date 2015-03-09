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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.EntryLogMetadataManager.EntryLogMetadata;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the garbage collector thread that runs in the background to
 * remove any entry log files that no longer contains any active ledger.
 */
public class GarbageCollectorThread extends BookieCriticalThread {
    private static final Logger LOG = LoggerFactory.getLogger(GarbageCollectorThread.class);
    private static final int COMPACTION_MAX_OUTSTANDING_REQUESTS = 1000;
    private static final int SECOND = 1000;

    // This is how often we want to run the Garbage Collector Thread (in milliseconds).
    final long gcInitialWaitTime;
    final long gcWaitTime;

    // Compaction parameters
    boolean enableMinorCompaction = false;
    final double minorCompactionThreshold;
    final long minorCompactionInterval;

    boolean enableMajorCompaction = false;
    final double majorCompactionThreshold;
    final long majorCompactionInterval;

    long lastMinorCompactionTime;
    long lastMajorCompactionTime;

    final int maxOutstandingRequests;
    final int compactionRate;
    final CompactionScannerFactory scannerFactory;

    // Entry Logger Handle
    final EntryLogger entryLogger;

    // Ledger Cache Handle
    final LedgerCache ledgerCache;

    final LedgerStorage storage;

    final ActiveLedgerManager activeLedgerManager;

    // flag to ensure gc thread will not be interrupted during compaction
    // to reduce the risk getting entry log corrupted
    final AtomicBoolean compacting = new AtomicBoolean(false);

    volatile boolean running = true;

    // track the last scanned successfully log id
    long scannedLogId = 0;

    // Boolean to trigger a forced GC.
    final AtomicBoolean forceGarbageCollection = new AtomicBoolean(false);
    volatile boolean suspendMajorCompaction = false;
    volatile boolean suspendMinorCompaction = false;

    private static class Offset {
        final long ledger;
        final long entry;
        final long offset;

        Offset(long ledger, long entry, long offset) {
            this.ledger = ledger;
            this.entry = entry;
            this.offset = offset;
        }
    }

    /**
     * A scanner wrapper to check whether a ledger is alive in an entry log file
     */
    class CompactionScannerFactory implements EntryLogger.EntryLogListener {
        final List<Offset> offsets = new ArrayList<Offset>();
        final Object flushLock = new Object();

        EntryLogScanner newScanner(final EntryLogMetadata meta) {
            final RateLimiter rateLimiter = RateLimiter.create(compactionRate);
            return new EntryLogScanner() {
                @Override
                public boolean accept(long ledgerId) {
                    return meta.containsLedger(ledgerId);
                }

                @Override
                public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException {
                    rateLimiter.acquire();
                    synchronized (CompactionScannerFactory.this) {
                        if (offsets.size() > maxOutstandingRequests) {
                            waitEntrylogFlushed();
                        }
                        long lid = entry.getLong();
                        if (lid != ledgerId) {
                            LOG.warn("Invalid entry found @ offset {} : expected ledger id = {}, but found {}.",
                                    new Object[] { offset, ledgerId, lid });
                            throw new IOException("Invalid entry found @ offset " + offset);
                        }
                        long entryId = entry.getLong();
                        entry.rewind();

                        long newoffset = entryLogger.addEntry(ledgerId, entry);
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Compact add entry : lid = {}, eid = {}, offset = {}",
                                    new Object[] { ledgerId, entryId, newoffset });
                        }
                        offsets.add(new Offset(ledgerId, entryId, newoffset));
                    }
                }
            };
        }

        @Override
        public void onRotateEntryLog() {
            synchronized (flushLock) {
                flushLock.notifyAll();
            }
        }

        synchronized private void waitEntrylogFlushed() throws IOException {
            try {
                if (offsets.size() <= 0) {
                    LOG.debug("Skipping entry log flushing, as there is no offsets!");
                    return;
                }
                Offset lastOffset = offsets.get(offsets.size() - 1);
                long lastOffsetLogId = EntryLogger.logIdForOffset(lastOffset.offset);
                while (lastOffsetLogId < entryLogger.getLeastUnflushedLogId() && running) {
                    synchronized (flushLock) {
                        flushLock.wait(1000);
                    }

                    lastOffset = offsets.get(offsets.size() - 1);
                    lastOffsetLogId = EntryLogger.logIdForOffset(lastOffset.offset);
                }
                if (lastOffsetLogId >= entryLogger.getLeastUnflushedLogId() && !running) {
                    throw new IOException("Shutdown garbage collector thread before compaction flushed");
                }

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted waiting for flush", ie);
            }

            for (Offset o : offsets) {
                ledgerCache.putEntryOffset(o.ledger, o.entry, o.offset);
            }
            offsets.clear();
        }

        synchronized void flush() throws IOException {
            waitEntrylogFlushed();
            ledgerCache.flushLedger(true);
        }
    }


    /**
     * Create a garbage collector thread.
     *
     * @param conf
     *          Server Configuration Object.
     * @throws IOException
     */
    public GarbageCollectorThread(ServerConfiguration conf,
                                  LedgerCache ledgerCache,
                                  EntryLogger entryLogger,
                                  LedgerStorage storage,
                                  ActiveLedgerManager activeLedgerManager)
        throws IOException {
        super("GarbageCollectorThread");

        this.ledgerCache = ledgerCache;
        this.entryLogger = entryLogger;
        this.storage = storage;
        this.activeLedgerManager = activeLedgerManager;

        this.gcInitialWaitTime = conf.getGcInitialWaitTime();
        this.gcWaitTime = conf.getGcWaitTime();
        this.maxOutstandingRequests = conf.getCompactionMaxOutstandingRequests();
        this.compactionRate = conf.getCompactionRate();
        this.scannerFactory = new CompactionScannerFactory();
        entryLogger.addListener(this.scannerFactory);
        // compaction parameters
        minorCompactionThreshold = conf.getMinorCompactionThreshold();
        minorCompactionInterval = conf.getMinorCompactionInterval() * SECOND;
        majorCompactionThreshold = conf.getMajorCompactionThreshold();
        majorCompactionInterval = conf.getMajorCompactionInterval() * SECOND;

        if (minorCompactionInterval > 0 && minorCompactionThreshold > 0) {
            if (minorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid minor compaction threshold "
                                    + minorCompactionThreshold);
            }
            if (minorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short minor compaction interval : "
                                    + minorCompactionInterval);
            }
            enableMinorCompaction = true;
        }

        if (majorCompactionInterval > 0 && majorCompactionThreshold > 0) {
            if (majorCompactionThreshold > 1.0f) {
                throw new IOException("Invalid major compaction threshold "
                                    + majorCompactionThreshold);
            }
            if (majorCompactionInterval <= gcWaitTime) {
                throw new IOException("Too short major compaction interval : "
                                    + majorCompactionInterval);
            }
            enableMajorCompaction = true;
        }

        if (enableMinorCompaction && enableMajorCompaction) {
            if (minorCompactionInterval >= majorCompactionInterval ||
                minorCompactionThreshold >= majorCompactionThreshold) {
                throw new IOException("Invalid minor/major compaction settings : minor ("
                                    + minorCompactionThreshold + ", " + minorCompactionInterval
                                    + "), major (" + majorCompactionThreshold + ", "
                                    + majorCompactionInterval + ")");
            }
        }

        LOG.info("Minor Compaction : enabled=" + enableMinorCompaction + ", threshold="
               + minorCompactionThreshold + ", interval=" + minorCompactionInterval);
        LOG.info("Major Compaction : enabled=" + enableMajorCompaction + ", threshold="
               + majorCompactionThreshold + ", interval=" + majorCompactionInterval);

        lastMinorCompactionTime = lastMajorCompactionTime = MathUtils.now();
    }

    public synchronized void enableForceGC(boolean suspendMajorCompaction, boolean suspendMinorCompaction) {
        this.suspendMajorCompaction = suspendMajorCompaction;
        this.suspendMinorCompaction = suspendMinorCompaction;
        if (enableMajorCompaction) {
            if (suspendMajorCompaction) {
                LOG.info("Suspend Major Compaction.");
            } else {
                LOG.info("Resume Major Compaction.");
            }
        }
        if (enableMinorCompaction) {
            if (suspendMinorCompaction) {
                LOG.info("Suspend Minor Compaction.");
            } else {
                LOG.info("Resume Minor Compaction.");
            }
        }
        if (forceGarbageCollection.compareAndSet(false, true)) {
            LOG.info("Forced garbage collection triggered by thread: " + Thread.currentThread().getName());
            notify();
        }
    }

    public void disableForceGC() {
        suspendMajorCompaction = false;
        suspendMinorCompaction = false;
        if (enableMajorCompaction) {
            LOG.info("Resume major compaction");
        }
        if (enableMinorCompaction) {
            LOG.info("Resume minor compaction");
        }
        if (forceGarbageCollection.compareAndSet(true, false)) {
            LOG.info("{} disabled force garbage collection since bookie has enough space now.", Thread
                    .currentThread().getName());
        }
    }

    /**
     * gc ledger storage.
     */
    void gc() {
        // gc inactive/deleted ledgers
        doGcLedgers();

        // Extract all of the ledger ID's that comprise all of the entry logs
        // (except for the current new one which is still being written to).
        extractMetaAndGCEntryLogs(entryLogger.getEntryLogMetadataManager());

        // if it isn't running, break to not access zookeeper
        if (!running) {
            return;
        }

        // gc inactive/deleted ledgers again, just in case ledgers are deleted during scanning entry logs
        doGcLedgers();

        // gc entry logs
        doGcEntryLogs();
    }

    @Override
    public void run() {
        long nextGcWaitTime = gcInitialWaitTime;
        while (running) {
            synchronized (this) {
                try {
                    wait(nextGcWaitTime);
                    nextGcWaitTime = gcWaitTime;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    nextGcWaitTime = gcWaitTime;
                    continue;
                }
            }
            boolean force = forceGarbageCollection.get();
            if (force) {
                LOG.info("Garbage collector thread forced to perform GC before expiry of wait time.");
            }

            try {
                gc();

                if (!running) {
                    break;
                }

                long curTime = MathUtils.now();

                // do minor compaction only when minor compaction is enabled and not suspended
                if (enableMinorCompaction && !suspendMinorCompaction &&
                        (force || curTime - lastMinorCompactionTime > minorCompactionInterval)) {
                    // enter minor compaction
                    ServerStatsProvider.getStatsLoggerInstance()
                            .getCounter(BookkeeperServerStatsLogger.BookkeeperServerCounter.NUM_MINOR_COMP)
                            .inc();
                    LOG.info("Enter minor compaction");
                    doCompactEntryLogs(minorCompactionThreshold);
                    lastMinorCompactionTime = MathUtils.now();
                }

                if (!running) {
                    break;
                }

                if (enableMajorCompaction && !suspendMajorCompaction &&
                        (force || curTime - lastMajorCompactionTime > majorCompactionInterval)) {
                    // enter major compaction
                    ServerStatsProvider.getStatsLoggerInstance()
                            .getCounter(BookkeeperServerStatsLogger.BookkeeperServerCounter.NUM_MAJOR_COMP)
                            .inc();
                    LOG.info("Enter major compaction");
                    doCompactEntryLogs(majorCompactionThreshold);
                    lastMajorCompactionTime = MathUtils.now();
                    // also move minor compaction time
                    lastMinorCompactionTime = lastMajorCompactionTime;
                }
            } finally {
                forceGarbageCollection.set(false);
            }
        }
    }

    /**
     * Do garbage collection ledger index files
     */
    private void doGcLedgers() {
        activeLedgerManager.garbageCollectLedgers(
        new ActiveLedgerManager.GarbageCollector() {
            @Override
            public void gc(long ledgerId) {
                try {
                    ledgerCache.deleteLedger(ledgerId);
                } catch (IOException e) {
                    LOG.error("Exception when deleting the ledger index file on the Bookie: ", e);
                }
            }
        });
    }

    /**
     * Garbage collect those entry loggers which are not associated with any active ledgers
     */
    private void doGcEntryLogs() {
        // Loop through all of the entry logs and remove the non-active ledgers.
        for (Long entryLogId : entryLogger.getEntryLogMetadataManager().getEntryLogs()) {
            doGcEntryLog(entryLogId, entryLogger.getEntryLogMetadataManager().getEntryLogMetadata(entryLogId));
        }
    }

    private void doGcEntryLog(long entryLogId, EntryLogMetadata meta) {
        for (Long entryLogLedger : meta.ledgersMap.keySet()) {
            // Remove the entry log ledger from the set if it isn't active.
            if (!activeLedgerManager.containsActiveLedger(entryLogLedger)) {
                meta.removeLedger(entryLogLedger);
            }
        }
        if (meta.isEmpty()) {
            // This means the entry log is not associated with any active ledgers anymore.
            // We can remove this entry log file now.
            LOG.info("Deleting entryLogId {} as it has no active ledgers!", entryLogId);
            removeEntryLog(entryLogId);
        }
    }

    /**
     * Compact entry logs if necessary.
     *
     * <p>
     * Compaction will be executed from low unused space to high unused space.
     * Those entry log files whose remaining size percentage is higher than threshold
     * would not be compacted.
     * </p>
     */
    @VisibleForTesting
    void doCompactEntryLogs(double threshold) {
        LOG.info("Do compaction to compact those files lower than {}", threshold);
        // sort the ledger meta by occupied unused space
        Comparator<EntryLogMetadata> sizeComparator = new Comparator<EntryLogMetadata>() {
            @Override
            public int compare(EntryLogMetadata m1, EntryLogMetadata m2) {
                long unusedSize1 = m1.totalSize - m1.remainingSize;
                long unusedSize2 = m2.totalSize - m2.remainingSize;
                if (unusedSize1 > unusedSize2) {
                    return -1;
                } else if (unusedSize1 < unusedSize2) {
                    return 1;
                } else {
                    return 0;
                }
            }
        };
        Collection<EntryLogMetadata> entryLogMetadatas = entryLogger.getEntryLogMetadataManager().getEntryLogMetadatas();
        List<EntryLogMetadata> logsToCompact = new ArrayList<EntryLogMetadata>(entryLogMetadatas.size());
        List<EntryLogMetadata> logsToRemove = new ArrayList<EntryLogMetadata>();
        logsToCompact.addAll(entryLogMetadatas);
        Collections.sort(logsToCompact, sizeComparator);
        for (EntryLogMetadata meta : logsToCompact) {
            if (meta.getUsage() >= threshold) {
                break;
            }
            LOG.debug("Compacting entry log {} below threshold {}.", meta.entryLogId, threshold);
            try {
                if (compactEntryLog(meta)) {
                    // schedule entry log to be removed after moving entries
                    logsToRemove.add(meta);
                }
            } catch (LedgerDirsManager.NoWritableLedgerDirException nwlde) {
                LOG.warn("No writable ledger directory available, aborting compaction", nwlde);
                break;
            } catch (IOException ioe) {
                // if compact entry log throws IOException, we don't want to remove that entry log.
                // however, if some entries from that log have been readded to the entry log, and
                // and the offset updated, it's ok to flush that.
                LOG.error("Error compacting entry log {}, log won't be deleted.", meta.entryLogId, ioe);
            }
            if (!running) { // if gc thread is not running, stop compaction
                return;
            }
        }

        if (logsToRemove.size() != 0) {
            // Mark compacting flag to make sure it would not be interrupted
            // by shutdown during entry logs removal.
            if (!compacting.compareAndSet(false, true)) {
                // set compacting flag failed, means compacting is true now
                // indicates another thread wants to interrupt gc thread to exit
                return;
            }

            try {
                scannerFactory.flush();
                for (EntryLogMetadata metadata: logsToRemove) {
                    LOG.info("Deleting entryLogId {} as it is compacted!", metadata.entryLogId);
                    removeEntryLog(metadata.entryLogId);
                }
            } catch (IOException e) {
                LOG.info("Exception when flushing cache and removing entry logs", e);
            } finally {
                compacting.set(false);
            }
        }
    }

    /**
     * Shutdown the garbage collector thread.
     *
     * @throws InterruptedException if there is an exception stopping gc thread.
     */
    public void shutdown() throws InterruptedException {
        this.running = false;
        if (compacting.compareAndSet(false, true)) {
            // if setting compacting flag succeed, means gcThread is not compacting now
            // it is safe to interrupt itself now
            this.interrupt();
            LOG.info("Interrupt gc thread.");
        } else {
            LOG.info("Failed to set compacting flag to true, skipping interrupting gc thread.");
        }
        this.join();
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    private void removeEntryLog(long entryLogId) {
        // remove entry log file successfully
        if (entryLogger.removeEntryLog(entryLogId)) {
            entryLogger.getEntryLogMetadataManager().removeEntryLogMetadata(entryLogId);
        }
    }

    /**
     * Compact an entry log.
     *
     * @param entryLogMeta
     *          Entry Log Metadata
     */
    protected boolean compactEntryLog(EntryLogMetadata entryLogMeta) throws IOException {
        // Similar with Sync Thread
        // try to mark compacting flag to make sure it would not be interrupted
        // by shutdown during compaction. otherwise it will receive
        // ClosedByInterruptException which may cause index file & entry logger
        // closed and corrupted.
        if (!compacting.compareAndSet(false, true)) {
            // set compacting flag failed, means compacting is true now
            // indicates another thread wants to interrupt gc thread to exit
            return false;
        }

        LOG.info("Compacting entry log : {}.", entryLogMeta.entryLogId);

        try {
            entryLogger.scanEntryLog(entryLogMeta.entryLogId,
                                     scannerFactory.newScanner(entryLogMeta));
            LOG.info("Compacted entry log : {}.", entryLogMeta.entryLogId);
        } catch (ShortReadException sre) {
            LOG.warn("Short read exception when compacting {} : ", entryLogMeta.entryLogId, sre);
            // ignore the last partial entry
        } finally {
            // clear compacting flag
            compacting.set(false);
        }

        return true;
    }

    /**
     * A scanner used to extract entry log meta from entry log files.
     */
    static class ExtractionScanner implements EntryLogScanner {
        EntryLogMetadata meta;

        public ExtractionScanner(EntryLogMetadata meta) {
            this.meta = meta;
        }

        @Override
        public boolean accept(long ledgerId) {
            return ledgerId != EntryLogger.INVALID_LID;
        }
        @Override
        public void process(long ledgerId, long offset, ByteBuffer entry) {
            // add new entry size of a ledger to entry log meta
            meta.addLedgerSize(ledgerId, entry.limit() + 4);
        }
    }

    /**
     * Method to read in all of the entry logs (those that we haven't done so yet),
     * and find the set of ledger ID's that make up each entry log file.
     *
     * @param entryLogMetadataManager
     *          entry log metadata manager to manage entry log metadata.
     */
    protected void extractMetaAndGCEntryLogs(EntryLogMetadataManager entryLogMetadataManager) {
        // Extract it for every entry log except for the current one.
        // Entry Log ID's are just a long value that starts at 0 and increments
        // by 1 when the log fills up and we roll to a new one.
        long curLogId = entryLogger.getLeastUnflushedLogId();
        boolean hasExceptionWhenScan = false;
        for (long entryLogId = scannedLogId; entryLogId < curLogId; entryLogId++) {
            // Comb the current entry log file if it has not already been extracted.
            if (entryLogMetadataManager.containsEntryLog(entryLogId)) {
                continue;
            }

            // check whether log file exists or not
            // if it doesn't exist, this log file might have been garbage collected.
            if (!entryLogger.logExists(entryLogId)) {
                continue;
            }

            LOG.info("Extracting entry log meta from entryLogId: " + entryLogId);

            try {
                // Read through the entry log file and extract the entry log meta
                EntryLogMetadata entryLogMeta = extractMetaFromEntryLog(entryLogger, entryLogId);
                entryLogMetadataManager.addEntryLogMetadata(entryLogMeta);
                // GC the log if possible
                doGcEntryLog(entryLogId, entryLogMeta);
            } catch (IOException e) {
                hasExceptionWhenScan = true;
                LOG.warn("Premature exception when processing " + entryLogId +
                         ", recovery will take care of the problem", e);
            }

            // if scan failed on some entry log, we don't move 'scannedLogId' to next id
            // if scan succeed, we don't need to scan it again during next gc run,
            // we move 'scannedLogId' to next id
            if (!hasExceptionWhenScan) {
                ++scannedLogId;
            }
        }
    }

    static EntryLogMetadata extractMetaFromEntryLog(EntryLogger entryLogger, long entryLogId)
            throws IOException {
        EntryLogMetadata entryLogMeta = new EntryLogMetadata(entryLogId);
        ExtractionScanner scanner = new ExtractionScanner(entryLogMeta);
        // Read through the entry log file and extract the entry log meta
        try {
            entryLogger.scanEntryLog(entryLogId, scanner);
        } catch (ShortReadException sre) {
            // short read exception, it means that the last entry in entry logger is corrupted due to
            // an unsuccessful shutdown (e.g kill -9 or power off)
            LOG.warn("Short read on retrieving entry log metadata for {} : ", entryLogId, sre);
        }
        LOG.info("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, entryLogMeta);
        return entryLogMeta;
    }
}
