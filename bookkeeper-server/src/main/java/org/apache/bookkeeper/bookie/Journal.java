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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;

/**
 * Provide journal related management.
 */
class Journal extends BookieThread {

    static Logger LOG = LoggerFactory.getLogger(Journal.class);

    private static class DaemonThreadFactory implements ThreadFactory {
        private ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        public Thread newThread(Runnable r) {
            Thread thread = defaultThreadFactory.newThread(r);
            thread.setDaemon(true);
            return thread;
        }
    }

    /**
     * Filter to pickup journals
     */
    private static interface JournalIdFilter {
        public boolean accept(long journalId);
    }

    /**
     * List all journal ids by a specified journal id filer
     *
     * @param journalDir journal dir
     * @param filter journal id filter
     * @return list of filtered ids
     */
    private static List<Long> listJournalIds(File journalDir, JournalIdFilter filter) {
        File logFiles[] = journalDir.listFiles();
        List<Long> logs = new ArrayList<Long>();
        for(File f: logFiles) {
            String name = f.getName();
            if (!name.endsWith(".txn")) {
                continue;
            }
            String idString = name.split("\\.")[0];
            long id = Long.parseLong(idString, 16);
            if (filter != null) {
                if (filter.accept(id)) {
                    logs.add(id);
                }
            } else {
                logs.add(id);
            }
        }
        Collections.sort(logs);
        return logs;
    }

    /**
     * Last Log Mark
     */
    class LastLogMark {
        private long txnLogId;
        private long txnLogPosition;
        private LastLogMark lastMark;
        LastLogMark(long logId, long logPosition) {
            this.txnLogId = logId;
            this.txnLogPosition = logPosition;
        }
        synchronized void setLastLogMark(long logId, long logPosition) {
            txnLogId = logId;
            txnLogPosition = logPosition;
        }
        synchronized void markLog() {
            lastMark = new LastLogMark(txnLogId, txnLogPosition);
        }

        synchronized LastLogMark getLastMark() {
            return lastMark;
        }
        synchronized long getTxnLogId() {
            return txnLogId;
        }
        synchronized long getTxnLogPosition() {
            return txnLogPosition;
        }

        synchronized void rollLog() throws NoWritableLedgerDirException {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            bb.putLong(lastMark.getTxnLogId());
            bb.putLong(lastMark.getTxnLogPosition());
            LOG.debug("RollLog to persist last marked log : {}", lastMark);
            List<File> writableLedgerDirs = ledgerDirsManager
                    .getWritableLedgerDirs();
            for (File dir : writableLedgerDirs) {
                File file = new File(dir, "lastMark");
                FileOutputStream fos = null;
                try {
                    fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                    fos = null;
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
                } finally {
                    // if stream already closed in try block successfully,
                    // stream might have nullified, in such case below
                    // call will simply returns
                    IOUtils.close(LOG, fos);
                }
            }
        }

        /**
         * Read last mark from lastMark file.
         * The last mark should first be max journal log id,
         * and then max log position in max journal log.
         */
        synchronized void readLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            for(File dir: ledgerDirsManager.getAllLedgerDirs()) {
                File file = new File(dir, "lastMark");
                try {
                    FileInputStream fis = new FileInputStream(file);
                    try {
                        int bytesRead = fis.read(buff);
                        if (bytesRead != 16) {
                            throw new IOException("Couldn't read enough bytes from lastMark."
                                                  + " Wanted " + 16 + ", got " + bytesRead);
                        }
                    } finally {
                        fis.close();
                    }
                    bb.clear();
                    long i = bb.getLong();
                    long p = bb.getLong();
                    if (i > txnLogId) {
                        txnLogId = i;
                        if(p > txnLogPosition) {
                          txnLogPosition = p;
                        }
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this bookie");
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("LastMark: logId - ").append(txnLogId)
              .append(" , position - ").append(txnLogPosition);

            return sb.toString();
        }
    }

    /**
     * Filter to return list of journals for rolling
     */
    private class JournalRollingFilter implements JournalIdFilter {
        @Override
        public boolean accept(long journalId) {
            if (journalId < lastLogMark.getLastMark().getTxnLogId()) {
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Scanner used to scan a journal
     */
    public static interface JournalScanner {
        /**
         * Process a journal entry.
         *
         * @param journalVersion
         *          Journal Version
         * @param offset
         *          File offset of the journal entry
         * @param entry
         *          Journal Entry
         * @throws IOException
         */
        public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Journal Entry to Record
     */
    private static class QueueEntry {
        ByteBuffer entry;
        long ledgerId;
        long entryId;
        WriteCallback cb;
        Object ctx;
        long enqueueTime;

        QueueEntry(ByteBuffer entry, long ledgerId, long entryId,
                   WriteCallback cb, Object ctx, long enqueueTime) {
            this.entry = entry.duplicate();
            this.cb = cb;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.enqueueTime = enqueueTime;
        }

        public void callback() {
            ServerStatsProvider.getStatsLoggerInstance()
                    .getOpStatsLogger(BookkeeperServerStatsLogger.BookkeeperServerOp
                            .JOURNAL_ADD_ENTRY).registerSuccessfulEvent(MathUtils.elapsedMSec(enqueueTime));
            if (LOG.isDebugEnabled()) {
                LOG.debug("Acknowledge Ledger:" + ledgerId + " Entry:" + entryId);
            }
            cb.writeComplete(0, ledgerId, entryId, null, ctx);
        }
    }

    private class ForceWriteRequest implements Runnable {
        private JournalChannel logFile;
        private LinkedList<QueueEntry> forceWriteWaiters;
        private boolean shouldClose;
        private boolean isMarker;
        private long lastFlushedPosition;
        private long logId;

        private ForceWriteRequest(JournalChannel logFile,
                          long logId,
                          long lastFlushedPosition,
                          LinkedList<QueueEntry> forceWriteWaiters,
                          boolean shouldClose,
                          boolean isMarker) {
            this.forceWriteWaiters = forceWriteWaiters;
            this.logFile = logFile;
            this.logId = logId;
            this.lastFlushedPosition = lastFlushedPosition;
            this.shouldClose = shouldClose;
            this.isMarker = isMarker;
        }

        public int process(boolean shouldForceWrite) throws IOException {
            if (isMarker) {
                return 0;
            }

            try {
                if (shouldForceWrite) {
                    long startTimeNanos = MathUtils.nowInNano();
                    this.logFile.forceWrite();
                    ServerStatsProvider.getStatsLoggerInstance()
                        .getOpStatsLogger(BookkeeperServerStatsLogger.BookkeeperServerOp
                            .JOURNAL_FORCE_WRITE_LATENCY).registerSuccessfulEvent(MathUtils.elapsedMSec(startTimeNanos));
                }

                lastLogMark.setLastLogMark(this.logId, this.lastFlushedPosition);

                // Notify the waiters that the force write succeeded
                cbThreadPool.submit(this);

                return this.forceWriteWaiters.size();
            }
            finally {
                closeFileIfNecessary();
            }
        }

        public void run() {
            for (QueueEntry e : this.forceWriteWaiters) {
                e.callback();    // Process cbs inline
            }
        }

        public void closeFileIfNecessary() {
            // Close if shouldClose is set
            if (shouldClose) {
                // We should guard against exceptions so its
                // safe to call in catch blocks
                try {
                    logFile.close();
                    // Call close only once
                    shouldClose = false;
                }
                catch (IOException ioe) {
                    LOG.error("I/O exception while closing file", ioe);
                }
            }
        }
    }

    /**
     * ForceWriteThread is a background thread which makes the journal durable periodically
     *
     */
    private class ForceWriteThread extends BookieThread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;
        // should we group force writes
        private final boolean enableGroupForceWrites;
        // Number of writes grouped by the previous write
        private volatile int groupingFactor;
        // make flush interval as a parameter
        public ForceWriteThread(Thread threadToNotifyOnEx, boolean enableGroupForceWrites) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
            this.groupingFactor = 0;

            // Export sampled stats for journal grouping efficiency.
            Stats.export(new SampledStat<Integer>(ServerStatsProvider
                .getStatsLoggerInstance().getStatName(BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType
                    .JOURNAL_FORCE_WRITE_GROUPING_COUNT), 0) {
                @Override
                public Integer doSample() {
                    return groupingFactor;
                }
            });

            // Export sampled stats for journal grouping efficiency.
            Stats.export(new SampledStat<Integer>(ServerStatsProvider
                .getStatsLoggerInstance().getStatName(BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType
                    .JOURNAL_FORCE_WRITE_QUEUE_SIZE), 0) {
                @Override
                public Integer doSample() {
                    return forceWriteRequests.size();
                }
            });

        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            boolean shouldForceWrite = true;
            JournalChannel currLogFile = null;
            int numReqInLastForceWrite = 0;
            while(running) {
                ForceWriteRequest req = null;
                try {
                    req = forceWriteRequests.take();

                    // Force write the file and then notify the write completions
                    //
                    if (!req.isMarker) {
                        if (shouldForceWrite) {
                            // if we are going to force write, any request that is already in the
                            // queue will benefit from this force write - post a marker prior to issuing
                            // the flush so until this marker is encountered we can skip the force write
                            if (enableGroupForceWrites) {
                                forceWriteRequests.put(new ForceWriteRequest(req.logFile, 0, 0, null, false, true));
                            }

                            // If we are about to issue a write, record the number of requests in
                            // the last force write and then reset the counter so we can accumulate
                            // requests in the write we are about to issue
                            if (numReqInLastForceWrite > 0) {
                                groupingFactor = numReqInLastForceWrite;
                                numReqInLastForceWrite = 0;
                            }
                        }
                        numReqInLastForceWrite += req.process(shouldForceWrite);
                        currLogFile = req.logFile;
                    }

                    if (enableGroupForceWrites &&
                        // if its a marker we should switch back to flushing
                        !req.isMarker &&
                        // This indicates that this is the last request in a given file
                        // so subsequent requests will go to a different file so we should
                        // flush on the next request
                        !req.shouldClose) {
                        shouldForceWrite = false;
                    }
                    else {
                        shouldForceWrite = true;
                    }
                } catch (IOException ioe) {
                    LOG.error("I/O exception in ForceWrite thread", ioe);
                    running = false;
                } catch (InterruptedException e) {
                    LOG.error("ForceWrite thread interrupted", e);
                    // close is idempotent
                    if (null != req) {
                        req.closeFileIfNecessary();
                    }
                    running = false;
                }
            }
            // Regardless of what caused us to exit, we should notify the
            // the parent thread as it should either exit or be in the process
            // of exiting else we will have write requests hang
            threadToNotifyOnEx.interrupt();
        }
        // shutdown sync thread
        void shutdown() throws InterruptedException {
            running = false;
            this.interrupt();
            this.join();
        }
    }

    final static long MB = 1024 * 1024L;
    final static int KB = 1024;
    // max journal file size
    final long maxJournalSize;
    // pre-allocation size for the journal files
    final long journalPreAllocSize;
    // write buffer size for the journal files
    final int journalWriteBufferSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final ServerConfiguration conf;
    ForceWriteThread forceWriteThread;
    // should we group force writes
    private final boolean enableGroupForceWrites;
    // Time after which we will stop grouping and issue the flush
    private final long maxGroupWaitInMSec;
    // Threshold after which we flush any buffered journal writes
    private final long bufferedWritesThreshold;

    private LastLogMark lastLogMark = new LastLogMark(0, 0);

    /**
     * The thread pool used to handle callback.
     */
    private final ExecutorService cbThreadPool;

    // journal entry queue to commit
    LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();
    LinkedBlockingQueue<ForceWriteRequest> forceWriteRequests = new LinkedBlockingQueue<ForceWriteRequest>();

    volatile boolean running = true;
    private LedgerDirsManager ledgerDirsManager;

    public Journal(ServerConfiguration conf, LedgerDirsManager ledgerDirsManager) {
        super("BookieJournal-" + conf.getBookiePort());
        this.ledgerDirsManager = ledgerDirsManager;
        this.conf = conf;
        this.journalDirectory = Bookie.getCurrentDirectory(conf.getJournalDir());
        this.maxJournalSize = conf.getMaxJournalSizeMB() * MB;
        this.journalPreAllocSize = conf.getJournalPreAllocSizeMB() * MB;
        this.journalWriteBufferSize = conf.getJournalWriteBufferSizeKB() * KB;
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.enableGroupForceWrites = conf.getJournalAdaptiveGroupWrites();
        this.forceWriteThread = new ForceWriteThread(this, enableGroupForceWrites);
        this.maxGroupWaitInMSec = conf.getJournalMaxGroupWaitMSec();
        this.bufferedWritesThreshold = conf.getJournalBufferedWritesThreshold();
        this.cbThreadPool = Executors.newFixedThreadPool(conf.getNumAddWorkerThreads(), new DaemonThreadFactory());

        // read last log mark
        lastLogMark.readLog();
        LOG.debug("Last Log Mark : {}", lastLogMark);
    }

    LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Records a <i>LastLogMark</i> in memory.
     *
     * <p>
     * The <i>LastLogMark</i> contains two parts: first one is <i>txnLogId</i>
     * (file id of a journal) and the second one is <i>txnLogPos</i> (offset in
     *  a journal). The <i>LastLogMark</i> indicates that those entries before
     * it have been persisted to both index and entry log files.
     * </p>
     *
     * <p>
     * This method is called before flushing entry log files and ledger cache.
     * </p>
     */
    public void markLog() {
        lastLogMark.markLog();
    }

    /**
     * Persists the <i>LastLogMark</i> marked by #markLog() to disk.
     *
     * <p>
     * This action means entries added before <i>LastLogMark</i> whose entry data
     * and index pages were already persisted to disk. It is the time to safely
     * remove journal files created earlier than <i>LastLogMark.txnLogId</i>.
     * </p>
     * <p>
     * If the bookie has crashed before persisting <i>LastLogMark</i> to disk,
     * it still has journal files contains entries for which index pages may not
     * have been persisted. Consequently, when the bookie restarts, it inspects
     * journal files to restore those entries; data isn't lost.
     * </p>
     * <p>
     * This method is called after flushing entry log files and ledger cache successfully, which is to ensure <i>LastLogMark</i> is pesisted.
     * </p>
     * @see #markLog()
     */
    public void rollLog() throws NoWritableLedgerDirException {
        lastLogMark.rollLog();
    }

    /**
     * Garbage collect older journals
     */
    public void gcJournals() {
        // list the journals that have been marked
        List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter());
        // keep MAX_BACKUP_JOURNALS journal files before marked journal
        if (logs.size() >= maxBackupJournals) {
            int maxIdx = logs.size() - maxBackupJournals;
            for (int i=0; i<maxIdx; i++) {
                long id = logs.get(i);
                // make sure the journal id is smaller than marked journal id
                if (id < lastLogMark.getLastMark().getTxnLogId()) {
                    File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                    if (!journalFile.delete()) {
                        LOG.warn("Could not delete old journal file {}", journalFile);
                    }
                    LOG.info("garbage collected journal " + journalFile.getName());
                }
            }
        }
    }

    /**
     * Scan the journal
     *
     * @param journalId
     *          Journal Log Id
     * @param journalPos
     *          Offset to start scanning
     * @param scanner
     *          Scanner to handle entries
     * @throws IOException
     */
    public void scanJournal(long journalId, long journalPos, JournalScanner scanner)
        throws IOException {
        JournalChannel recLog;
        if (journalPos <= 0) {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPreAllocSize, journalWriteBufferSize, journalPos);
        }
        int journalVersion = recLog.getFormatVersion();
        try {
            ByteBuffer lenBuff = ByteBuffer.allocate(4);
            ByteBuffer recBuff = ByteBuffer.allocate(64*1024);
            while(true) {
                // entry start offset
                long offset = recLog.fc.position();
                // start reading entry
                lenBuff.clear();
                fullRead(recLog, lenBuff);
                if (lenBuff.remaining() != 0) {
                    break;
                }
                lenBuff.flip();
                int len = lenBuff.getInt();
                if (len == 0) {
                    break;
                }
                recBuff.clear();
                if (recBuff.remaining() < len) {
                    recBuff = ByteBuffer.allocate(len);
                }
                recBuff.limit(len);
                if (fullRead(recLog, recBuff) != len) {
                    // This seems scary, but it just means that this is where we
                    // left off writing
                    break;
                }
                recBuff.flip();
                scanner.process(journalVersion, offset, recBuff);
            }
        } finally {
            recLog.close();
        }
    }

    /**
     * Replay journal files
     *
     * @param scanner
     *          Scanner to process replayed entries.
     * @throws IOException
     */
    public void replay(JournalScanner scanner) throws IOException {
        final long markedLogId = lastLogMark.getTxnLogId();
        List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
            @Override
            public boolean accept(long journalId) {
                if (journalId < markedLogId) {
                    return false;
                }
                return true;
            }
        });
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLogId > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLogId) {
                throw new IOException("Recovery log " + markedLogId + " is missing");
            }
        }
        LOG.debug("Try to relay journal logs : {}", logs);
        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        for(Long id: logs) {
            long logPosition = 0L;
            if(id == markedLogId) {
                logPosition = lastLogMark.getTxnLogPosition();
            }
            scanJournal(id, logPosition, scanner);
        }
    }

    /**
     * record an add entry operation in journal
     */
    public void logAddEntry(ByteBuffer entry, WriteCallback cb, Object ctx) {
        long ledgerId = entry.getLong();
        long entryId = entry.getLong();
        entry.rewind();
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
            BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_QUEUE_SIZE)
            .inc();
        queue.add(new QueueEntry(entry, ledgerId, entryId, cb, ctx, MathUtils.nowInNano()));
    }

    /**
     * Get the length of journal entries queue.
     *
     * @return length of journal entry queue.
     */
    public int getJournalQueueLength() {
        return queue.size();
    }

    /**
     * A thread used for persisting journal entries to journal files.
     *
     * <p>
     * Besides persisting journal entries, it also takes responsibility of
     * rolling journal files when a journal file reaches journal file size
     * limitation.
     * </p>
     * <p>
     * During journal rolling, it first closes the writing journal, generates
     * new journal file using current timestamp, and continue persistence logic.
     * Those journals will be garbage collected in SyncThread.
     * </p>
     * @see Bookie#SyncThread
     */
    @Override
    public void run() {
        LinkedList<QueueEntry> toFlush = new LinkedList<QueueEntry>();
        ByteBuffer lenBuff = ByteBuffer.allocate(4);
        JournalChannel logFile = null;
        forceWriteThread.start();
        try {
            long logId = 0;
            BufferedChannel bc = null;
            long lastFlushPosition = 0;

            QueueEntry qe = null;
            while (true) {
                // new journal file to write
                if (null == logFile) {
                    logId = MathUtils.now();
                    logFile = new JournalChannel(journalDirectory, logId, journalPreAllocSize, journalWriteBufferSize);
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = 0;
                }

                if (qe == null) {
                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                    } else {
                        qe = queue.poll();
                        // We should issue a forceWrite if any of the three conditions below holds good
                        // 1. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                        // publish at a time - common case in tests.
                        boolean isQueueEmpty = (qe == null);
                        if (isQueueEmpty) {
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_EMPTY_QUEUE).inc();
                        }
                        // 2. If we have buffered more than the buffWriteThreshold
                        boolean hasMaxBufferedSizeExceeded = (bc.position() > lastFlushPosition + bufferedWritesThreshold);
                        if (hasMaxBufferedSizeExceeded) {
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES).inc();
                        }
                        // 3. If the oldest pending entry has been pending for longer than the max wait time
                        boolean hasMaxWaitExceeded = (enableGroupForceWrites && (MathUtils.elapsedMSec(toFlush.getFirst().enqueueTime) > maxGroupWaitInMSec));
                        if (hasMaxWaitExceeded) {
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_MAX_WAIT).inc();
                        }

                        // toFlush is non null and not empty so should be safe to access getFirst
                        if (isQueueEmpty || hasMaxBufferedSizeExceeded || hasMaxWaitExceeded) {
                            bc.flush(false);
                            lastFlushPosition = bc.position();

                            // Trace the lifetime of entries through persistence
                            if (LOG.isDebugEnabled()) {
                                for (QueueEntry e : toFlush) {
                                    LOG.debug("Written and queuing for flush Ledger:" + e.ledgerId + " Entry:" + e.entryId);
                                }
                            }

                            forceWriteRequests.put(new ForceWriteRequest(logFile, logId, lastFlushPosition, toFlush, (lastFlushPosition > maxJournalSize), false));
                            toFlush = new LinkedList<QueueEntry>();
                            // check whether journal file is over file limit
                            if (bc.position() > maxJournalSize) {
                                logFile = null;
                                continue;
                            }
                        }
                    }
                }

                if (!running) {
                    LOG.info("Journal Manager is asked to shut down, quit.");
                    break;
                }

                if (qe == null) { // no more queue entry
                    continue;
                }
                ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                        BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_WRITE_BYTES)
                        .add(qe.entry.remaining());
                ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                        BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_QUEUE_SIZE)
                        .dec();

                lenBuff.clear();
                lenBuff.putInt(qe.entry.remaining());
                lenBuff.flip();
                //
                // we should be doing the following, but then we run out of
                // direct byte buffers
                // logFile.write(new ByteBuffer[] { lenBuff, qe.entry });
                bc.write(lenBuff);
                bc.write(qe.entry);

                // NOTE: preAlloc depends on the fact that we don't change file size while this is
                // called or useful parts of the file will be zeroed out - in other words
                // it depends on single threaded flushes to the JournalChannel
                logFile.preAllocIfNeeded();
                toFlush.add(qe);
                qe = null;
            }
            logFile.close();
            logFile = null;
        } catch (IOException ioe) {
            LOG.error("I/O exception in Journal thread!", ioe);
        } catch (InterruptedException ie) {
            LOG.warn("Journal exits when shutting down", ie);
        } finally {
            // There could be packets queued for forceWrite on this logFile
            // That is fine as this exception is going to anyway take down the
            // the bookie. If we execute this as a part of graceful shutdown,
            // close will flush the file system cache making any previous
            // cached writes durable so this is fine as well.
            IOUtils.close(LOG, logFile);
        }
    }

    /**
     * Shuts down the journal.
     */
    public synchronized void shutdown() {
        try {
            if (!running) {
                return;
            }
            forceWriteThread.shutdown();
            cbThreadPool.shutdown();
            cbThreadPool.awaitTermination(5, TimeUnit.SECONDS);
            cbThreadPool.shutdownNow();
            running = false;
            this.interrupt();
            this.join();
        } catch (InterruptedException ie) {
            LOG.warn("Interrupted during shutting down journal : ", ie);
        }
    }

    private static int fullRead(JournalChannel fc, ByteBuffer bb) throws IOException {
        int total = 0;
        while(bb.remaining() > 0) {
            int rc = fc.read(bb);
            if (rc <= 0) {
                return total;
            }
            total += rc;
        }
        return total;
    }
}
