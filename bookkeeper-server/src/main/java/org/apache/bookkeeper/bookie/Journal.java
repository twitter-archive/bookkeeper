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
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.CheckpointProgress.CheckPoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.DaemonThreadFactory;
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
     * A wrapper over log mark to provide a checkpoint for users of journal
     * to do checkpointing.
     */
    public class LogMarkCheckPoint implements CheckPoint {
        final LastLogMark mark;

        public LogMarkCheckPoint(LastLogMark checkpoint) {
            this.mark = checkpoint;
        }

        /**
         * Telling journal a checkpoint is finished.
         *
         * @throws IOException
         */
        public void checkpointComplete(boolean compact) throws IOException {
            mark.rollLog(mark);
            if (compact) {
                // list the journals that have been marked
                List<Long> logs = listJournalIds(journalDirectory, new JournalRollingFilter(mark));
                // keep MAX_BACKUP_JOURNALS journal files before marked journal
                if (logs.size() >= maxBackupJournals) {
                    int maxIdx = logs.size() - maxBackupJournals;
                    for (int i=0; i<maxIdx; i++) {
                        long id = logs.get(i);
                        // make sure the journal id is smaller than marked journal id
                        if (id < mark.getCurMark().getLogFileId()) {
                            File journalFile = new File(journalDirectory, Long.toHexString(id) + ".txn");
                            if (!journalFile.delete()) {
                                LOG.warn("Could not delete old journal file {}", journalFile);
                            }
                            LOG.info("garbage collected journal " + journalFile.getName());
                        }
                    }
                }
            }
        }

        @Override
        public int compareTo(CheckPoint o) {
            if (o == CheckPoint.MAX) {
                return -1;
            }
            return mark.getCurMark().compare(((LogMarkCheckPoint)o).mark.getCurMark());
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof LogMarkCheckPoint)) {
                return false;
            }
            return 0 == compareTo((LogMarkCheckPoint)o);
        }

        @Override
        public int hashCode() {
            return mark.hashCode();
        }
    }

    /**
     * Last Log Mark
     */
    class LastLogMark {
        private LogMark curMark;

        LastLogMark(long logId, long logPosition) {
            this.curMark = new LogMark(logId, logPosition);
        }
        synchronized void setCurLogMark(long logId, long logPosition) {
            curMark.setLogMark(logId, logPosition);
        }
        synchronized LastLogMark markLog() {
            return new LastLogMark(curMark.getLogFileId(), curMark.getLogFileOffset());
        }
        synchronized LogMark getCurMark() {
            return curMark;
        }

        synchronized void rollLog(LastLogMark lastMark) throws NoWritableLedgerDirException {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            lastMark.getCurMark().writeLogMark(bb);
            LOG.debug("RollLog to persist last marked log : {}", lastMark.getCurMark());
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
            LogMark mark = new LogMark();
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
                    mark.readLogMark(bb);
                    if (curMark.compare(mark) < 0) {
                        curMark.setLogMark(mark.getLogFileId(), mark.logFileOffset);
                    }
                } catch (IOException e) {
                    LOG.error("Problems reading from " + file + " (this is okay if it is the first time starting this bookie");
                }
            }
        }
    }

    /**
     * Filter to return list of journals for rolling
     */
    private static class JournalRollingFilter implements JournalIdFilter {

        final LastLogMark lastMark;

        JournalRollingFilter(LastLogMark lastMark) {
            this.lastMark = lastMark;
        }
        
        @Override
        public boolean accept(long journalId) {
            if (journalId < lastMark.getCurMark().getLogFileId()) {
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
                    this.logFile.forceWrite(false);
                    ServerStatsProvider.getStatsLoggerInstance()
                        .getOpStatsLogger(BookkeeperServerStatsLogger.BookkeeperServerOp
                            .JOURNAL_FORCE_WRITE_LATENCY).registerSuccessfulEvent(MathUtils.elapsedMSec(startTimeNanos));
                }
                lastLogMark.setCurLogMark(this.logId, this.lastFlushedPosition);

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
    // should we flush if the queue is empty
    private final boolean flushWhenQueueEmpty;
    // should we hint the filesystem to remove pages from cache after force write
    private final boolean removePagesFromCache;

    private final LastLogMark lastLogMark = new LastLogMark(0, 0);

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

        // Unless there is a cap on the max wait (which requires group force writes)
        // we cannot skip flushing for queue empty
        this.flushWhenQueueEmpty = !enableGroupForceWrites || conf.getJournalFlushWhenQueueEmpty();

        this.removePagesFromCache = conf.getJournalRemovePagesFromCache();
        // read last log mark
        lastLogMark.readLog();
        LOG.debug("Last Log Mark : {}", lastLogMark.getCurMark());
    }

    LastLogMark getLastLogMark() {
        return lastLogMark;
    }

    /**
     * Application tried to schedule a checkpoint. After all the txns added
     * before checkpoint are persisted, a <i>checkpoint</i> will be returned
     * to application. Application could use <i>checkpoint</i> to do its logic.
     */
    public CheckPoint requestCheckpoint() {
        return new LogMarkCheckPoint(lastLogMark.markLog());
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
        final LogMark markedLog = lastLogMark.getCurMark();
        List<Long> logs = listJournalIds(journalDirectory, new JournalIdFilter() {
            @Override
            public boolean accept(long journalId) {
                if (journalId < markedLog.getLogFileId()) {
                    return false;
                }
                return true;
            }
        });
        // last log mark may be missed due to no sync up before
        // validate filtered log ids only when we have markedLogId
        if (markedLog.getLogFileId() > 0) {
            if (logs.size() == 0 || logs.get(0) != markedLog.getLogFileId()) {
                throw new IOException("Recovery log " + markedLog.getLogFileId() + " is missing");
            }
        }
        LOG.debug("Try to relay journal logs : {}", logs);
        // TODO: When reading in the journal logs that need to be synced, we
        // should use BufferedChannels instead to minimize the amount of
        // system calls done.
        for(Long id: logs) {
            long logPosition = 0L;
            if(id == markedLog.getLogFileId()) {
                logPosition = markedLog.getLogFileOffset();
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
                    logFile = new JournalChannel(journalDirectory,
                                        logId,
                                        journalPreAllocSize,
                                        journalWriteBufferSize,
                                        removePagesFromCache);
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = 0;
                }

                if (qe == null) {
                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                    } else {
                        long pollWaitTime = maxGroupWaitInMSec - MathUtils.elapsedMSec(toFlush.getFirst().enqueueTime);
                        if (flushWhenQueueEmpty || pollWaitTime < 0) {
                            pollWaitTime = 0;
                        }
                        qe = queue.poll(pollWaitTime, TimeUnit.MILLISECONDS);
                        boolean shouldFlush = false;
                        // We should issue a forceWrite if any of the three conditions below holds good
                        // 1. If the oldest pending entry has been pending for longer than the max wait time
                        if (enableGroupForceWrites && (MathUtils.elapsedMSec(toFlush.getFirst().enqueueTime) > maxGroupWaitInMSec)) {
                            shouldFlush = true;
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_MAX_WAIT).inc();
                        } else if ((bc.position() > lastFlushPosition + bufferedWritesThreshold)) {
                            // 2. If we have buffered more than the buffWriteThreshold
                            shouldFlush = true;
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES).inc();
                        } else if (qe == null) {
                            // We should get here only if we flushWhenQueueEmpty is true else we would wait
                            // for timeout that would put is past the maxWait threshold
                            // 3. If the queue is empty i.e. no benefit of grouping. This happens when we have one
                            // publish at a time - common case in tests.
                            shouldFlush = true;
                            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                                BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType.JOURNAL_NUM_FLUSH_EMPTY_QUEUE).inc();
                        }

                        // toFlush is non null and not empty so should be safe to access getFirst
                        if (shouldFlush) {
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
