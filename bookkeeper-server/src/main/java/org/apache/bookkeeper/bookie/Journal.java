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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide journal related management.
 */
class Journal extends Thread {

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

        synchronized void rollLog() {
            byte buff[] = new byte[16];
            ByteBuffer bb = ByteBuffer.wrap(buff);
            // we should record <logId, logPosition> marked in markLog
            // which is safe since records before lastMark have been
            // persisted to disk (both index & entry logger)
            bb.putLong(lastMark.getTxnLogId());
            bb.putLong(lastMark.getTxnLogPosition());
            if (LOG.isDebugEnabled()) {
                LOG.debug("RollLog to persist last marked log : " + lastMark);
            }
            for(File dir: ledgerDirectories) {
                File file = new File(dir, "lastMark");
                try {
                    FileOutputStream fos = new FileOutputStream(file);
                    fos.write(buff);
                    fos.getChannel().force(true);
                    fos.close();
                } catch (IOException e) {
                    LOG.error("Problems writing to " + file, e);
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
            for(File dir: ledgerDirectories) {
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
        QueueEntry(ByteBuffer entry, long ledgerId, long entryId,
                   WriteCallback cb, Object ctx) {
            this.entry = entry.duplicate();
            this.cb = cb;
            this.ctx = ctx;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.startTimeMillis = MathUtils.now();
        }

        long startTimeMillis;

        ByteBuffer entry;

        long ledgerId;

        long entryId;

        WriteCallback cb;

        Object ctx;
    }

    private class ForceWriteRequest {
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

        public void process(boolean shouldForceWrite) throws IOException {
            if (isMarker) {
                return;
            }

            try {
                if (shouldForceWrite) {
                    this.logFile.getBufferedChannel().forceWrite();
                }

                lastLogMark.setLastLogMark(this.logId, this.lastFlushedPosition);

                // Notify the waiters that the force write succeeded
                for (QueueEntry e : this.forceWriteWaiters) {
                    ServerStatsProvider.getStatsLoggerInstance()
                            .getOpStatsLogger(BookkeeperServerStatsLogger.BookkeeperServerOp
                            .JOURNAL_ADD_ENTRY).registerSuccessfulEvent(MathUtils.now()
                            - e.startTimeMillis);
                    e.cb.writeComplete(0, e.ledgerId, e.entryId, null, e.ctx);
                }
            }
            finally {
                closeFileIfNecessary();
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
    private class ForceWriteThread extends Thread {
        volatile boolean running = true;
        // This holds the queue entries that should be notified after a
        // successful force write
        Thread threadToNotifyOnEx;
        // should we group force writes
        private final boolean enableGroupForceWrites;
        // make flush interval as a parameter
        public ForceWriteThread(Thread threadToNotifyOnEx, boolean enableGroupForceWrites) {
            super("ForceWriteThread");
            this.threadToNotifyOnEx = threadToNotifyOnEx;
            this.enableGroupForceWrites = enableGroupForceWrites;
        }
        @Override
        public void run() {
            LOG.info("ForceWrite Thread started");
            boolean shouldForceWrite = true;
            JournalChannel currLogFile = null;
            while(running) {
                ForceWriteRequest req = null;
                try {
                    req = forceWriteRequests.take();

                    // Force write the file and then notify the write completions
                    //
                    if (!req.isMarker) {
                        if (enableGroupForceWrites) {
                            // if we are going to force write, any request that is already in the
                            // queue will benefit from this force write - post a marker prior to issuing
                            // the flush so until this marker is encountered we can skip the force write
                            if (shouldForceWrite) {
                                forceWriteRequests.put(new ForceWriteRequest(req.logFile, 0, 0, null, false, true));
                            }
                        }
                        req.process(shouldForceWrite);
                        currLogFile = req.logFile;
                    }

                    if (enableGroupForceWrites &&
                        // if its a marker we should switch back to flushing unless its not
                        // for the current file where the shouldClose would have caused the
                        // force write flag to have been reset
                        (!req.isMarker || req.logFile != currLogFile) &&
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
    // max journal file size
    final long maxJournalSize;
    // number journal files kept before marked journal
    final int maxBackupJournals;

    final File journalDirectory;
    final File ledgerDirectories[];
    final ServerConfiguration conf;
    ForceWriteThread forceWriteThread;

    private LastLogMark lastLogMark = new LastLogMark(0, 0);

    // journal entry queue to commit
    LinkedBlockingQueue<QueueEntry> queue = new LinkedBlockingQueue<QueueEntry>();
    LinkedBlockingQueue<ForceWriteRequest> forceWriteRequests = new LinkedBlockingQueue<ForceWriteRequest>();

    volatile boolean running = true;

    public Journal(ServerConfiguration conf) {
        super("BookieJournal-" + conf.getBookiePort());
        this.conf = conf;
        this.journalDirectory = Bookie.getCurrentDirectory(conf.getJournalDir());
        this.ledgerDirectories = Bookie.getCurrentDirectories(conf.getLedgerDirs());
        this.maxJournalSize = conf.getMaxJournalSize() * MB;
        this.maxBackupJournals = conf.getMaxBackupJournals();
        this.forceWriteThread = new ForceWriteThread(this, conf.getGroupJournalForceWrites());

        // read last log mark
        lastLogMark.readLog();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Last Log Mark : " + lastLogMark);
        }
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
    public void rollLog() {
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
            recLog = new JournalChannel(journalDirectory, journalId);
        } else {
            recLog = new JournalChannel(journalDirectory, journalId, journalPos);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Try to relay journal logs : " + logs);
        }
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
        queue.add(new QueueEntry(entry, ledgerId, entryId, cb, ctx));
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
                    logFile = new JournalChannel(journalDirectory, logId);
                    bc = logFile.getBufferedChannel();

                    lastFlushPosition = 0;
                }

                if (qe == null) {
                    if (toFlush.isEmpty()) {
                        qe = queue.take();
                    } else {
                        qe = queue.poll();
                        if (qe == null || bc.position() > lastFlushPosition + 512*1024) {
                            //logFile.force(false);
                            bc.flush(false);
                            lastFlushPosition = bc.position();
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
