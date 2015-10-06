package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * SyncThread is a background thread which flushes ledger index pages periodically.
 * Also it takes responsibility of garbage collecting journal files.
 *
 * <p>
 * Before flushing, SyncThread first records a log marker {journalId, journalPos} in memory,
 * which indicates entries before this log marker would be persisted to ledger files.
 * Then sync thread begins flushing ledger index pages to ledger index files, flush entry
 * logger to ensure all entries persisted to entry loggers for future reads.
 * </p>
 * <p>
 * After all data has been persisted to ledger index files and entry loggers, it is safe
 * to persist the log marker to disk. If bookie failed after persist log mark,
 * bookie is able to relay journal entries started from last log mark without losing
 * any entries.
 * </p>
 * <p>
 * Those journal files whose id are less than the log id in last log mark, could be
 * removed safely after persisting last log mark. We provide a setting to let user keeping
 * number of old journal files which may be used for manual recovery in critical disaster.
 * </p>
 */
@VisibleForTesting
public class SyncThread extends BookieCriticalThread implements CheckpointProgress {
    private final static Logger LOG = LoggerFactory.getLogger(SyncThread.class);

    /**
     * Sync Action Representing a CheckPoint or a Flush.
     */
    class SyncAction implements Callable<Boolean> {
        // sync ledger storage up to this checkpoint
        final CheckPoint checkpoint;
        // is the sync request a fully flush request
        final boolean isFlush;
        boolean completed = false;

        SyncAction(CheckPoint cp, boolean isFlush) {
            this.checkpoint = cp;
            this.isFlush = isFlush;
        }

        @Override
        public Boolean call() {
            boolean flushFailed = false;
            try {
                if (!isFlush) {
                    ledgerStorage.checkpoint(checkpoint);
                } else {
                    ledgerStorage.flush();
                }
            } catch (NoWritableLedgerDirException e) {
                LOG.error("No writeable ledger directories when flushing ledger storage : ", e);
                flushFailed = true;
                dirsListener.allDisksFull();
            } catch (IOException e) {
                LOG.error("Exception flushing ledger storage : ", e);
                flushFailed = true;
            }

            // if flush failed, we should not roll last mark, otherwise we would
            // have some ledgers are not flushed and their journal entries were lost
            if (!flushFailed) {
                try {
                    checkpoint.checkpointComplete(true);
                    completed = true;
                } catch (IOException e) {
                    LOG.error("Exception on completing checkpoint " + checkpoint + " : ", e);
                    // treat it as disks full so bookie would transition to readonly mode
                    dirsListener.allDisksFull();
                }
            }
            return completed;
        }
    }

    /**
     * Properly I should change SyncThread to an executor, which is a more natural way for
     * callback on sync requests.
     */
    static class SyncRequest extends FutureTask<Boolean> {

        final SyncAction action;

        public SyncRequest(SyncAction action) {
            super(action);
            this.action = action;
        }
    }

    volatile boolean running = true;
    // flag to ensure sync thread will not be interrupted during flush
    final AtomicBoolean flushing = new AtomicBoolean(false);
    final LinkedBlockingQueue<SyncRequest> syncRequests =
            new LinkedBlockingQueue<SyncRequest>();

    final LedgerStorage ledgerStorage;
    final LedgerDirsListener dirsListener;
    final Journal journal;

    private final Object suspensionLock = new Object();
    private boolean suspended = false;

    public SyncThread(ServerConfiguration conf,
                      LedgerDirsListener dirsListener,
                      LedgerStorage ledgerStorage,
                      Journal journal) {
        super("SyncThread");
        this.dirsListener = dirsListener;
        this.ledgerStorage = ledgerStorage;
        this.journal = journal;
    }

    private Future<Boolean> offerSyncRequest(SyncAction action) {
        SyncRequest request = new SyncRequest(action);
        syncRequests.offer(request);
        return request;
    }

    /**
     * flush data up to given logMark and roll log if success
     * @param checkpoint
     */
    @VisibleForTesting
    public void checkPoint(final CheckPoint checkpoint) {
        new SyncAction(checkpoint, !running).call();
    }

    /**
     * flush on current state.
     */
    Future<Boolean> flush() {
        CheckPoint cp = requestCheckpoint();
        LOG.info("Flush ledger storage at checkpoint {}.", cp);
        return offerSyncRequest(new SyncAction(cp, true));
    }

    /**
     * Suspend sync thread. (for testing)
     */
    @VisibleForTesting
    public void suspendSync() {
        synchronized(suspensionLock) {
            suspended = true;
        }
    }

    /**
     * Resume sync thread. (for testing)
     */
    @VisibleForTesting
    public void resumeSync() {
        synchronized(suspensionLock) {
            suspended = false;
            suspensionLock.notify();
        }
    }

    @Override
    public void run() {
        while(running) {
            SyncRequest request;
            try {
                request = syncRequests.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }

            synchronized (suspensionLock) {
                while (suspended) {
                    try {
                        suspensionLock.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        continue;
                    }
                }
            }

            // try to mark flushing flag to check if interrupted
            if (!flushing.compareAndSet(false, true)) {
                // set flushing flag failed, means flushing is true now
                // indicates another thread wants to interrupt sync thread to exit
                break;
            }

            // run the sync request.
            request.run();

            flushing.set(false);
        }
    }

    // shutdown sync thread
    void shutdown() throws InterruptedException {
        LOG.info("Shutting down SyncThread.");
        // Wake up and finish sync thread
        running = false;
        flushing.compareAndSet(false, true);
        CheckPoint cp = requestCheckpoint();
        startCheckpoint(cp);
        this.join();

        LOG.info("Finished shutting down SyncThread.");
    }

    @Override
    public CheckPoint requestCheckpoint() {
        return journal.requestCheckpoint();
    }

    @Override
    public void startCheckpoint(CheckPoint checkpoint) {
        offerSyncRequest(new SyncAction(checkpoint, !running));
    }

}
