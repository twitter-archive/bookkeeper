package org.apache.bookkeeper.bookie;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
public class SyncThread {
    private final static Logger LOG = LoggerFactory.getLogger(SyncThread.class);

    final ScheduledExecutorService executor;
    final int checkpointInterval;
    final LedgerStorage ledgerStorage;
    final LedgerDirsListener dirsListener;
    final CheckpointSource checkpointSource;

    private final Object suspensionLock = new Object();
    private boolean suspended = false;
    private boolean disableCheckpoint = false;

    public SyncThread(ServerConfiguration conf,
                      LedgerDirsListener dirsListener,
                      LedgerStorage ledgerStorage,
                      CheckpointSource checkpointSource) {
        this.dirsListener = dirsListener;
        this.ledgerStorage = ledgerStorage;
        this.checkpointSource = checkpointSource;
        ThreadFactoryBuilder tfb = new ThreadFactoryBuilder()
                .setNameFormat("SyncThread-" + conf.getBookiePort() + "-%d");
        this.executor = Executors.newSingleThreadScheduledExecutor(tfb.build());
        this.checkpointInterval = conf.getCheckpointInterval();
        LOG.debug("Checkpoint Interval : {}", checkpointInterval);
    }

    void start() {
        executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
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
                    if (!disableCheckpoint) {
                        checkpoint(checkpointSource.newCheckpoint());
                    }
                } catch (Throwable t) {
                    LOG.error("Exception in SyncThread", t);
                    dirsListener.fatalError();
                }
            }
        }, checkpointInterval, checkpointInterval, TimeUnit.MILLISECONDS);
    }

    public Future<Void> requestFlush() {
        return executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    flush();
                } catch (Throwable t) {
                    LOG.error("Exception flushing ledgers ", t);
                }
                return null;
            }
        });
    }

    private void flush() {
        Checkpoint checkpoint = checkpointSource.newCheckpoint();
        try {
            ledgerStorage.flush();
        } catch (NoWritableLedgerDirException e) {
            LOG.error("No writeable ledger directories", e);
            dirsListener.allDisksFull();
            return;
        } catch (IOException e) {
            LOG.error("Exception flushing ledgers", e);
            return;
        }

        if (disableCheckpoint) {
            return;
        }

        LOG.info("Flush ledger storage at checkpoint {}.", checkpoint);
        try {
            checkpointSource.checkpointComplete(checkpoint, false);
        } catch (IOException e) {
            LOG.error("Exception marking checkpoint as complete", e);
            dirsListener.allDisksFull();
        }
    }

    @VisibleForTesting
    public void checkpoint(Checkpoint checkpoint) {
        try {
            checkpoint = ledgerStorage.checkpoint(checkpoint);
        } catch (NoWritableLedgerDirException e) {
            LOG.error("No writeable ledger directories", e);
            dirsListener.allDisksFull();
            return;
        } catch (IOException e) {
            LOG.error("Exception flushing ledgers", e);
            return;
        }

        try {
            checkpointSource.checkpointComplete(checkpoint, true);
        } catch (IOException e) {
            LOG.error("Exception marking checkpoint as complete", e);
            dirsListener.allDisksFull();
        }
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

    @VisibleForTesting
    public void disableCheckpoint() {
        disableCheckpoint = true;
    }

    // shutdown sync thread
    void shutdown() throws InterruptedException {
        LOG.info("Shutting down SyncThread.");
        requestFlush();
        executor.shutdown();
        long start = MathUtils.now();
        while (!executor.awaitTermination(5, TimeUnit.MINUTES)) {
            long now = MathUtils.now();
            LOG.info("SyncThread taking a long time to shutdown. Has taken {}"
                    + " seconds so far", TimeUnit.SECONDS.convert(now - start, TimeUnit.MILLISECONDS));
        }
        LOG.info("Finished shutting down SyncThread.");
    }

}
