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
package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.client.AsyncCallback.CloseCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.State;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger.BookkeeperClientCounter;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger.BookkeeperClientOp;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor.OrderedSafeGenericCallback;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

/**
 * Ledger handle contains ledger metadata and is used to access the read and
 * write operations to a ledger.
 */
public class LedgerHandle {
    final static Logger LOG = LoggerFactory.getLogger(LedgerHandle.class);

    final byte[] ledgerKey;
    LedgerMetadata metadata;
    final BookKeeper bk;
    final long ledgerId;
    long lastAddPushed;
    long lastAddConfirmed;
    long length;
    final DigestManager macManager;
    final DistributionSchedule distributionSchedule;
    final AtomicInteger refCount;
    final RateLimiter throttler;

    /**
     * Invalid entry id. This value is returned from methods which
     * should return an entry id but there is no valid entry available.
     */
    final static public long INVALID_ENTRY_ID = BookieProtocol.INVALID_ENTRY_ID;

    final AtomicInteger blockAddCompletions = new AtomicInteger(0);
    final Queue<PendingAddOp> pendingAddOps = new ConcurrentLinkedQueue<PendingAddOp>();

    LedgerHandle(BookKeeper bk, long ledgerId, LedgerMetadata metadata,
                 DigestType digestType, byte[] password)
            throws GeneralSecurityException, NumberFormatException {
        this.bk = bk;
        this.refCount = new AtomicInteger(0);
        this.metadata = metadata;

        if (metadata.isClosed()) {
            lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
            length = metadata.getLength();
        } else {
            lastAddConfirmed = lastAddPushed = INVALID_ENTRY_ID;
            length = 0;
        }

        this.ledgerId = ledgerId;

        this.throttler = RateLimiter.create(bk.getConf().getThrottleValue());

        macManager = DigestManager.instantiate(ledgerId, password, digestType);
        this.ledgerKey = MacDigestManager.genDigest("ledger", password);
        distributionSchedule = new RoundRobinDistributionSchedule(
                metadata.getWriteQuorumSize(), metadata.getAckQuorumSize(), metadata.getEnsembleSize());
    }

    public void hintOpen() {
        // Hint to the handle that an open operation was performed. This is so that the handle can handle refCounts accordingly.
        if (refCount.getAndIncrement() == 0) {
            bk.getStatsLogger().getCounter(BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_OPEN_LEDGERS).inc();
        }
    }

    /**
     * Get the id of the current ledger
     *
     * @return the id of the ledger
     */
    public long getId() {
        return ledgerId;
    }

    /**
     * Get the last confirmed entry id on this ledger. It reads
     * the local state of the ledger handle, which is different
     * from the readLastConfirmed call. In the case the ledger
     * is not closed and the client is a reader, it is necessary
     * to call readLastConfirmed to obtain an estimate of the
     * last add operation that has been confirmed.
     *
     * @see #readLastConfirmed()
     *
     * @return the last confirmed entry id or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been confirmed
     */
    public synchronized long getLastAddConfirmed() {
        return lastAddConfirmed;
    }

    synchronized void setLastAddConfirmed(long lac) {
        this.lastAddConfirmed = lac;
    }

    /**
     * Get the entry id of the last entry that has been enqueued for addition (but
     * may not have possibly been persited to the ledger)
     *
     * @return the id of the last entry pushed or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID} if no entry has been pushed
     */
    synchronized public long getLastAddPushed() {
        return lastAddPushed;
    }

    /**
     * Get the Ledger's key/password.
     *
     * @return byte array for the ledger's key/password.
     */
    public byte[] getLedgerKey() {
        return Arrays.copyOf(ledgerKey, ledgerKey.length);
    }

    /**
     * Get the LedgerMetadata
     *
     * @return LedgerMetadata for the LedgerHandle
     */
    LedgerMetadata getLedgerMetadata() {
        return metadata;
    }

    /**
     * Get the DigestManager
     *
     * @return DigestManager for the LedgerHandle
     */
    DigestManager getDigestManager() {
        return macManager;
    }

    /**
     *  Add to the length of the ledger in bytes.
     *
     * @param delta
     * @return
     */
    long addToLength(long delta) {
        this.length += delta;
        return this.length;
    }

    /**
     * Returns the length of the ledger in bytes.
     *
     * @return the length of the ledger in bytes
     */
    synchronized public long getLength() {
        return this.length;
    }

    /**
     * Get the Distribution Schedule
     *
     * @return DistributionSchedule for the LedgerHandle
     */
    DistributionSchedule getDistributionSchedule() {
        return distributionSchedule;
    }

    void writeLedgerConfig(GenericCallback<Void> writeCb) {
        LOG.debug("Writing metadata to ledger manager: {}, {}", this.ledgerId, metadata.getVersion());

        bk.getLedgerManager().writeLedgerMetadata(ledgerId, metadata, writeCb);
    }

    synchronized public boolean isClosed() {
        return metadata.isClosed();
    }

    /**
     * Close this ledger synchronously.
     * @see #asyncClose
     */
    public void close()
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncClose(new SyncCloseCallback(), counter);

        counter.block(0);
        if ((counter.getrc() != BKException.Code.OK) &&
            (counter.getrc() != BKException.Code.LedgerClosedException)) {
            throw BKException.create(counter.getrc());
        }
    }

    /**
     * Asynchronous close, any adds in flight will return errors.
     *
     * Closing a ledger will ensure that all clients agree on what the last entry
     * of the ledger is. This ensures that, once the ledger has been closed, all
     * reads from the ledger will return the same set of entries.
     *
     * @param origCb
     *          callback implementation
     * @param origCtx
     *          control object
     */
    public void asyncClose(final CloseCallback origCb, final Object origCtx) {
        CloseCallback cb = new CloseCallback() {
            @Override
            public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
                if (rc == BKException.Code.OK) {
                    if (refCount.decrementAndGet() == 0) {
                        // Closed a ledger
                        bk.getStatsLogger().getCounter(BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_OPEN_LEDGERS).dec();
                    }
                }
                origCb.closeComplete(rc, lh, origCtx);
            }
        };
        asyncCloseInternal(cb, origCtx, BKException.Code.LedgerClosedException);
    }

    void asyncCloseInternal(final CloseCallback cb, final Object ctx, final int rc) {
        try {
            doAsyncCloseInternal(cb, ctx, rc);
        } catch (RejectedExecutionException re) {
            LOG.debug("Failed to close ledger {} : ", ledgerId, re);
            errorOutPendingAdds(bk.getReturnRc(rc));
            cb.closeComplete(bk.getReturnRc(BKException.Code.InterruptedException), this, ctx);
        }
    }

    /**
     * Same as public version of asyncClose except that this one takes an
     * additional parameter which is the return code to hand to all the pending
     * add ops
     *
     * @param finalCb
     * @param ctx
     * @param rc
     */
    void doAsyncCloseInternal(final CloseCallback finalCb, final Object ctx, final int rc) {
        final CloseCallback cb = new CloseCallback() {

            final long startTime = MathUtils.nowInNano();

            @Override
            public void closeComplete(int newRc, LedgerHandle newLh, Object newCtx) {
                if (BKException.Code.OK == newRc) {
                    getStatsLogger().getOpStatsLogger(BookkeeperClientOp.LEDGER_CLOSE)
                        .registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTime));
                } else {
                    getStatsLogger().getOpStatsLogger(BookkeeperClientOp.LEDGER_CLOSE)
                        .registerFailedEvent(MathUtils.elapsedMicroSec(startTime));
                }
                finalCb.closeComplete(newRc, newLh, newCtx);
            }
        };
        bk.mainWorkerPool.submitOrdered(ledgerId, new SafeRunnable() {
            @Override
            public void safeRun() {
                if (isClosed()) {
                    // if the metadata is already closed, we don't need to proceed the process
                    // otherwise, it might end up encountering bad version error log messages when updating metadata
                    cb.closeComplete(BKException.Code.LedgerClosedException, LedgerHandle.this, ctx);
                    return;
                }
                final long prevLastEntryId;
                final long prevLength;
                final State prevState;
                List<PendingAddOp> pendingAdds;

                synchronized(LedgerHandle.this) {
                    prevState = metadata.getState();
                    prevLastEntryId = metadata.getLastEntryId();
                    prevLength = metadata.getLength();

                    // drain pending adds first
                    pendingAdds = drainPendingAddsToErrorOut();

                    // synchronized on LedgerHandle.this to ensure that
                    // lastAddPushed can not be updated after the metadata
                    // is closed.
                    metadata.setLength(length);

                    // Close operation is idempotent, so no need to check if we are
                    // already closed
                    metadata.close(lastAddConfirmed);
                    lastAddPushed = lastAddConfirmed;
                }

                // error out all pending adds during closing, the callbacks shouldn't be
                // running under any bk locks.
                errorOutPendingAdds(rc, pendingAdds);

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing ledger: " + ledgerId + " at entryId: "
                              + metadata.getLastEntryId() + " with this many bytes: " + metadata.getLength());
                }

                final class CloseCb extends OrderedSafeGenericCallback<Void> {
                    CloseCb() {
                        super(bk.mainWorkerPool, ledgerId);
                    }

                    @Override
                    public void safeOperationComplete(final int rc, Void result) {
                        if (rc == BKException.Code.MetadataVersionException) {
                            rereadMetadata(new OrderedSafeGenericCallback<LedgerMetadata>(bk.mainWorkerPool,
                                                                                          ledgerId) {
                                @Override
                                public void safeOperationComplete(int newrc, LedgerMetadata newMeta) {
                                    if (newrc != BKException.Code.OK) {
                                        LOG.error("Error reading new metadata from ledger " + ledgerId
                                                  + " when closing, code=" + newrc);
                                        cb.closeComplete(rc, LedgerHandle.this, ctx);
                                    } else {
                                        metadata.setState(prevState);
                                        if (prevState.equals(State.CLOSED)) {
                                            metadata.close(prevLastEntryId);
                                        }

                                        metadata.setLength(prevLength);
                                        if (!metadata.isNewerThan(newMeta)
                                                && !metadata.isConflictWith(newMeta)) {
                                            // use the new metadata's ensemble, in case re-replication already
                                            // replaced some bookies in the ensemble.
                                            metadata.setEnsembles(newMeta.getEnsembles());
                                            metadata.setVersion(newMeta.version);
                                            metadata.setLength(length);
                                            metadata.close(getLastAddConfirmed());
                                            writeLedgerConfig(new CloseCb());
                                            return;
                                        } else {
                                            metadata.setLength(length);
                                            metadata.close(getLastAddConfirmed());
                                            LOG.warn("Conditional update ledger metadata for ledger " + ledgerId + " failed.");
                                            cb.closeComplete(rc, LedgerHandle.this, ctx);
                                        }
                                    }
                                }
                                @Override
                                public String toString() {
                                    return String.format("ReReadMetadataForClose(%d)", ledgerId);
                                }
                            });
                        } else if (rc != BKException.Code.OK) {
                            LOG.error("Error update ledger metadata for ledger " + ledgerId + " : " + rc);
                            cb.closeComplete(rc, LedgerHandle.this, ctx);
                        } else {
                            cb.closeComplete(BKException.Code.OK, LedgerHandle.this, ctx);
                        }
                    }

                    @Override
                    public String toString() {
                        return String.format("WriteLedgerConfigForClose(%d)", ledgerId);
                    }
                }

                writeLedgerConfig(new CloseCb());

            }

            @Override
            public String toString() {
                return String.format("CloseLedgerHandle(%d)", ledgerId);
            }
        });
    }

    /**
     * Read a sequence of entries synchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence (included)
     * @param lastEntry
     *          id of last entry of sequence (included)
     *
     */
    public Enumeration<LedgerEntry> readEntries(long firstEntry, long lastEntry)
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();

        asyncReadEntries(firstEntry, lastEntry, new SyncReadCallback(), counter);

        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getSequence();
    }

    /**
     * Read a sequence of entries asynchronously.
     *
     * @param firstEntry
     *          id of first entry of sequence
     * @param lastEntry
     *          id of last entry of sequence
     * @param cb
     *          object implementing read callback interface
     * @param ctx
     *          control object
     */
    public void asyncReadEntries(long firstEntry, long lastEntry,
                                 ReadCallback cb, Object ctx) {
        // Little sanity check
        if (firstEntry < 0 || lastEntry > getLastAddConfirmed()
                || firstEntry > lastEntry) {
            cb.readComplete(BKException.Code.ReadException, this, null, ctx);
            return;
        }
        doAsyncReadEntries(firstEntry, lastEntry, cb, ctx);
    }

    private void doAsyncReadEntries(long firstEntry, long lastEntry,
                                    ReadCallback cb, Object ctx) {
        new PendingReadOp(this, bk.scheduler, firstEntry, lastEntry, cb, ctx)
                .enablePiggybackLAC(true).initiate();
    }
    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data) throws InterruptedException, BKException {
        return addEntry(data, 0, data.length);
    }

    /**
     * Add entry synchronously to an open ledger.
     *
     * @param data
     *         array of bytes to be written to the ledger
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @return the entryId of the new inserted entry
     */
    public long addEntry(byte[] data, int offset, int length)
            throws InterruptedException, BKException {
        LOG.debug("Adding entry {}", data);

        SyncCounter counter = new SyncCounter();
        counter.inc();

        SyncAddCallback callback = new SyncAddCallback();
        asyncAddEntry(data, offset, length, callback, counter);
        counter.block(0);

        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return callback.entryId;
    }

    /**
     * Add entry asynchronously to an open ledger.
     *
     * @param data
     *          array of bytes to be written
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     */
    public void asyncAddEntry(final byte[] data, final AddCallback cb,
                              final Object ctx) {
        asyncAddEntry(data, 0, data.length, cb, ctx);
    }

    /**
     * Add entry asynchronously to an open ledger, using an offset and range.
     *
     * @param data
     *          array of bytes to be written
     * @param offset
     *          offset from which to take bytes from data
     * @param length
     *          number of bytes to take from data
     * @param cb
     *          object implementing callbackinterface
     * @param ctx
     *          some control object
     * @throws ArrayIndexOutOfBoundsException if offset or length is negative or
     *          offset and length sum to a value higher than the length of data.
     */
    public void asyncAddEntry(final byte[] data, final int offset, final int length,
                              final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx);
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    /**
     * Make a recovery add entry request. Recovery adds can add to a ledger even if
     * it has been fenced.
     *
     * This is only valid for bookie and ledger recovery, which may need to replicate
     * entries to a quorum of bookies to ensure data safety.
     *
     * Normal client should never call this method.
     */
    void asyncRecoveryAddEntry(final byte[] data, final int offset, final int length,
                               final AddCallback cb, final Object ctx) {
        PendingAddOp op = new PendingAddOp(LedgerHandle.this, cb, ctx).enableRecoveryAdd();
        doAsyncAddEntry(op, data, offset, length, cb, ctx);
    }

    private void doAsyncAddEntry(final PendingAddOp op, final byte[] data, final int offset, final int length,
                                 final AddCallback cb, final Object ctx) {
        if (offset < 0 || length < 0
                || (offset + length) > data.length) {
            throw new ArrayIndexOutOfBoundsException(
                "Invalid values for offset("+offset
                +") or length("+length+")");
        }
        throttler.acquire();

        final long entryId;
        final long currentLength;
        boolean wasClosed = false;
        synchronized(this) {
            // synchronized on this to ensure that
            // the ledger isn't closed between checking and
            // updating lastAddPushed
            if (metadata.isClosed()) {
                wasClosed = true;
                entryId = -1;
                currentLength = 0;
            } else {
                entryId = ++lastAddPushed;
                currentLength = addToLength(length);
                op.setEntryId(entryId);
                bk.getStatsLogger().getCounter(BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_PENDING_ADD).inc();
                pendingAddOps.add(op);
            }
        }

        if (wasClosed) {
            LOG.warn("Attempt to add to closed ledger: {}", ledgerId);
            cb.addComplete(BKException.Code.LedgerClosedException,
                           LedgerHandle.this, INVALID_ENTRY_ID, ctx);
            return;
        }

        try {
            bk.mainWorkerPool.submit(new SafeRunnable() {
                @Override
                public void safeRun() {
                    ChannelBuffer toSend = macManager.computeDigestAndPackageForSending(
                                               entryId, getLastAddConfirmed(), currentLength, data, offset, length);
                    op.initiate(toSend, length);
                }
                @Override
                public String toString() {
                    return String.format("AsyncAddEntry(lid=%d, eid=%d)", ledgerId, entryId);
                }
            });
        } catch (RejectedExecutionException e) {
            cb.addComplete(bk.getReturnRc(BKException.Code.InterruptedException),
                    LedgerHandle.this, INVALID_ENTRY_ID, ctx);
        }
    }

    synchronized void updateLastConfirmed(long lac, long len) {
        if (lac > lastAddConfirmed) {
            lastAddConfirmed = lac;
            bk.getStatsLogger().getCounter(BookkeeperClientCounter.LAC_UPDATE_HITS).inc();
        } else {
            bk.getStatsLogger().getCounter(BookkeeperClientCounter.LAC_UPDATE_MISSES).inc();
        }
        lastAddPushed = Math.max(lastAddPushed, lac);
        length = Math.max(length, len);
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies. This
     * call obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @param cb
     * @param ctx
     */

    public void asyncReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
                @Override
                public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                    if (rc == BKException.Code.OK) {
                        updateLastConfirmed(data.lastAddConfirmed, data.length);
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    } else {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            };
        new ReadLastConfirmedOp(this, innercb).initiate();
    }

    /**
     * Obtains asynchronously the last confirmed write from a quorum of bookies.
     * It is similar as
     * {@link #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)},
     * but it doesn't wait all the responses from the quorum. It would callback
     * immediately if it received a LAC which is larger than current LAC.
     *
     * @see #asyncTryReadLastConfirmed(org.apache.bookkeeper.client.AsyncCallback.ReadLastConfirmedCallback, Object)
     *
     * @param cb
     *          callback to return read last confirmed
     * @param ctx
     *          callback context
     */
    public void asyncTryReadLastConfirmed(final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);
            @Override
            public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                if (rc == BKException.Code.OK) {
                    updateLastConfirmed(data.lastAddConfirmed, data.length);
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            }
        };
        new TryReadLastConfirmedOp(this, innercb, getLastAddConfirmed()).initiate();
    }

    public void asyncReadLastConfirmedLongPoll(final long timeOutInMillis, final ReadLastConfirmedCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedComplete(BKException.Code.OK, lastEntryId, ctx);
            return;
        }
        ReadLastConfirmedOp.LastConfirmedDataCallback innercb = new ReadLastConfirmedOp.LastConfirmedDataCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);
            @Override
            public void readLastConfirmedDataComplete(int rc, DigestManager.RecoveryData data) {
                if (rc == BKException.Code.OK) {
                    updateLastConfirmed(data.lastAddConfirmed, data.length);
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, data.lastAddConfirmed, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedComplete(rc, INVALID_ENTRY_ID, ctx);
                    }
                }
            }
        };
        new ReadLastConfirmedLongPollOp(this, innercb, getLastAddConfirmed(), timeOutInMillis).initiate();
    }

    public void asyncReadLastConfirmedAndEntry(final long timeOutInMillis, final AsyncCallback.ReadLastConfirmedAndEntryCallback cb, final Object ctx) {
        asyncReadLastConfirmedAndEntry(timeOutInMillis, false, cb, ctx);
    }

    public void asyncReadLastConfirmedAndEntry(final long timeOutInMillis, final boolean parallel, final AsyncCallback.ReadLastConfirmedAndEntryCallback cb, final Object ctx) {
        boolean isClosed;
        long lastEntryId;
        synchronized (this) {
            isClosed = metadata.isClosed();
            lastEntryId = metadata.getLastEntryId();
        }
        if (isClosed) {
            cb.readLastConfirmedAndEntryComplete(BKException.Code.OK, lastEntryId, null, ctx);
            return;
        }
        ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback innercb = new ReadLastConfirmedAndEntryOp.LastConfirmedAndEntryCallback() {
            AtomicBoolean completed = new AtomicBoolean(false);
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry) {
                if (rc == BKException.Code.OK) {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedAndEntryComplete(rc, lastAddConfirmed, entry, ctx);
                    }
                } else {
                    if (completed.compareAndSet(false, true)) {
                        cb.readLastConfirmedAndEntryComplete(rc, INVALID_ENTRY_ID, null, ctx);
                    }
                }
            }
        };
        new ReadLastConfirmedAndEntryOp(this, innercb, getLastAddConfirmed(), timeOutInMillis, bk.scheduler).parallelRead(parallel).initiate();
    }

    /**
     * Context objects for synchronous call to read last confirmed.
     */
    static class LastConfirmedCtx {
        final static long ENTRY_ID_PENDING = -10;
        long response;
        int rc;

        LastConfirmedCtx() {
            this.response = ENTRY_ID_PENDING;
        }

        void setLastConfirmed(long lastConfirmed) {
            this.response = lastConfirmed;
        }

        long getlastConfirmed() {
            return this.response;
        }

        void setRC(int rc) {
            this.rc = rc;
        }

        int getRC() {
            return this.rc;
        }

        boolean ready() {
            return (this.response != ENTRY_ID_PENDING);
        }
    }

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies. This call
     * obtains the the last add confirmed each bookie has received for this ledger
     * and returns the maximum. If the ledger has been closed, the value returned by this
     * call may not correspond to the id of the last entry of the ledger, since it reads
     * the hint of bookies. Consequently, in the case the ledger has been closed, it may
     * return a different value than getLastAddConfirmed, which returns the local value
     * of the ledger handle.
     *
     * @see #getLastAddConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long readLastConfirmed()
            throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized(ctx) {
            while(!ctx.ready()) {
                ctx.wait();
            }
        }

        if(ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    /**
     * Obtains synchronously the last confirmed write from a quorum of bookies.
     * It is similar as {@link #readLastConfirmed()}, but it doesn't wait all the responses
     * from the quorum. It would callback immediately if it received a LAC which is larger
     * than current LAC.
     *
     * @see #readLastConfirmed()
     *
     * @return The entry id of the last confirmed write or {@link #INVALID_ENTRY_ID INVALID_ENTRY_ID}
     *         if no entry has been confirmed
     * @throws InterruptedException
     * @throws BKException
     */
    public long tryReadLastConfirmed() throws InterruptedException, BKException {
        LastConfirmedCtx ctx = new LastConfirmedCtx();
        asyncTryReadLastConfirmed(new SyncReadLastConfirmedCallback(), ctx);
        synchronized (ctx) {
            while (!ctx.ready()) {
                ctx.wait();
            }
        }
        if (ctx.getRC() != BKException.Code.OK) throw BKException.create(ctx.getRC());
        return ctx.getlastConfirmed();
    }

    // close the ledger and send fails to all the adds in the pipeline
    void handleUnrecoverableErrorDuringAdd(int rc) {
        if (metadata.isInRecovery()) {
            // we should not close ledger if ledger is recovery mode
            // otherwise we may lose entry.
            errorOutPendingAdds(rc);
            return;
        }
        asyncCloseInternal(NoopCloseCallback.instance, null, rc);
    }

    void errorOutPendingAdds(int rc) {
        errorOutPendingAdds(rc, drainPendingAddsToErrorOut());
    }

    synchronized List<PendingAddOp> drainPendingAddsToErrorOut() {
        PendingAddOp pendingAddOp;
        List<PendingAddOp> opsDrained = new ArrayList<PendingAddOp>(pendingAddOps.size());
        while ((pendingAddOp = pendingAddOps.poll()) != null) {
            bk.getStatsLogger().getCounter(BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_PENDING_ADD).dec();
            addToLength(-pendingAddOp.entryLength);
            opsDrained.add(pendingAddOp);
        }
        return opsDrained;
    }

    void errorOutPendingAdds(int rc, List<PendingAddOp> ops) {
        for (PendingAddOp op : ops) {
            op.submitCallback(rc);
        }
    }

    void sendAddSuccessCallbacks() {
        // Start from the head of the queue and proceed while there are
        // entries that have had all their responses come back
        PendingAddOp pendingAddOp;
        while ((pendingAddOp = pendingAddOps.peek()) != null
               && blockAddCompletions.get() == 0) {
            if (!pendingAddOp.completed) {
                return;
            }
            pendingAddOps.remove();
            bk.getStatsLogger().getCounter(BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_PENDING_ADD).dec();
            setLastAddConfirmed(pendingAddOp.entryId);
            pendingAddOp.submitCallback(BKException.Code.OK);
        }

    }

    EnsembleInfo replaceBookieInMetadata(final Map<Integer, InetSocketAddress> failedBookies)
            throws BKException.BKNotEnoughBookiesException {
        final ArrayList<InetSocketAddress> newEnsemble = new ArrayList<InetSocketAddress>();
        final long newEnsembleStartEntry = getLastAddConfirmed() + 1;
        final HashSet<Integer> replacedBookies = new HashSet<Integer>();
        synchronized (metadata) {
            newEnsemble.addAll(metadata.currentEnsemble);
            for (Map.Entry<Integer, InetSocketAddress> entry : failedBookies.entrySet()) {
                int idx = entry.getKey();
                InetSocketAddress addr = entry.getValue();
                LOG.info("Handling failure of bookie: {} index: {}", addr, idx);
                if (!newEnsemble.get(idx).equals(addr)) {
                    // ensemble has already changed, failure of this addr is immaterial
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Write did not succeed to {}, bookieIndex {}, but we have already fixed it.",
                                  addr, idx);
                    }
                    continue;
                }
                try {
                    InetSocketAddress newBookie = bk.bookieWatcher.replaceBookie(newEnsemble, idx);
                    newEnsemble.set(idx, newBookie);
                    replacedBookies.add(idx);
                } catch (BKException.BKNotEnoughBookiesException e) {
                    // if there is no bookie replaced, we throw not enough bookie exception
                    if (replacedBookies.size() <= 0) {
                        throw e;
                    } else {
                        break;
                    }
                }
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Changing ensemble from: {} to: {} for ledger: {} starting at entry: {}," +
                        " failed bookies: {}, replaced bookies: {}",
                          new Object[] { metadata.currentEnsemble, newEnsemble, ledgerId,
                                  (getLastAddConfirmed() + 1), failedBookies, replacedBookies });
            }
            metadata.addEnsemble(newEnsembleStartEntry, newEnsemble);
        }
        return new EnsembleInfo(newEnsemble, failedBookies, replacedBookies);
    }

    void handleBookieFailure(final Map<Integer, InetSocketAddress> failedBookies) {
        blockAddCompletions.incrementAndGet();

        synchronized (metadata) {
            try {
                EnsembleInfo ensembleInfo = replaceBookieInMetadata(failedBookies);
                if (ensembleInfo.replacedBookies.isEmpty()) {
                    blockAddCompletions.decrementAndGet();
                    return;
                }
                writeLedgerConfig(new ChangeEnsembleCb(ensembleInfo));
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.error("Could not get additional bookie to "
                          + "remake ensemble, closing ledger: " + ledgerId);
                handleUnrecoverableErrorDuringAdd(e.getCode());
                return;
            }
        }
    }

    // Contains newly reformed ensemble, bookieIndex, failedBookieAddress
    static final class EnsembleInfo {
        private final ArrayList<InetSocketAddress> newEnsemble;
        private final Map<Integer, InetSocketAddress> failedBookies;
        final Set<Integer> replacedBookies;

        public EnsembleInfo(ArrayList<InetSocketAddress> newEnsemble,
                            Map<Integer, InetSocketAddress> failedBookies,
                            Set<Integer> replacedBookies) {
            this.newEnsemble = newEnsemble;
            this.failedBookies = failedBookies;
            this.replacedBookies = replacedBookies;
        }
    }

    /**
     * Callback which is updating the ledgerMetadata in zk with the newly
     * reformed ensemble. On MetadataVersionException, will reread latest
     * ledgerMetadata and act upon.
     */
    private final class ChangeEnsembleCb extends OrderedSafeGenericCallback<Void> {
        private final EnsembleInfo ensembleInfo;

        ChangeEnsembleCb(EnsembleInfo ensembleInfo) {
            super(bk.mainWorkerPool, ledgerId);
            this.ensembleInfo = ensembleInfo;
        }

        @Override
        public void safeOperationComplete(final int rc, Void result) {
            if (rc == BKException.Code.MetadataVersionException) {
                // We changed the ensemble, but got a version exception. We
                // should still consider this as an ensemble change
                bk.getStatsLogger().getCounter(
                        BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_ENSEMBLE_CHANGE).inc();
                rereadMetadata(new ReReadLedgerMetadataCb(rc,
                                       ensembleInfo));
                return;
            } else if (rc != BKException.Code.OK) {
                LOG.error(
                        "Could not persist ledger metadata while changing ensemble to: {}, closing ledger : {}.",
                        ensembleInfo.newEnsemble, rc);
                handleUnrecoverableErrorDuringAdd(rc);
                return;
            }
            blockAddCompletions.decrementAndGet();

            // We've successfully changed an ensemble
            bk.getStatsLogger().getCounter(
                    BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_ENSEMBLE_CHANGE).inc();
            // the failed bookie has been replaced
            unsetSuccessAndSendWriteRequest(ensembleInfo.replacedBookies);
        }

        @Override
        public String toString() {
            return String.format("ChangeEnsemble(%d)", ledgerId);
        }
    }

    /**
     * Callback which is reading the ledgerMetadata present in zk. This will try
     * to resolve the version conflicts.
     */
    private final class ReReadLedgerMetadataCb extends OrderedSafeGenericCallback<LedgerMetadata> {
        private final int rc;
        private final EnsembleInfo ensembleInfo;

        ReReadLedgerMetadataCb(int rc, EnsembleInfo ensembleInfo) {
            super(bk.mainWorkerPool, ledgerId);
            this.rc = rc;
            this.ensembleInfo = ensembleInfo;
        }

        @Override
        public void safeOperationComplete(int newrc, LedgerMetadata newMeta) {
            if (newrc != BKException.Code.OK) {
                LOG.error("Error reading new metadata from ledger "
                        + "after changing ensemble, code=" + newrc);
                handleUnrecoverableErrorDuringAdd(rc);
            } else {
                if (!resolveConflict(newMeta)) {
                    LOG.error("Could not resolve ledger metadata conflict while changing ensemble to: {},"
                            + " old meta data is \n {} \n, new meta data is \n {}, closing ledger",
                            new Object[] { ensembleInfo.newEnsemble, metadata, newMeta });
                    handleUnrecoverableErrorDuringAdd(rc);
                }
            }
        }

        @Override
        public String toString() {
            return String.format("ReReadLedgerMetadata(%d)", ledgerId);
        }

        /**
         * Specific resolve conflicts happened when multiple bookies failures in same ensemble.
         * <p>
         * Resolving the version conflicts between local ledgerMetadata and zk
         * ledgerMetadata. This will do the following:
         * <ul>
         * <li>
         * check whether ledgerMetadata state matches of local and zk</li>
         * <li>
         * if the zk ledgerMetadata still contains the failed bookie, then
         * update zookeeper with the newBookie otherwise send write request</li>
         * </ul>
         * </p>
         */
        private boolean resolveConflict(LedgerMetadata newMeta) {
            // make sure the ledger isn't closed by other ones.
            if (metadata.getState() != newMeta.getState()) {
                return false;
            }

            // We should check number of ensembles since there are two kinds of metadata conflicts:
            // - Case 1: Multiple bookies involved in ensemble change.
            //           Number of ensembles should be same in this case.
            // - Case 2: Recovery (Auto/Manually) replaced ensemble and ensemble changed.
            //           The metadata changed due to ensemble change would have one more ensemble
            //           than the metadata changed by recovery.
            int diff = newMeta.getEnsembles().size() - metadata.getEnsembles().size();
            if (0 != diff) {
                if (-1 == diff) {
                    // Case 1: metadata is changed by other ones (e.g. Recovery)
                    return updateMetadataIfPossible(newMeta);
                }
                return false;
            }

            //
            // Case 2:
            //
            // If the failed the bookie is still existed in the metadata (in zookeeper), it means that
            // the ensemble change of the failed bookie is failed due to metadata conflicts. so try to
            // update the ensemble change metadata again. Otherwise, it means that the ensemble change
            // is already succeed, unset the success and re-adding entries.
            if (!areFailedBookiesReplaced(newMeta, ensembleInfo)) {
                // If the in-memory data doesn't contains the failed bookie, it means the ensemble change
                // didn't finish, so try to resolve conflicts with the metadata read from zookeeper and
                // update ensemble changed metadata again.
                if (areFailedBookiesReplaced(metadata, ensembleInfo)) {
                    return updateMetadataIfPossible(newMeta);
                }
            } else {
                // We've successfully changed an ensemble
                bk.getStatsLogger().getCounter(
                        BookkeeperClientStatsLogger.BookkeeperClientCounter.NUM_ENSEMBLE_CHANGE).inc();
                // the failed bookie has been replaced
                blockAddCompletions.decrementAndGet();
                unsetSuccessAndSendWriteRequest(ensembleInfo.replacedBookies);
            }
            return true;
        }

        /**
         * Check whether all the failed bookies are replaced.
         *
         * @param newMeta
         *          new ledger metadata
         * @param ensembleInfo
         *          ensemble info used for ensemble change.
         * @return true if all failed bookies are replaced, false otherwise
         */
        private boolean areFailedBookiesReplaced(LedgerMetadata newMeta, EnsembleInfo ensembleInfo) {
            boolean replaced = true;
            for (Map.Entry<Integer, InetSocketAddress> entry : ensembleInfo.failedBookies.entrySet()) {
                replaced = !newMeta.currentEnsemble.get(entry.getKey()).equals(entry.getValue());
            }
            return replaced;
        }

        private boolean updateMetadataIfPossible(LedgerMetadata newMeta) {
            // if the local metadata is newer than zookeeper metadata, it means that metadata is updated
            // again when it was trying re-reading the metatada, re-kick the reread again
            if (metadata.isNewerThan(newMeta)) {
                rereadMetadata(this);
                return true;
            }
            // make sure the metadata doesn't changed by other ones.
            if (metadata.isConflictWith(newMeta)) {
                return false;
            }
            LOG.info("Resolve ledger metadata conflict while changing ensemble to: {},"
                    + " old meta data is \n {} \n, new meta data is \n {}.", new Object[] {
                    ensembleInfo.newEnsemble, metadata, newMeta });
            // update znode version
            metadata.setVersion(newMeta.getVersion());
            // merge ensemble infos from new meta except last ensemble
            // since they might be modified by recovery tool.
            metadata.mergeEnsembles(newMeta.getEnsembles());
            writeLedgerConfig(new ChangeEnsembleCb(ensembleInfo));
            return true;
        }

    }

    void unsetSuccessAndSendWriteRequest(final Set<Integer> bookies) {
        for (PendingAddOp pendingAddOp : pendingAddOps) {
            for (Integer bookieIndex: bookies) {
                pendingAddOp.unsetSuccessAndSendWriteRequest(bookieIndex);
            }
        }
    }

    void rereadMetadata(final GenericCallback<LedgerMetadata> cb) {
        bk.getLedgerManager().readLedgerMetadata(ledgerId, cb);
    }

    void recover(GenericCallback<Void> finalCb) {
        recover(finalCb, null);
    }

    void recover(GenericCallback<Void> finalCb,
                 @VisibleForTesting BookkeeperInternalCallbacks.ReadEntryListener listener) {
        final GenericCallback<Void> cb = new TimedGenericCallback<Void>(finalCb, BKException.Code.OK,
                this.getStatsLogger().getOpStatsLogger(BookkeeperClientOp.LEDGER_RECOVER));

        boolean wasClosed = false;
        boolean wasInRecovery = false;

        synchronized (this) {
            if (metadata.isClosed()) {
                lastAddConfirmed = lastAddPushed = metadata.getLastEntryId();
                length = metadata.getLength();
                wasClosed = true;
            } else {
                wasClosed = false;
                if (metadata.isInRecovery()) {
                    wasInRecovery = true;
                } else {
                    wasInRecovery = false;
                    metadata.markLedgerInRecovery();
                }
            }
        }

        if (wasClosed) {
            // We are already closed, nothing to do
            cb.operationComplete(BKException.Code.OK, null);
            return;
        }

        if (wasInRecovery) {
            // if metadata is already in recover, dont try to write again,
            // just do the recovery from the starting point
            new LedgerRecoveryOp(LedgerHandle.this, cb)
                        .parallelRead(bk.getConf().getEnableParallelRecoveryRead())
                        .readBatchSize(bk.getConf().getRecoveryReadBatchSize())
                        .setCouldClose(true)
                        .setEntryListener(listener)
                        .initiate();
            return;
        }

        final LedgerRecoveryOp recoveryOp = new LedgerRecoveryOp(LedgerHandle.this, cb)
                .parallelRead(bk.getConf().getEnableParallelRecoveryRead())
                .readBatchSize(bk.getConf().getRecoveryReadBatchSize())
                .setCouldClose(false).setEntryListener(listener);
        // Issue the recovery op & update ledger config in parallel.
        recoveryOp.initiate();
        writeLedgerConfig(new OrderedSafeGenericCallback<Void>(bk.mainWorkerPool, ledgerId) {
            @Override
            public void safeOperationComplete(final int rc, Void result) {
                if (rc == BKException.Code.MetadataVersionException) {
                    recoveryOp.cancel();
                    rereadMetadata(new OrderedSafeGenericCallback<LedgerMetadata>(bk.mainWorkerPool,
                                                                                  ledgerId) {
                        @Override
                        public void safeOperationComplete(int rc, LedgerMetadata newMeta) {
                            if (rc != BKException.Code.OK) {
                                cb.operationComplete(rc, null);
                            } else {
                                metadata = newMeta;
                                recover(cb);
                            }
                        }
                        @Override
                        public String toString() {
                            return String.format("ReReadMetadataForRecover(%d)", ledgerId);
                        }
                    });
                } else if (rc == BKException.Code.OK) {
                    recoveryOp.notifyClose();
                } else {
                    recoveryOp.cancel();
                    LOG.error("Error writing ledger config {} of ledger {}", rc, ledgerId);
                    cb.operationComplete(rc, null);
                }
            }
            @Override
            public String toString() {
                return String.format("WriteLedgerConfigForRecoer(%d)", ledgerId);
            }
        });
    }

    public BookkeeperClientStatsLogger getStatsLogger() {
        // We log everything to one client logger. So just return the stats logger for bk
        return bk.getStatsLogger();
    }

    static class NoopCloseCallback implements CloseCallback {
        static NoopCloseCallback instance = new NoopCloseCallback();

        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            if (rc != BKException.Code.OK) {
                LOG.warn("Close failed: " + BKException.getMessage(rc));
            }
            // noop
        }
    }

    private static class SyncReadCallback implements ReadCallback {
        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param seq
         *          sequence of entries
         * @param ctx
         *          control object
         */
        @Override
        public void readComplete(int rc, LedgerHandle lh,
                                 Enumeration<LedgerEntry> seq, Object ctx) {

            SyncCounter counter = (SyncCounter) ctx;
            synchronized (counter) {
                counter.setSequence(seq);
                counter.setrc(rc);
                counter.dec();
                counter.notify();
            }
        }
    }

    private static class SyncAddCallback implements AddCallback {
        long entryId = -1;

        /**
         * Implementation of callback interface for synchronous read method.
         *
         * @param rc
         *          return code
         * @param lh
         *          ledger handle
         * @param entry
         *          entry identifier
         * @param ctx
         *          control object
         */
        @Override
        public void addComplete(int rc, LedgerHandle lh, long entry, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;

            this.entryId = entry;
            counter.setrc(rc);
            counter.dec();
        }
    }

    private static class SyncReadLastConfirmedCallback implements ReadLastConfirmedCallback {
        /**
         * Implementation of  callback interface for synchronous read last confirmed method.
         */
        @Override
        public void readLastConfirmedComplete(int rc, long lastConfirmed, Object ctx) {
            LastConfirmedCtx lcCtx = (LastConfirmedCtx) ctx;

            synchronized(lcCtx) {
                lcCtx.setRC(rc);
                lcCtx.setLastConfirmed(lastConfirmed);
                lcCtx.notify();
            }
        }
    }

    private static class SyncCloseCallback implements CloseCallback {
        /**
         * Close callback method
         *
         * @param rc
         * @param lh
         * @param ctx
         */
        @Override
        public void closeComplete(int rc, LedgerHandle lh, Object ctx) {
            SyncCounter counter = (SyncCounter) ctx;
            counter.setrc(rc);
            synchronized (counter) {
                counter.dec();
                counter.notify();
            }
        }
    }
}
