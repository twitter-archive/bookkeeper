package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLastConfirmedAndEntryOp extends SafeRunnable
        implements BookkeeperInternalCallbacks.ReadEntryCallback {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedAndEntryOp.class);

    int speculativeReadTimeout;
    final int maxSpeculativeReadTimeout;
    final private ScheduledExecutorService scheduler;
    ReadLACAndEntryRequest request;
    final Set<InetSocketAddress> heardFromHosts;
    final Set<InetSocketAddress> emptyResponsesFromHosts;
    final int maxMissedReadsAllowed;
    boolean parallelRead = false;
    final AtomicBoolean requestComplete = new AtomicBoolean(false);

    final long requestTimeNano;
    private final LedgerHandle lh;
    private final LastConfirmedAndEntryCallback cb;

    private int numResponsesPending;
    private final int numEmptyResponsesAllowed;
    private volatile boolean hasValidResponse = false;
    private final long prevLastAddConfirmed;
    private long lastAddConfirmed;
    private long timeOutInMillis;

    abstract class ReadLACAndEntryRequest extends LedgerEntry {

        final AtomicBoolean complete = new AtomicBoolean(false);

        int rc = BKException.Code.OK;
        int firstError = BKException.Code.OK;
        int numMissedEntryReads = 0;

        final ArrayList<InetSocketAddress> ensemble;
        final List<Integer> orderedEnsemble;

        ReadLACAndEntryRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(lId, eId);

            this.ensemble = ensemble;
            this.orderedEnsemble = lh.bk.placementPolicy.reorderReadLACSequence(ensemble,
                lh.distributionSchedule.getWriteSet(entryId));
        }

        synchronized int getFirstError() {
            return firstError;
        }

        /**
         * Execute the read request.
         */
        abstract void read();

        /**
         * Complete the read request from <i>host</i>.
         *
         * @param host
         *          host that respond the read
         * @param buffer
         *          the data buffer
         * @return return true if we managed to complete the entry;
         *         otherwise return false if the read entry is not complete or it is already completed before
         */
        boolean complete(InetSocketAddress host, final ChannelBuffer buffer, long entryId) {
            ChannelBufferInputStream is;
            try {
                is = lh.macManager.verifyDigestAndReturnData(entryId, buffer);
            } catch (BKException.BKDigestMatchException e) {
                logErrorAndReattemptRead(host, "Mac mismatch", BKException.Code.DigestMatchException);
                return false;
            }

            if (!complete.getAndSet(true)) {
                rc = BKException.Code.OK;
                entryDataStream = is;
                this.entryId = entryId;

                /*
                 * The length is a long and it is the last field of the metadata of an entry.
                 * Consequently, we have to subtract 8 from METADATA_LENGTH to get the length.
                 */
                length = buffer.getLong(DigestManager.METADATA_LENGTH - 8);
                return true;
            } else {
                return false;
            }
        }

        /**
         * Fail the request with given result code <i>rc</i>.
         *
         * @param rc
         *          result code to fail the request.
         * @return true if we managed to fail the entry; otherwise return false if it already failed or completed.
         */
        boolean fail(int rc) {
            if (complete.compareAndSet(false, true)) {
                this.rc = rc;
                translateAndSetFirstError(rc);
                completeRequest();
                return true;
            } else {
                return false;
            }
        }

        synchronized private void translateAndSetFirstError(int rc) {
            if (BKException.Code.OK == firstError ||
                BKException.Code.NoSuchEntryException == firstError ||
                BKException.Code.NoSuchLedgerExistsException == firstError) {
                firstError = rc;
            } else if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                BKException.Code.NoSuchEntryException != rc &&
                BKException.Code.NoSuchLedgerExistsException != rc) {
                // if other exception rather than NoSuchEntryException is returned
                // we need to update firstError to indicate that it might be a valid read but just failed.
                firstError = rc;
            }
        }


        /**
         * Log error <i>errMsg</i> and reattempt read from <i>host</i>.
         *
         * @param host
         *          host that just respond
         * @param errMsg
         *          error msg to log
         * @param rc
         *          read result code
         */
        synchronized void logErrorAndReattemptRead(InetSocketAddress host, String errMsg, int rc) {
            translateAndSetFirstError(rc);

            if (BKException.Code.NoSuchEntryException == rc ||
                BKException.Code.NoSuchLedgerExistsException == rc) {
                ++numMissedEntryReads;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(errMsg + " while reading entry: " + entryId + " ledgerId: " + lh.ledgerId + " from bookie: "
                    + host);
            }
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         *
         * @param heardFromHosts
         *      the set of hosts that we already received responses.
         * @return host we sent to if we sent. null otherwise.
         */
        abstract InetSocketAddress maybeSendSpeculativeRead(Set<InetSocketAddress> heardFromHosts);

        /**
         * Whether the read request completed.
         *
         * @return true if the read request is completed.
         */
        boolean isComplete() {
            return complete.get();
        }

        /**
         * Get result code of this entry.
         *
         * @return result code.
         */
        int getRc() {
            return rc;
        }

        @Override
        public String toString() {
            return String.format("L%d-E%d", ledgerId, entryId);
        }
    }

    class ParallelReadRequest extends ReadLACAndEntryRequest {

        int numPendings;

        ParallelReadRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);
            numPendings = orderedEnsemble.size();
        }

        @Override
        void read() {
            for (int bookieIndex : orderedEnsemble) {
                InetSocketAddress to = ensemble.get(bookieIndex);
                try {
                    sendReadTo(to, this);
                } catch (InterruptedException ie) {
                    LOG.error("Interrupted reading entry {} : ", this, ie);
                    Thread.currentThread().interrupt();
                    fail(BKException.Code.InterruptedException);
                    return;
                }
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(InetSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(host, errMsg, rc);
            --numPendings;
            // if received all responses or this entry doesn't meet quorum write, complete the request.
            if (numMissedEntryReads > maxMissedReadsAllowed || numPendings == 0) {
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
            }
        }

        @Override
        InetSocketAddress maybeSendSpeculativeRead(Set<InetSocketAddress> heardFromHosts) {
            // no speculative read
            return null;
        }
    }

    class SequenceReadRequest extends ReadLACAndEntryRequest {
        final static int NOT_FOUND = -1;
        int nextReplicaIndexToReadFrom = 0;

        final BitSet sentReplicas;
        final BitSet erroredReplicas;
        final BitSet emptyResponseReplicas;

        SequenceReadRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(ensemble, lId, eId);

            this.sentReplicas = new BitSet(orderedEnsemble.size());
            this.erroredReplicas = new BitSet(orderedEnsemble.size());
            this.emptyResponseReplicas = new BitSet(orderedEnsemble.size());
        }

        private synchronized int getNextReplicaIndexToReadFrom() {
            return nextReplicaIndexToReadFrom;
        }

        private int getReplicaIndex(InetSocketAddress host) {
            int bookieIndex = ensemble.indexOf(host);
            if (bookieIndex == -1) {
                return NOT_FOUND;
            }
            return orderedEnsemble.indexOf(bookieIndex);
        }

        private BitSet getSentToBitSet() {
            BitSet b = new BitSet(ensemble.size());

            for (int i = 0; i < sentReplicas.length(); i++) {
                if (sentReplicas.get(i)) {
                    b.set(orderedEnsemble.get(i));
                }
            }
            return b;
        }

        private BitSet getHeardFromBitSet(Set<InetSocketAddress> heardFromHosts) {
            BitSet b = new BitSet(ensemble.size());
            for (InetSocketAddress i : heardFromHosts) {
                int index = ensemble.indexOf(i);
                if (index != -1) {
                    b.set(index);
                }
            }
            return b;
        }

        private boolean readsOutstanding() {
            return (sentReplicas.cardinality() - erroredReplicas.cardinality() - emptyResponseReplicas.cardinality()) > 0;
        }

        /**
         * Send to next replica speculatively, if required and possible.
         * This returns the host we may have sent to for unit testing.
         * @return host we sent to if we sent. null otherwise.
         */
        @Override
        synchronized InetSocketAddress maybeSendSpeculativeRead(Set<InetSocketAddress> heardFromHosts) {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getEnsembleSize()) {
                return null;
            }

            BitSet sentTo = getSentToBitSet();
            BitSet heardFrom = getHeardFromBitSet(heardFromHosts);
            sentTo.and(heardFrom);

            // only send another read, if we have had no response at all (even for other entries)
            // from any of the other bookies we have sent the request to
            if (sentTo.cardinality() == 0) {
                return sendNextRead();
            } else {
                return null;
            }
        }

        @Override
        void read() {
            sendNextRead();
        }

        synchronized InetSocketAddress sendNextRead() {
            if (nextReplicaIndexToReadFrom >= getLedgerMetadata().getEnsembleSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read

                // Do it a bit pessimistically, only when finished trying all replicas
                // to check whether we received more missed reads than maxMissedReadsAllowed
                if (BKException.Code.BookieHandleNotAvailableException == firstError &&
                    numMissedEntryReads > maxMissedReadsAllowed) {
                    firstError = BKException.Code.NoSuchEntryException;
                }

                fail(firstError);
                return null;
            }

            int replica = nextReplicaIndexToReadFrom;
            int bookieIndex = orderedEnsemble.get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                InetSocketAddress to = ensemble.get(bookieIndex);
                sendReadTo(to, this);
                sentReplicas.set(replica);
                return to;
            } catch (InterruptedException ie) {
                LOG.error("Interrupted reading entry " + this, ie);
                Thread.currentThread().interrupt();
                fail(BKException.Code.InterruptedException);
                return null;
            }
        }

        @Override
        synchronized void logErrorAndReattemptRead(InetSocketAddress host, String errMsg, int rc) {
            super.logErrorAndReattemptRead(host, errMsg, rc);

            int replica = getReplicaIndex(host);
            if (replica == NOT_FOUND) {
                LOG.error("Received error from a host which is not in the ensemble {} {}.", host, ensemble);
                return;
            }

            if (BKException.Code.OK == rc) {
                emptyResponseReplicas.set(replica);
            } else {
                erroredReplicas.set(replica);
            }

            if (!readsOutstanding()) {
                sendNextRead();
            }
        }

        @Override
        boolean complete(InetSocketAddress host, ChannelBuffer buffer, long entryId) {
            boolean completed = super.complete(host, buffer, entryId);
            if (completed) {
                lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.SPECULATIVES_PER_READ_LAC)
                        .registerSuccessfulEvent(getNextReplicaIndexToReadFrom());
            }
            return completed;
        }

        @Override
        boolean fail(int rc) {
            boolean completed = super.fail(rc);
            if (completed) {
                lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.SPECULATIVES_PER_READ_LAC)
                        .registerFailedEvent(getNextReplicaIndexToReadFrom());
            }
            return completed;
        }
    }


    ReadLastConfirmedAndEntryOp(LedgerHandle lh, LastConfirmedAndEntryCallback cb,
                                long lac, long timeOutInMillis, ScheduledExecutorService scheduler) {
        this.lh = lh;
        this.cb = cb;
        this.prevLastAddConfirmed = this.lastAddConfirmed = lac;
        this.timeOutInMillis = timeOutInMillis;
        this.numResponsesPending = 0;
        this.numEmptyResponsesAllowed = getLedgerMetadata().getWriteQuorumSize()
                - getLedgerMetadata().getAckQuorumSize() + 1;
        this.requestTimeNano = MathUtils.nowInNano();
        this.scheduler = scheduler;
        maxMissedReadsAllowed = getLedgerMetadata().getWriteQuorumSize()
            - getLedgerMetadata().getAckQuorumSize();
        speculativeReadTimeout = lh.bk.getConf().getFirstSpeculativeReadLACTimeout();
        maxSpeculativeReadTimeout = lh.bk.getConf().getMaxSpeculativeReadLACTimeout();
        heardFromHosts = new HashSet<InetSocketAddress>();
        emptyResponsesFromHosts = new HashSet<InetSocketAddress>();
    }

    protected LedgerMetadata getLedgerMetadata() {
        return lh.metadata;
    }

    ReadLastConfirmedAndEntryOp parallelRead(boolean enabled) {
        this.parallelRead = enabled;
        return this;
    }

    /**
     * Speculative Read Logic
     */
    @Override
    public void safeRun() {
        if (!requestComplete.get() && !request.isComplete()) {
            if (null != request.maybeSendSpeculativeRead(heardFromHosts)) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Send speculative ReadLAC {} for ledger {} (previousLAC: {}). Hosts heard are {}.",
                        new Object[] {request, lh.getId(), lastAddConfirmed, heardFromHosts });
                }
                if (speculativeReadTimeout > 0) {
                    speculativeReadTimeout = Math.min(maxSpeculativeReadTimeout, speculativeReadTimeout * 2);
                    scheduleSpeculativeRead(speculativeReadTimeout);
                }
            }
        }
    }

    private void scheduleSpeculativeRead(final int speculativeReadTimeout) {
        try {
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    // let the speculative read running this same thread
                    lh.bk.mainWorkerPool.submitOrdered(lh.getId(),
                        ReadLastConfirmedAndEntryOp.this);
                }
            }, speculativeReadTimeout, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException re) {
            LOG.warn("Failed to schedule speculative readLAC for ledger {} (lac = {}, speculativeReadTimeout = {}) : ",
                    new Object[] { lh.getId(), lastAddConfirmed, speculativeReadTimeout, re });
        }
    }

    public void initiate() {
        if (speculativeReadTimeout > 0 && !parallelRead) {
            scheduleSpeculativeRead(speculativeReadTimeout);
        }

        if (parallelRead) {
            request = new ParallelReadRequest(lh.metadata.currentEnsemble, lh.ledgerId, lastAddConfirmed + 1);
        } else {
            request = new SequenceReadRequest(lh.metadata.currentEnsemble, lh.ledgerId, lastAddConfirmed + 1);
        }
        request.read();
    }

    void sendReadTo(InetSocketAddress to, ReadLACAndEntryRequest entry) throws InterruptedException {
        long previousLAC = lastAddConfirmed;
        if (LOG.isDebugEnabled()) {
            LOG.debug("Calling Read LAC and Entry with {} and long polling interval {} on Bookie {} - Parallel {}", new Object[] {previousLAC, timeOutInMillis, to, parallelRead});
        }
        lh.bk.bookieClient.readEntryWaitForLACUpdate(to,
            lh.ledgerId,
            BookieProtocol.LAST_ADD_CONFIRMED,
            previousLAC,
            timeOutInMillis,
            true,
            this, new ReadLastConfirmedAndEntryContext(to));
        this.numResponsesPending++;
    }

    /**
     * Wrapper to get all recovered data from the request
     */
    interface LastConfirmedAndEntryCallback {
        public void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry);
    }

    private static class ReadLastConfirmedAndEntryContext implements BookkeeperInternalCallbacks.ReadEntryCallbackCtx {
        final InetSocketAddress bookie;
        long lac = LedgerHandle.INVALID_ENTRY_ID;

        ReadLastConfirmedAndEntryContext(InetSocketAddress bookie) {
            this.bookie = bookie;
        }

        @Override
        public void setLastAddConfirmed(long lac) {
            this.lac = lac;
        }

        @Override
        public long getLastAddConfirmed() {
            return lac;
        }
    }

    private void submitCallback(int rc, long lastAddConfirmed, LedgerEntry entry) {
        long latencyMicros = MathUtils.elapsedMicroSec(requestTimeNano);
        if (BKException.Code.OK != rc) {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY)
                .registerFailedEvent(latencyMicros);
        } else {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY)
                .registerSuccessfulEvent(latencyMicros);
            Enum op;
            if (this.prevLastAddConfirmed < lastAddConfirmed) {
                op = BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY_HIT;
            } else {
                op = BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY_MISS;
            }
            lh.getStatsLogger().getOpStatsLogger(op).registerSuccessfulEvent(latencyMicros);
        }
        cb.readLastConfirmedAndEntryComplete(rc, lastAddConfirmed, entry);
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} received response for (lid={}, eid={}) : {}",
                new Object[] { getClass().getName(), ledgerId, entryId, rc });
        }
        ReadLastConfirmedAndEntryContext rCtx = (ReadLastConfirmedAndEntryContext) ctx;
        InetSocketAddress bookie = rCtx.bookie;
        numResponsesPending--;
        if (BKException.Code.OK == rc) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received lastAddConfirmed (lac={}) from bookie({}) for (lid={}).",
                    new Object[] { lastAddConfirmed, bookie, ledgerId });
            }

            if (rCtx.getLastAddConfirmed() > lastAddConfirmed) {
                lastAddConfirmed = rCtx.getLastAddConfirmed();
                lh.updateLastConfirmed(rCtx.getLastAddConfirmed(), 0L);
            }

            hasValidResponse = true;

            if (entryId != BookieProtocol.LAST_ADD_CONFIRMED) {
                if (request.complete(bookie, buffer, entryId)) {
                    // callback immediately
                    submitCallback(BKException.Code.OK, lastAddConfirmed, request);
                    requestComplete.set(true);
                    heardFromHosts.add(bookie);
                }
            } else {
                LOG.debug("Empty Response {}, {}", ledgerId, lastAddConfirmed);
                emptyResponsesFromHosts.add(bookie);
                if (emptyResponsesFromHosts.size() >= numEmptyResponsesAllowed) {
                    completeRequest();
                } else {
                    request.logErrorAndReattemptRead(bookie, "Empty Response", rc);
                }
                return;
            }
        } else if (BKException.Code.UnauthorizedAccessException == rc && !requestComplete.get()) {
            submitCallback(rc, lastAddConfirmed, null);
            requestComplete.set(true);
        } else {
            request.logErrorAndReattemptRead(bookie, "Error: " + BKException.getMessage(rc), rc);
            return;
        }

        if (numResponsesPending <= 0) {
            completeRequest();
        }
    }

    private void completeRequest() {
        if (requestComplete.compareAndSet(false, true)) {
            if (!hasValidResponse) {
                // no success called
                submitCallback(request.getFirstError(), lastAddConfirmed, null);
            } else {
                // callback
                submitCallback(BKException.Code.OK, lastAddConfirmed, null);
            }
        }
    }

    @Override
    public String toString() {
        return String.format("ReadLastConfirmedAndEntryOp(lid=%d, lac=%d])", lh.ledgerId, lastAddConfirmed);
    }

}
