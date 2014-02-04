package org.apache.bookkeeper.client;

import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLastConfirmedAndEntryOp implements BookkeeperInternalCallbacks.ReadEntryCallback {
    static final Logger LOG = LoggerFactory.getLogger(ReadLastConfirmedAndEntryOp.class);

    /**
     * Wrapper to get all recovered data from the request
     */
    interface LastConfirmedAndEntryCallback {
        public void readLastConfirmedAndEntryComplete(int rc, long lastAddConfirmed, LedgerEntry entry, boolean updateLACOnly);
    }

    private static class ReadLastConfirmedAndEntryContext implements BookkeeperInternalCallbacks.ReadEntryCallbackCtx {
        final int bookieIndex;
        long lac = LedgerHandle.INVALID_ENTRY_ID;

        ReadLastConfirmedAndEntryContext(int bookieIndex) {
            this.bookieIndex = bookieIndex;
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

    final long requestTimeNano;
    private final LedgerHandle lh;
    private final LastConfirmedAndEntryCallback cb;

    private int numResponsesPending;
    private volatile boolean hasValidResponse = false;
    private volatile boolean completed = false;
    private long lastAddConfirmed;
    private long timeOutInMillis;


    ReadLastConfirmedAndEntryOp(LedgerHandle lh, LastConfirmedAndEntryCallback cb,
                                long lac, long timeOutInMillis) {
        this.lh = lh;
        this.cb = cb;
        this.lastAddConfirmed = lac;
        this.timeOutInMillis = timeOutInMillis;
        this.numResponsesPending = 0;
        this.requestTimeNano = MathUtils.nowInNano();
    }

    public void initiate() {
        long previousLAC = lastAddConfirmed;
        LOG.trace("Calling Read LAC and Entry with {} and long polling interval {}", previousLAC, timeOutInMillis);
        for (int i = 0; i < lh.metadata.currentEnsemble.size(); i++) {
            lh.bk.bookieClient.readEntryWaitForLACUpdate(lh.metadata.currentEnsemble.get(i),
                lh.ledgerId,
                BookieProtocol.LAST_ADD_CONFIRMED,
                previousLAC,
                timeOutInMillis,
                true,
                this, new ReadLastConfirmedAndEntryContext(i));
            this.numResponsesPending++;
        }
    }

    private void submitCallback(int rc, long lastAddConfirmed, LedgerEntry entry) {
        submitCallback(rc, lastAddConfirmed, entry, false);
    }

    private void submitCallback(int rc, long lastAddConfirmed, LedgerEntry entry, boolean updateLACOnly) {
        long latencyMicros = MathUtils.elapsedMicroSec(requestTimeNano);
        if (BKException.Code.OK != rc) {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY)
                .registerFailedEvent(latencyMicros);
        } else {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientStatsLogger.BookkeeperClientOp.READ_LAST_CONFIRMED_AND_ENTRY)
                .registerSuccessfulEvent(latencyMicros);
        }
        cb.readLastConfirmedAndEntryComplete(rc, lastAddConfirmed, entry, updateLACOnly);
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, long entryId, ChannelBuffer buffer, Object ctx) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("{} received response for (lid={}, eid={}) : {}",
                new Object[] { getClass().getName(), ledgerId, entryId, rc });
        }
        ReadLastConfirmedAndEntryContext rCtx = (ReadLastConfirmedAndEntryContext) ctx;
        int bookieIndex = rCtx.bookieIndex;
        numResponsesPending--;
        if (BKException.Code.OK == rc) {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Received lastAddConfirmed (lac={}) from bookie({}) for (lid={}).",
                    new Object[] { lastAddConfirmed, bookieIndex, ledgerId });
            }

            boolean lastAddConfirmedUpdated = false;
            if (rCtx.getLastAddConfirmed() > lastAddConfirmed) {
                lastAddConfirmed = rCtx.getLastAddConfirmed();
                lastAddConfirmedUpdated = true;
            }

            if (entryId != BookieProtocol.LAST_ADD_CONFIRMED) {
                try {
                    LedgerEntry le = new LedgerEntry(ledgerId, entryId);
                    le.entryDataStream = lh.macManager.verifyDigestAndReturnData(entryId, buffer);

                    // callback immediately
                    submitCallback(BKException.Code.OK, lastAddConfirmed, le);
                    hasValidResponse = true;
                } catch (BKException.BKDigestMatchException e) {
                    LOG.error("Mac mismatch for ledger: " + ledgerId + ", entry: " + entryId
                        + " while reading last entry from bookie: "
                        + lh.metadata.currentEnsemble.get(bookieIndex));
                }
            } else {
                if (lastAddConfirmedUpdated) {
                    submitCallback(BKException.Code.OK, lastAddConfirmed, null, true);
                }
                hasValidResponse = true;
            }
        } else if (BKException.Code.UnauthorizedAccessException == rc && !completed) {
            submitCallback(rc, lastAddConfirmed, null);
            completed = true;
        } else if (BKException.Code.NoSuchLedgerExistsException == rc ||
            BKException.Code.NoSuchEntryException == rc) {
            hasValidResponse = true;
        }
        if (numResponsesPending == 0 && !completed) {
            if (!hasValidResponse) {
                // no success called
                submitCallback(BKException.Code.LedgerRecoveryException, lastAddConfirmed, null);
            } else {
                // callback
                submitCallback(BKException.Code.OK, lastAddConfirmed, null);
            }
            completed = true;
        }
    }

}
