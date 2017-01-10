/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bookkeeper.client;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.bookkeeper.client.AsyncCallback.AddCallback;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieProtocol;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.client.BookKeeperClientStats.RESPONSE_CODE;

/**
 * This represents a pending add operation. When it has got success from all
 * bookies, it sees if its at the head of the pending adds queue, and if yes,
 * sends ack back to the application. If a bookie fails, a replacement is made
 * and placed at the same position in the ensemble. The pending adds are then
 * rereplicated.
 *
 *
 */
class PendingAddOp implements WriteCallback, TimerTask {
    final static Logger LOG = LoggerFactory.getLogger(PendingAddOp.class);

    ChannelBuffer toSend;
    AddCallback cb;
    Object ctx;
    long entryId;
    int entryLength;

    DistributionSchedule.AckSet ackSet;
    boolean completed = false;

    LedgerHandle lh;
    boolean isRecoveryAdd = false;
    long requestTimeNanos;

    final int timeoutSec;
    final boolean delayEnsembleChange;

    Timeout timeout = null;

    PendingAddOp(LedgerHandle lh, AddCallback cb, Object ctx) {
        this.lh = lh;
        this.cb = cb;
        this.ctx = ctx;
        this.entryId = LedgerHandle.INVALID_ENTRY_ID;

        this.ackSet = lh.distributionSchedule.getAckSet();

        this.timeoutSec = lh.bk.getConf().getAddEntryQuorumTimeout();
        this.delayEnsembleChange = lh.bk.getConf().getDelayEnsembleChange();
        this.lh.numPendingAddsGauge.inc();
    }

    /**
     * Enable the recovery add flag for this operation.
     * @see LedgerHandle#asyncRecoveryAddEntry
     */
    PendingAddOp enableRecoveryAdd() {
        isRecoveryAdd = true;
        return this;
    }

    void setEntryId(long entryId) {
        this.entryId = entryId;
    }

    void sendWriteRequest(int bookieIndex) {
        int flags = isRecoveryAdd ? BookieProtocol.FLAG_RECOVERY_ADD : BookieProtocol.FLAG_NONE;

        lh.bk.bookieClient.addEntry(lh.metadata.currentEnsemble.get(bookieIndex), lh.ledgerId, lh.ledgerKey, entryId, toSend,
                this, bookieIndex, flags);
    }

    @Override
    public void run(Timeout timeout) {
        timeoutQuorumWait();
    }

    void timeoutQuorumWait() {
        lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.TIMEOUT_ADD_ENTRY)
                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
        try {
            lh.bk.mainWorkerPool.submitOrdered(lh.ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    if (completed) {
                        return;
                    }
                    lh.handleUnrecoverableErrorDuringAdd(BKException.Code.AddEntryQuorumTimeoutException);
                }
                @Override
                public String toString() {
                    return String.format("AddEntryQuorumTimeout(lid=%d, eid=%d)", lh.ledgerId, entryId);
                }
            });
        } catch (RejectedExecutionException e) {
            LOG.warn("Timeout add entry quorum wait failed {} entry: {}", lh.ledgerId, entryId);
        }
    }

    void unsetSuccessAndSendWriteRequest(int bookieIndex) {
        if (toSend == null) {
            // this addOp hasn't yet had its mac computed. When the mac is
            // computed, its write requests will be sent, so no need to send it
            // now
            return;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Unsetting success for ledger: " + lh.ledgerId + " entry: " + entryId + " bookie index: "
                      + bookieIndex);
        }

        // if we had already heard a success from this array index, need to
        // increment our number of responses that are pending, since we are
        // going to unset this success
        if (!ackSet.removeBookieAndCheck(bookieIndex)) {
            // unset completed if this results in loss of ack quorum
            completed = false;
        }

        sendWriteRequest(bookieIndex);
    }

    void initiate(ChannelBuffer toSend, int entryLength) {
        if (timeoutSec > 0) {
            this.timeout = lh.bk.bookieClient.scheduleTimeout(this, timeoutSec, TimeUnit.SECONDS);
        }
        this.requestTimeNanos = MathUtils.nowInNano();
        this.toSend = toSend;
        this.entryLength = entryLength;
        for (int bookieIndex : lh.distributionSchedule.getWriteSet(entryId)) {
            sendWriteRequest(bookieIndex);
        }
    }

    @Override
    public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
        int bookieIndex = (Integer) ctx;

        if (!lh.metadata.currentEnsemble.get(bookieIndex).equals(addr)) {
            // ensemble has already changed, failure of this addr is immaterial
            if (LOG.isDebugEnabled()) {
                LOG.debug("Write did not succeed: " + ledgerId + ", " + entryId + ". But we have already fixed it.");
            }
            return;
        }

        // must record all acks, even if complete (completion can be undone by an ensemble change)
        boolean ackQuorum = false;
        if (BKException.Code.OK == rc) {
            ackQuorum = ackSet.completeBookieAndCheck(bookieIndex);
        }

        if (completed) {
            // even the add operation is completed, but because we don't reset completed flag back to false when
            // #unsetSuccessAndSendWriteRequest doesn't break ack quorum constraint. we still have current pending
            // add op is completed but never callback. so do a check here to complete again.
            //
            // E.g. entry x is going to complete.
            //
            // 1) entry x + k hits a failure. lh.handleBookieFailure increases blockAddCompletions to 1, for ensemble change
            // 2) entry x receives all responses, sets completed to true but fails to send success callback because
            //    blockAddCompletions is 1
            // 3) ensemble change completed. lh unset success starting from x to x+k, but since the unset doesn't break ackSet
            //    constraint. #removeBookieAndCheck doesn't set completed back to false.
            // 4) so when the retry request on new bookie completes, it finds the pending op is already completed.
            //    we have to trigger #sendAddSuccessCallbacks
            //
            sendAddSuccessCallbacks();
            // I am already finished, ignore incoming responses.
            // otherwise, we might hit the following error handling logic, which might cause bad things.
            return;
        }

        switch (rc) {
        case BKException.Code.OK:
            // continue
            break;
        case BKException.Code.ClientClosedException:
            // bookie client is closed.
            lh.errorOutPendingAdds(rc);
            return;
        case BKException.Code.LedgerFencedException:
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fencing exception on write: " + ledgerId + ", " + entryId);
            }
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        case BKException.Code.UnauthorizedAccessException:
            LOG.warn("Unauthorized access exception on write: {}, {}", ledgerId, entryId);
            lh.handleUnrecoverableErrorDuringAdd(rc);
            return;
        default:
            if (delayEnsembleChange) {
                if (ackSet.failBookieAndCheck(bookieIndex, addr) || rc == BKException.Code.WriteOnReadOnlyBookieException) {
                    Map<Integer, BookieSocketAddress> failedBookies = ackSet.getFailedBookies();
                    LOG.warn("Failed to write entry ({}, {}) to bookies {}, handling failures.",
                             new Object[] { ledgerId, entryId, failedBookies });
                    // we can't meet ack quorum requirement, trigger ensemble change.
                    lh.handleBookieFailure(failedBookies);
                } else {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Failed to write entry ({}, {}) to bookie ({}, {})," +
                                  " but it didn't break ack quorum, delaying ensemble change : {}",
                                  new Object[] { ledgerId, entryId, bookieIndex, addr, BKException.getMessage(rc) });
                    }
                }
            } else {
                LOG.warn("Failed to write entry ({}, {}): {}",
                         new Object[] { ledgerId, entryId, BKException.getMessage(rc) });
                lh.handleBookieFailure(ImmutableMap.of(bookieIndex, addr));
            }
            return;
        }

        if (ackQuorum && !completed) {
            completed = true;

            sendAddSuccessCallbacks();
        }
    }

    void sendAddSuccessCallbacks() {
        lh.sendAddSuccessCallbacks();
    }

    void submitCallback(final int rc) {
        if (null != timeout) {
            timeout.cancel();
        }
        lh.getStatsLogger().scope(BookKeeperClientStats.ADD_ENTRY)
            .scope(RESPONSE_CODE).getCounter(String.valueOf(rc)).inc();
        if (rc != BKException.Code.OK) {
            lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.ADD_ENTRY)
                    .registerFailedEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
            lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.ADD_ENTRY_BYTES)
                    .registerFailedEvent(entryLength);
        } else {
            lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.ADD_ENTRY)
                    .registerSuccessfulEvent(MathUtils.elapsedMicroSec(requestTimeNanos));
            lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.ADD_ENTRY_BYTES)
                    .registerSuccessfulEvent(entryLength);
        }
        long completeStartNanos = MathUtils.nowInNano();
        cb.addComplete(rc, lh, entryId, ctx);
        lh.getStatsLogger().getOpStatsLogger(BookKeeperClientStats.ADD_COMPLETE)
                .registerSuccessfulEvent(MathUtils.elapsedMicroSec(completeStartNanos));
        lh.numPendingAddsGauge.dec();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PendingAddOp(lid:").append(lh.ledgerId)
          .append(", eid:").append(entryId).append(", completed:")
          .append(completed).append(")");
        return sb.toString();
    }

}
