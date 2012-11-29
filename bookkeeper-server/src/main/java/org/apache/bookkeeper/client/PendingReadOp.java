package org.apache.bookkeeper.client;

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
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.BKException.BKDigestMatchException;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.RangeReadCallback;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalReadRequest;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalReadResponse;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalRangeReadRequest;
import org.apache.bookkeeper.proto.BookKeeperInternalProtocol.InternalRangeReadResponse;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger.BookkeeperClientOp;
import org.apache.bookkeeper.stats.BookkeeperClientStatsLogger.BookkeeperClientSimpleStatType;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.BookKeeperSharedSemaphore.BKSharedOp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;

/**
 * Sequence of entries of a ledger that represents a pending read operation.
 * When all the data read has come back, the application callback is called.
 * This class could be improved because we could start pushing data to the
 * application as soon as it arrives rather than waiting for the whole thing.
 *
 */
public class PendingReadOp implements Enumeration<LedgerEntry>, ReadEntryCallback,
        RangeReadCallback {
    Logger LOG = LoggerFactory.getLogger(PendingReadOp.class);

    Map<InetSocketAddress, InternalRangeReadRequest> rangeRequestMap;
    Queue<LedgerEntryRequest> seq;
    ReadCallback cb;
    Object ctx;
    LedgerHandle lh;
    long numPendingEntries;
    long startEntryId;
    long endEntryId;
    long requestTimeMillis;
    public class LedgerEntryRequest extends LedgerEntry {
        int nextReplicaIndexToReadFrom = 0;
        AtomicBoolean complete = new AtomicBoolean(false);

        int firstError = BKException.Code.OK;

        final ArrayList<InetSocketAddress> ensemble;

        LedgerEntryRequest(ArrayList<InetSocketAddress> ensemble, long lId, long eId) {
            super(lId, eId);

            this.ensemble = ensemble;
        }

        void sendNextRead() {
            if (nextReplicaIndexToReadFrom >= lh.metadata.getWriteQuorumSize()) {
                // we are done, the read has failed from all replicas, just fail the
                // read
                submitCallback(firstError);
                return;
            }

            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;

            try {
                sendReadTo(ensemble.get(bookieIndex), this);
            } catch (InterruptedException ie) {
                LOG.error("Interrupted reading entry " + this, ie);
                Thread.currentThread().interrupt();
                submitCallback(BKException.Code.ReadException);
            }
        }

        /**
         * The first time we send a request to any bookie, we try to batch them together.
         */
        void sendFirstRead() {
            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom);
            nextReplicaIndexToReadFrom++;
            sendReadBuffered(ensemble.get(bookieIndex), this);
        }

        void logErrorAndReattemptRead(String errMsg, int rc) {
            if (firstError == BKException.Code.OK) {
                firstError = rc;
            }

            int bookieIndex = lh.distributionSchedule.getWriteSet(entryId).get(nextReplicaIndexToReadFrom - 1);
            LOG.error(errMsg + " while reading entry: " + entryId + " ledgerId: " + lh.ledgerId + " from bookie: "
                      + ensemble.get(bookieIndex));

            sendNextRead();
        }

        // return true if we managed to complete the entry
        boolean complete(final ChannelBuffer buffer) {
            ChannelBufferInputStream is;
            try {
                is = lh.macManager.verifyDigestAndReturnData(entryId, buffer);
            } catch (BKDigestMatchException e) {
                logErrorAndReattemptRead("Mac mismatch", BKException.Code.DigestMatchException);
                return false;
            }

            if (!complete.getAndSet(true)) {
                entryDataStream = is;

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

        boolean isComplete() {
            return complete.get();
        }

        public String toString() {
            return String.format("L%d-E%d", ledgerId, entryId);
        }
    }

    PendingReadOp(LedgerHandle lh, long startEntryId, long endEntryId, ReadCallback cb, Object ctx) {

        seq = new ArrayDeque<LedgerEntryRequest>((int) (endEntryId - startEntryId));
        this.cb = cb;
        this.ctx = ctx;
        this.lh = lh;
        this.startEntryId = startEntryId;
        this.endEntryId = endEntryId;
        this.rangeRequestMap = new HashMap<InetSocketAddress, InternalRangeReadRequest>();
        numPendingEntries = endEntryId - startEntryId + 1;
    }

    public void initiate() throws InterruptedException {
        long nextEnsembleChange = startEntryId, i = startEntryId;
        this.requestTimeMillis = MathUtils.now();
        ArrayList<InetSocketAddress> ensemble = null;
        do {

            if (i == nextEnsembleChange) {
                ensemble = lh.metadata.getEnsemble(i);
                nextEnsembleChange = lh.metadata.getNextEnsembleChange(i);
            }
            LedgerEntryRequest entry = new LedgerEntryRequest(ensemble, lh.ledgerId, i);
            seq.add(entry);
            i++;

            entry.sendFirstRead();
        } while (i <= endEntryId);
        // Flush any pending range reads.
        for (Map.Entry<InetSocketAddress, InternalRangeReadRequest> mapEntry : rangeRequestMap.entrySet()) {
            InternalRangeReadRequest request = mapEntry.getValue();
            InetSocketAddress to = mapEntry.getKey();
            sendRangeReadTo(to, request);
        }
        rangeRequestMap.clear();

    }

    void sendReadTo(InetSocketAddress to, LedgerEntryRequest entry) throws InterruptedException {
        lh.getStatsLogger().getSimpleStatLogger(BookkeeperClientSimpleStatType.NUM_PERMITS_TAKEN).inc();
        lh.bkSharedSem.acquire(BKSharedOp.READ_OP);

        lh.bk.bookieClient.readEntry(to, lh.ledgerId, entry.entryId, 
                                     this, entry);
    }

    void sendRangeReadTo(InetSocketAddress to, InternalRangeReadRequest request) {
        // Don't acquire any semaphores as we will remove them anyway.
        lh.bk.bookieClient.rangeReadEntry(to, request, this, request);
    }

    void sendReadBuffered(InetSocketAddress to, LedgerEntryRequest entry) {
        InternalRangeReadRequest request;
        if ((request = rangeRequestMap.get(to)) == null) {
            request = new InternalRangeReadRequest();
            request.ledgerId = lh.ledgerId;
            rangeRequestMap.put(to, request);
        }
        request.numRequests++;
        InternalReadRequest readRequest = new InternalReadRequest(lh.ledgerId, entry.entryId);
        request.requests.put(readRequest, entry);
        // If we have exceeded the threshold, send the request.
        // TODO: Make the size configurable!!.
        if (request.numRequests == 10) {
            request = rangeRequestMap.remove(to);
            sendRangeReadTo(to, request);
        }
    }

    // Uses readEntryComplete to signal completion of each individual entry.
    @Override
    public void rangeReadComplete(int rc, InternalRangeReadResponse response, Object ctx) {
        // We passed the original request as the ctx object so we should be getting it back
        // here.
        InternalRangeReadRequest originalRequest = (InternalRangeReadRequest)ctx;
        // TODO: Make the List<> in the Internal structure a Set<>
        for (InternalReadResponse readResponse : response.responses) {
            LedgerEntryRequest entry;
            if (null == (entry = originalRequest.requests.remove(new InternalReadRequest(
                    readResponse.ledgerId, readResponse.entryId)))) {
                // We got a response for something we did not make a request for.
                LOG.warn("Received a response for ledger:" + readResponse.ledgerId +
                        " and entry:" + readResponse.entryId + " but we did not make this request.");
                continue;
            }
            readEntryComplete(readResponse.returnCode, readResponse.ledgerId, readResponse.entryId,
                    readResponse.responseBody, entry);
        }
        // If there are some entries for which we did not get a response, error them out
        // with the error code for the range request which should reflect the highest priority error.
        for (Map.Entry<InternalReadRequest, LedgerEntryRequest> mapEntry : originalRequest.requests.entrySet()) {
            long ledgerId = mapEntry.getKey().ledgerId;
            long entryId = mapEntry.getValue().entryId;
            LOG.error("During a range read operation, we did not get a response for ledger:" + ledgerId
                    + " and entry:" + entryId + " and return code:" + rc);
            readEntryComplete(rc, ledgerId, entryId, null, mapEntry.getValue());
        }
    }

    @Override
    public void readEntryComplete(int rc, long ledgerId, final long entryId, final ChannelBuffer buffer, Object ctx) {
        final LedgerEntryRequest entry = (LedgerEntryRequest) ctx;

        lh.getStatsLogger().getSimpleStatLogger(BookkeeperClientSimpleStatType.NUM_PERMITS_TAKEN).dec();
        lh.bkSharedSem.release(BKSharedOp.READ_OP);

        // if we just read only one entry, and this entry is not existed (in recoveryRead case)
        // we don't need to do ReattemptRead, otherwise we could not handle following case:
        //
        // an empty ledger with quorum (bk1, bk2), bk2 is failed forever.
        // bk1 return NoLedgerException, client do ReattemptRead to bk2 but bk2 isn't connected
        // so the read 0 entry would failed. this ledger could never be closed.
        if (startEntryId == endEntryId) {
            if (BKException.Code.NoSuchLedgerExistsException == rc ||
                BKException.Code.NoSuchEntryException == rc) {
                submitCallback(rc);
                return;
            }
        }

        if (rc != BKException.Code.OK) {
            entry.logErrorAndReattemptRead("Error: " + BKException.getMessage(rc), rc);
            return;
        }

        if (entry.complete(buffer)) {
            numPendingEntries--;
        }

        if (numPendingEntries == 0) {
            submitCallback(BKException.Code.OK);
        }

        if(numPendingEntries < 0)
            LOG.error("Read too many values");
    }

    private void submitCallback(int code) {
        long latencyMillis = MathUtils.now() - requestTimeMillis;
        if (code != BKException.Code.OK) {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientOp.READ_ENTRY)
                    .registerFailedEvent(latencyMillis);
        } else {
            lh.getStatsLogger().getOpStatsLogger(BookkeeperClientOp.READ_ENTRY)
                    .registerSuccessfulEvent(latencyMillis);
        }
        cb.readComplete(code, lh, PendingReadOp.this, PendingReadOp.this.ctx);
    }
    public boolean hasMoreElements() {
        return !seq.isEmpty();
    }

    public LedgerEntry nextElement() throws NoSuchElementException {
        return seq.remove();
    }

    public int size() {
        return seq.size();
    }
}
