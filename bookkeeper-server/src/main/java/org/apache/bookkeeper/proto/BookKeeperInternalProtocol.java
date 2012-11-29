package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.client.PendingReadOp;
import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Internal representation of the protobuf generated objects.
 * TODO: As we refactor more code, move all callbacks to use these objects rather than explicit variables
 */
public class BookKeeperInternalProtocol {
    public static class InternalReadRequest {
        public long ledgerId;
        public long entryId;
        public InternalReadRequest() {
            this.ledgerId = -1;
            this.entryId = -1;
        }
        public InternalReadRequest(long ledgerId, long entryId) {
            this.ledgerId = ledgerId;
            this.entryId = entryId;
        }

        @Override
        public boolean equals(Object _that) {
            if (!(_that instanceof InternalReadRequest)) {
                return false;
            }
            InternalReadRequest that = (InternalReadRequest)_that;
            return this.ledgerId == that.ledgerId && this.entryId == that.entryId;
        }

        @Override
        public int hashCode() {
            return (int)this.ledgerId << 16 + (int)this.entryId;
        }
    }

    public static class InternalReadResponse {
        public int returnCode;
        public long ledgerId = -1;
        public long entryId = -1;
        public ChannelBuffer responseBody;
        public InternalReadResponse(int rc, long ledgerId, long entryId, ChannelBuffer responseBody) {
            this.returnCode = rc;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.responseBody = responseBody;
        }

        @Override
        public int hashCode() {
            return (int)this.ledgerId << 16 + (int)this.entryId;
        }
    }

    public static class InternalRangeReadRequest {
        public Map<InternalReadRequest, PendingReadOp.LedgerEntryRequest>
                requests;
        // Just for consistency. Could use requests.size()
        public int numRequests;
        // ledgerIds in the InternalReadRequests will be equal to this value.
        // TODO: Clean this to remove the redundancy if needed.
        public long ledgerId;
        public InternalRangeReadRequest() {
            this.numRequests = 0;
            this.requests = new HashMap<InternalReadRequest, PendingReadOp.LedgerEntryRequest>();
        }
    }

    public static class InternalRangeReadResponse {
        // Just for consistency. Could use responses.size()
        public int numResponses;
        public int returnCode;
        public List<InternalReadResponse> responses = new ArrayList<InternalReadResponse>();
    }
}
