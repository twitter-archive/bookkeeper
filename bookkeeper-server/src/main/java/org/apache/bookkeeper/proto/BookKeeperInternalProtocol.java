package org.apache.bookkeeper.proto;

import org.jboss.netty.buffer.ChannelBuffer;

import java.util.ArrayList;
import java.util.List;

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
    }

    public static class InternalReadResponse {
        public int returnCode;
        public long ledgerId;
        public long entryId;
        public ChannelBuffer responseBody;
        public InternalReadResponse(int rc, long ledgerId, long entryId, ChannelBuffer responseBody) {
            this.returnCode = rc;
            this.ledgerId = ledgerId;
            this.entryId = entryId;
            this.responseBody = responseBody;
        }
    }

    public static class InternalRangeReadRequest {
        // Just for consistency. Could use requests.size()
        public int numRequests;
        // ledgerIds in the InternalReadRequests will be equal to this value.
        // TODO: Clean this to remove the redundancy if needed.
        public long ledgerId;
        public List<InternalReadRequest> requests;
        public InternalRangeReadRequest() {
            this.numRequests = 0;
            this.requests = new ArrayList<InternalReadRequest>();
        }
    }

    public static class InternalRangeReadResponse {
        // Just for consistency. Could use responses.size()
        public int numResponses;
        public int returnCode;
        public List<InternalReadResponse> responses = new ArrayList<InternalReadResponse>();
    }
}
