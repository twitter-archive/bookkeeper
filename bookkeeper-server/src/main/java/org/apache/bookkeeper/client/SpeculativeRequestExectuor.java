package org.apache.bookkeeper.client;

import com.google.common.util.concurrent.ListenableFuture;

public interface SpeculativeRequestExectuor {

    /**
     * Issues a speculative request and indicates if more speculative
     * requests should be issued
     *
     * @return whether more speculative requests should be issued
     */
    ListenableFuture<Boolean> issueSpeculativeRequest();
}
