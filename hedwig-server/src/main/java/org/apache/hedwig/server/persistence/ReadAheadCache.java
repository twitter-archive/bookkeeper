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
package org.apache.hedwig.server.persistence;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.server.stats.ServerStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import org.apache.bookkeeper.util.DaemonThreadFactory;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.exceptions.PubSubException.ServerNotResponsibleForTopicException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.jmx.HedwigJMXService;
import org.apache.hedwig.server.jmx.HedwigMBeanInfo;
import org.apache.hedwig.server.jmx.HedwigMBeanRegistry;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.HedwigServerSimpleStatType;
import org.apache.hedwig.util.Callback;

public class ReadAheadCache implements PersistenceManager, HedwigJMXService {
    static Logger logger = LoggerFactory.getLogger(ReadAheadCache.class);

    protected abstract class CacheRequest extends SafeRunnable {
        public CacheRequest() {
            pendingCacheRequest.incrementAndGet();
        }

        @Override
        public void safeRun() {
            pendingCacheRequest.decrementAndGet();
        }
    }

    /**
     * The underlying persistence manager that will be used for persistence and
     * scanning below the cache
     */
    protected PersistenceManagerWithRangeScan realPersistenceManager;

    /**
     * The structure for the cache
     */
    final protected Cache<CacheKey, CacheValue> cache;

    /**
     * We maintain an estimate of the current size of the cache
     */
    final protected AtomicLong presentCacheSize = new AtomicLong(0);

    /**
     * We maintain an estimate of the pending cache requests
     */
    final protected AtomicInteger pendingCacheRequest = new AtomicInteger(0);

    /**
     * One instance of a callback that we will pass to the underlying
     * persistence manager when asking it to persist messages
     */
    protected PersistCallback persistCallbackInstance = new PersistCallback();

    /**
     * Exceptions that we will use to signal error from read-ahead
     */
    protected NoSuchSeqIdException noSuchSeqIdExceptionInstance = new NoSuchSeqIdException();
    protected ReadAheadException readAheadExceptionInstance = new ReadAheadException();
    protected StubEvictedException stubEvictedInstance = new StubEvictedException();

    protected ServerConfiguration cfg;
    protected final OrderedSafeExecutor cacheWorkers;

    // Read-ahead count configuration
    final private int readAheadCount;
    final private long readAheadSizeLimit;

    // cache default parameters
    final private static int INITIAL_CAPACITY = 64;

    // Seconds to wait before cancel active tasks
    final private static int CANCEL_DELAY = 15;

    // JMX Beans
    ReadAheadCacheBean jmxCacheBean = null;

    private class EntryWeigher implements Weigher<CacheKey, CacheValue> {
        public int weigh(CacheKey cacheKey, CacheValue cacheValue)  {
            // Approximate weight for stub which will be corrected later
            return cfg.getCacheEntryOverheadBytes() + (!cacheValue.isStub()
                    ? cacheValue.getMessage().getBody().size() : 0);
        }
    }

    private class CacheValueLoader implements Callable<CacheValue> {
        final private CacheValue cacheValue;
        private boolean called = false;
        public CacheValueLoader() {
            cacheValue = new CacheValue();
        }
        public CacheValueLoader(ScanCallback cb, Object ctx) {
            cacheValue = new CacheValue();
            cacheValue.addCallback(cb, ctx);
        }
        public CacheValueLoader(Message message) {
            cacheValue = new CacheValue(message);
        }
        public CacheValue getCacheValue() {
            return cacheValue;
        }
        public boolean wasCalled() {
            return called;
        }
        @Override
        public CacheValue call() throws Exception {
            called = true;
            onCacheInsert(cacheValue);
            return cacheValue;
        }
    }

    private class CacheRemovalListener implements RemovalListener<CacheKey, CacheValue> {
        public void onRemoval(final RemovalNotification<CacheKey, CacheValue> removal) {
            CacheKey cacheKey = removal.getKey();
            CacheValue cacheValue = removal.getValue();
            if (logger.isDebugEnabled()) {
                logger.debug("Removing key {} from cache, causal : {}, isStub: {}",
                        new Object[] { cacheKey, removal.getCause().name(), cacheValue.isStub() });
            }

            if (cacheValue.isStub()) {
                enqueueWithoutFailure(cacheKey.getTopic(),
                        new ExceptionOnCacheKey(cacheKey, cacheValue, stubEvictedInstance));
            }
            onCacheRemoval(removal.getValue());
        }
    }

    private CacheValue getCacheValueIfPresent(CacheKey cacheKey) {
        CacheValue cacheValue = cache.getIfPresent(cacheKey);
        if (cacheValue != null && !cacheValue.isStub()) {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_HITS).inc();
        }
        else {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_MISS).inc();
        }
        return cacheValue;
    }

    private CacheValue getCacheValue(CacheKey cacheKey, CacheValueLoader loader) {
        CacheValue cacheValue = null;
        try {
            cacheValue = cache.get(cacheKey, loader);
        } catch (Exception exception) {
            // Should not happen
            logger.error("Exception {} thrown when looking up read-ahead cache", exception);
        }
        assert null != cacheValue : "Found null value for cache key " + cacheKey;

        if (loader.wasCalled() && logger.isDebugEnabled()) {
            logger.debug("Cache key {} inserted w/ weight {}", cacheKey, cacheValue.getCacheWeight());
        }

        if (!cacheValue.isStub() && !loader.wasCalled()) {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_HITS).inc();
        }
        else {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_MISS).inc();
        }

        return cacheValue;
    }

    /**
     * Constructor. Starts the cache worker threads
     *
     * @param realPersistenceManager
     */
    public ReadAheadCache(PersistenceManagerWithRangeScan realPersistenceManager, ServerConfiguration cfg) {
        this.realPersistenceManager = realPersistenceManager;
        this.cfg = cfg;
        int numThreads = cfg.getNumReadAheadCacheThreads();
        this.cache = CacheBuilder.newBuilder()
                .concurrencyLevel(numThreads)
                .initialCapacity(INITIAL_CAPACITY)
                .maximumWeight(cfg.getMaximumCacheSize())
                .weigher(new EntryWeigher())
                .softValues()
                .expireAfterAccess(cfg.getCacheEntryTTL(), TimeUnit.SECONDS)
                .removalListener(new CacheRemovalListener())
                .build();
        this.cacheWorkers = new OrderedSafeExecutor(numThreads, new DaemonThreadFactory());
        this.readAheadCount = Math.max(1, cfg.getReadAheadCount());
        this.readAheadSizeLimit  = cfg.getReadAheadSizeBytes();
    }

    public ReadAheadCache start() {
        return this;
    }

    private void onCacheInsert(CacheValue cacheValue) {
        if (cacheValue.isStub()) {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_STUBS).inc();
        }
        else {
            // Update the cache size
            int size = cacheValue.getCacheWeight();
            presentCacheSize.addAndGet(size);
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.CACHE_ENTRY_SIZE).add(size);
        }
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                HedwigServerSimpleStatType.NUM_CACHED_ENTRIES).inc();
    }

    private void onCacheRemoval(CacheValue cacheValue) {
        if (cacheValue.wasStub()) {
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.NUM_CACHE_STUBS).dec();
        }
        else {
            // Update the cache size
            int size = cacheValue.getCacheWeight();
            presentCacheSize.addAndGet(-size);
            ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                    HedwigServerSimpleStatType.CACHE_ENTRY_SIZE).add(-size);
        }
        ServerStatsProvider.getStatsLoggerInstance().getSimpleStatLogger(
                HedwigServerSimpleStatType.NUM_CACHED_ENTRIES).dec();
    }

    void addMessageToCache(CacheKey cacheKey, Message message) {
        CacheValueLoader loader = new CacheValueLoader(message);
        CacheValue cacheValue = getCacheValue(cacheKey, loader);
        // Sync on cache value to replace stub if not already
        synchronized (cacheValue) {
            if (cacheValue.isStub()) {
                cacheValue.setMessageAndInvokeCallbacks(message);
                // Replace cache entry to reflect new weight
                cache.put(cacheKey, loader.getCacheValue());
                onCacheInsert(loader.getCacheValue());
            }
        }
    }

    /**
     * ========================================================================
     * Methods of {@link PersistenceManager} that we will pass straight down to
     * the real persistence manager.
     */

    public long getSeqIdAfterSkipping(ByteString topic, long seqId, int skipAmount) {
        return realPersistenceManager.getSeqIdAfterSkipping(topic, seqId, skipAmount);
    }

    public MessageSeqId getCurrentSeqIdForTopic(ByteString topic) throws ServerNotResponsibleForTopicException {
        return realPersistenceManager.getCurrentSeqIdForTopic(topic);
    }

    /**
     * ========================================================================
     * Other methods of {@link PersistenceManager} that the cache needs to take
     * some action on.
     *
     * 1. Persist: We pass it through to the real persistence manager but insert
     * our callback on the return path
     *
     */
    public void persistMessage(PersistRequest request) {
        // make a new PersistRequest object so that we can insert our own
        // callback in the middle. Assign the original request as the context
        // for the callback.

        PersistRequest newRequest = new PersistRequest(request.getTopic(), request.getMessage(),
                persistCallbackInstance, request);
        realPersistenceManager.persistMessage(newRequest);
    }

    /**
     * The callback that we insert on the persist request return path. The
     * callback simply inserts the message into our cache.
     *
     */
    public class PersistCallback implements Callback<PubSubProtocol.MessageSeqId> {

        /**
         * In case there is a failure in persisting, just pass it to the
         * original callback
         */
        public void operationFailed(Object ctx, PubSubException exception) {
            PersistRequest originalRequest = (PersistRequest) ctx;
            Callback<PubSubProtocol.MessageSeqId> originalCallback = originalRequest.getCallback();
            Object originalContext = originalRequest.getCtx();
            originalCallback.operationFailed(originalContext, exception);
        }

        /**
         * When the persist finishes, we first notify the original callback of
         * success, and then opportunistically treat the message as if it just
         * came in through a scan
         */
        public void operationFinished(Object ctx, PubSubProtocol.MessageSeqId resultOfOperation) {
            PersistRequest originalRequest = (PersistRequest) ctx;

            // Lets call the original callback first so that the publisher can
            // hear success
            originalRequest.getCallback().operationFinished(originalRequest.getCtx(), resultOfOperation);

            // Original message that was persisted didn't have the local seq-id.
            // Lets add that in
            Message messageToCache = Message.newBuilder(originalRequest.getMessage()).setMsgId(resultOfOperation).build();

            // Add this newly persisted message to our cache
            CacheKey cacheKey = new CacheKey(originalRequest.getTopic(), resultOfOperation.getLocalComponent());
            addMessageToCache(cacheKey, messageToCache);
        }
    }

     protected void enqueueWithoutFailure(ByteString topic, final CacheRequest obj) {
         try {
             cacheWorkers.submitOrdered(topic, obj);
         } catch (RejectedExecutionException exception) {
             // Should not happen
             pendingCacheRequest.decrementAndGet();
             logger.error("Failed to submit cache request for topic {} : {}", topic.toStringUtf8(), exception);
         }
     }

    /**
     * Another method from {@link PersistenceManager}.
     *
     * Scan - Since the scan needs to touch the cache, we will just enqueue
     * the scan request and let the cache maintainer thread handle it.
     */
    public void scanSingleMessage(ScanRequest request) {
        enqueueWithoutFailure(request.getTopic(), new ScanRequestWrapper(request));
    }

    /**
     * Another method from {@link PersistenceManager}.
     *
     * Enqueue the request so that the cache maintainer thread can delete all
     * message-ids older than the one specified
     */
    public void deliveredUntil(ByteString topic, Long seqId) {
        realPersistenceManager.deliveredUntil(topic, seqId);
    }

    /**
     * Another method from {@link PersistenceManager}.
     *
     * Since this is a cache layer on top of an underlying persistence manager,
     * we can just call the consumedUntil method there. The messages older than
     * the latest one passed here won't be accessed anymore so they should just
     * get aged out of the cache eventually. For now, there is no need to
     * proactively remove those entries from the cache.
     */
    public void consumedUntil(ByteString topic, Long seqId) {
        realPersistenceManager.consumedUntil(topic, seqId);
    }

    public void setMessageBound(ByteString topic, Integer bound) {
        realPersistenceManager.setMessageBound(topic, bound);
    }

    public void clearMessageBound(ByteString topic) {
        realPersistenceManager.clearMessageBound(topic);
    }

    public void consumeToBound(ByteString topic) {
        realPersistenceManager.consumeToBound(topic);
    }

    /**
     * Stop method which shutdown threads
     */
    public void stop() {
        cacheWorkers.shutdown();
        cacheWorkers.forceShutdown(CANCEL_DELAY, TimeUnit.SECONDS);
    }

    /**
     * For unit test to remove the message from the cache
     *
     * @param cacheKey
     */
    void removeMessageFromCache(CacheKey cacheKey) {
        cache.invalidate(cacheKey);
    }

    void removeAllMessagesFromCache() {
        cache.invalidateAll();
    }

    protected class ExceptionOnCacheKey extends CacheRequest {
        CacheKey cacheKey;
        CacheValue cacheValue;
        Exception exception;

        public ExceptionOnCacheKey(CacheKey cacheKey, Exception exception) {
            this.cacheKey = cacheKey;
            this.exception = exception;
        }

        public ExceptionOnCacheKey(CacheKey cacheKey, CacheValue cacheValue, Exception exception) {
            this(cacheKey, exception);
            this.cacheValue = cacheValue;
        }

        /**
         * If for some reason, an outstanding read on a cache stub fails.
         * To handle this, we simply send error on the callbacks registered
         * for that stub, and delete the entry from the cache.
         */
        @Override
        public void safeRun() {
            if (null != cacheValue) {
                cacheValue.setErrorAndInvokeCallbacks(exception);
            }
            else {
                cacheValue = getCacheValueIfPresent(cacheKey);
                if (cacheValue != null && cacheValue.isStub()) {
                    cacheValue.setErrorAndInvokeCallbacks(exception);
                    removeMessageFromCache(cacheKey);
                }
            }
            super.safeRun();
        }
    }

    @SuppressWarnings("serial")
    protected static class NoSuchSeqIdException extends Exception {

        public NoSuchSeqIdException() {
            super("No such seq-id");
        }
    }

    @SuppressWarnings("serial")
    protected static class ReadAheadException extends Exception {
        public ReadAheadException() {
            super("Read ahead failed");
        }
    }

    @SuppressWarnings("serial")
    protected static class StubEvictedException extends Exception {
        public StubEvictedException() {
            super("Stub entry evicted");
        }
    }

    public class CancelScanRequestOp extends CacheRequest {

        final CancelScanRequest request;

        public CancelScanRequestOp(CancelScanRequest request) {
            this.request = request;
        }

        @Override
        public void safeRun() {
            // cancel scan request
            cancelScanRequest(request.getScanRequest());
            super.safeRun();
        }

        void cancelScanRequest(ScanRequest request) {
            if (null == request) {
                // nothing to cancel
                return;
            }

            CacheKey cacheKey = new CacheKey(request.getTopic(), request.getStartSeqId());
            CacheValue cacheValue = cache.getIfPresent(cacheKey);
            if (null == cacheValue) {
                // cache value is evicted
                // so it's callback would be called, we don't need to worry about
                // cancel it. since it was treated as executed.
                return;
            }
            cacheValue.removeCallback(request.getCallback(), request.getCtx());
        }
    }

    public void cancelScanRequest(ByteString topic, CancelScanRequest request) {
        enqueueWithoutFailure(topic, new CancelScanRequestOp(request));
    }

    protected class ScanResponse extends CacheRequest {
        CacheKey cacheKey;
        Message message;

        public ScanResponse(CacheKey cacheKey, Message message) {
            this.cacheKey = cacheKey;
            this.message = message;
        }

        @Override
        public void safeRun() {
            addMessageToCache(cacheKey, message);
            super.safeRun();
        }
    }

    protected class ScanRequestWrapper extends CacheRequest implements ScanCallback {
        ScanRequest request;            // Original request
        Set<Long> stubs;                // Installed stub entries
        long maxSeqIdScanned;           // Maximum seq-id scanned

        public ScanRequestWrapper(ScanRequest request) {
            this.request = request;
            this.stubs = new HashSet<Long>();
            this.maxSeqIdScanned = Long.MIN_VALUE;
        }

        @Override
        public void messageScanned(Object ctx, Message message) {
            long seqId = message.getMsgId().getLocalComponent();
            if (seqId > maxSeqIdScanned) {
                maxSeqIdScanned = seqId;
            }
            stubs.remove(seqId);
            CacheKey cacheKey = new CacheKey(request.getTopic(), seqId);
            enqueueWithoutFailure(cacheKey.getTopic(), new ScanResponse(cacheKey, message));
        }

        @Override
        public void scanFinished(Object ctx, ReasonForFinish reason) {
            // If the scan finished because no more messages are present, its ok
            // to leave the stubs in place because they will get filled in as
            // new publishes happen. However, if the scan finished due to some
            // other reason, e.g., read ahead size limit was reached, we want to
            // delete the stubs, so that when the time comes, we can schedule
            // another read ahead request.
            if (reason != ReasonForFinish.NO_MORE_MESSAGES) {
                failIfMessageNotFound(readAheadExceptionInstance);
            }
            else if (maxSeqIdScanned > 0) {
                failIfMessageGCed();
            }
        }

        @Override
        public void scanFailed(Object ctx, Exception exception) {
            failIfMessageNotFound(exception);
        }

        private void failIfMessageGCed() {
            for (Long seqId : stubs) {
                if (seqId < maxSeqIdScanned) {
                    CacheKey cacheKey = new CacheKey(request.getTopic(), seqId);
                    ExceptionOnCacheKey runnable = new ExceptionOnCacheKey(cacheKey, noSuchSeqIdExceptionInstance);
                    enqueueWithoutFailure(cacheKey.getTopic(), runnable);
                }
            }
        }

        private void failIfMessageNotFound(Exception exception) {
            if (maxSeqIdScanned > 0) {
                failIfMessageGCed();
            }
            for (Long seqId : stubs) {
                if (seqId > maxSeqIdScanned) {
                    CacheKey cacheKey = new CacheKey(request.getTopic(), seqId);
                    ExceptionOnCacheKey runnable = new ExceptionOnCacheKey(cacheKey, exception);
                    enqueueWithoutFailure(cacheKey.getTopic(), runnable);
                }
            }
        }

        /**
         * This method just checks if the provided seq-id already exists in the
         * cache. If not, a range read of the specified amount is issued.
         *
         * @param seqIdStart
         * @param readAheadCount
         * return range read request if read-ahead issued.
         */
        RangeScanRequest doReadAhead(long seqIdStart, int readAheadCount) {
            RangeScanRequest rangeRequest = null;
            CacheValueLoader loader;
            ByteString topic = request.getTopic();
            long seqId = seqIdStart;

            int i = 0;
            for (; i < readAheadCount; i++) {
                if (!stubs.contains(seqId)) {
                    loader = new CacheValueLoader();
                    getCacheValue(new CacheKey(topic, seqId), loader);
                    if (!loader.wasCalled()) {
                        break;
                    }
                    stubs.add(seqId);
                }
                seqId = realPersistenceManager.getSeqIdAfterSkipping(topic, seqId, 1);
            }

            // read ahead
            if (i > 0) {
                rangeRequest = new RangeScanRequest(topic, seqIdStart, i, readAheadSizeLimit, this, null);
                realPersistenceManager.scanMessages(rangeRequest);
            }
            return rangeRequest;
        }

        /**
         * To handle a scan request, we lookup cache w/ hook up, try to do read-ahead
         * (which might cause a range read to be issued to the underlying persistence
         * manager). The scan callback will be called later when the message arrives
         * as a result of the range scan issued to the underlying persistence manager.
         */
        @Override
        public void safeRun() {
            // Call cache w/ lookup hookup
            CacheKey cacheKey = new CacheKey(request.getTopic(), request.getStartSeqId());
            CacheValueLoader loader = new CacheValueLoader(request.getCallback(), request.getCtx());
            CacheValue cacheValue = getCacheValue(cacheKey, loader);

            if (!loader.wasCalled()) {
                // Add callback which will either be called right away (if not stub),
                // or taken care of by active read-ahead request handling.
                cacheValue.addCallback(request.getCallback(), request.getCtx());

                // Requested key was already there in the cache,
                // lets look a little beyond
                long seqId = realPersistenceManager.getSeqIdAfterSkipping(cacheKey.getTopic(),
                        cacheKey.getSeqId(), (readAheadCount+1)/2);
                doReadAhead(seqId, (readAheadCount+1)/2);
            }
            else
            {
                stubs.add(cacheKey.getSeqId());
                doReadAhead(cacheKey.getSeqId(), readAheadCount);
            }

            super.safeRun();
        }
    }

    /**
     * For unit test to cover read-ahead range scan.
     *
     * @param topic
     * @param seqId
     * @param readAheadCount
     * @return The range read that should be issued
     */
    RangeScanRequest doReadAhead(ByteString topic, long seqId, int readAheadCount,
                                 ScanCallback cb, Object ctx) {
        ScanRequest request = new ScanRequest(topic, seqId, cb, ctx);
        ScanRequestWrapper wrapper = new ScanRequestWrapper(request);
        return wrapper.doReadAhead(seqId, readAheadCount);
    }

    @Override
    public void registerJMX(HedwigMBeanInfo parent) {
        try {
            jmxCacheBean = new ReadAheadCacheBean(this);
            HedwigMBeanRegistry.getInstance().register(jmxCacheBean, parent);
        } catch (Exception e) {
            logger.warn("Failed to register readahead cache with JMX", e);
            jmxCacheBean = null;
        }
    }

    @Override
    public void unregisterJMX() {
        try {
            if (jmxCacheBean != null) {
                HedwigMBeanRegistry.getInstance().unregister(jmxCacheBean);
            }
        } catch (Exception e) {
            logger.warn("Failed to unregister readahead cache with JMX", e);
        }
    }
}
