/**
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
package org.apache.bookkeeper.zookeeper;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provide a mechanism to perform an operation on ZooKeeper that is safe on disconnections
 * and recoverable errors.
 */
class ZooWorker {

    static final Logger logger = LoggerFactory.getLogger(ZooWorker.class);

    int attempts = 0;
    long startTimeNanos;
    long elapsedTimeMs = 0L;
    final RetryPolicy retryPolicy;
    final OpStatsLogger statsLogger;

    ZooWorker(RetryPolicy retryPolicy, OpStatsLogger statsLogger) {
        this.retryPolicy = retryPolicy;
        this.statsLogger = statsLogger;
        this.startTimeNanos = MathUtils.nowInNano();
    }

    public boolean allowRetry(int rc) {
        elapsedTimeMs = MathUtils.elapsedMSec(startTimeNanos);
        if (!ZkUtils.isRecoverableException(rc)) {
            if (KeeperException.Code.OK.intValue() == rc) {
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
            } else {
                statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
            }
            return false;
        }
        ++attempts;
        return retryPolicy.allowRetry(attempts, elapsedTimeMs);
    }

    public long nextRetryWaitTime() {
        return retryPolicy.nextRetryWaitTime(attempts, elapsedTimeMs);
    }

    static interface ZooCallable<T> {
        /**
         * Be compatible with ZooKeeper interface.
         *
         * @return value
         * @throws InterruptedException
         * @throws KeeperException
         */
        public T call() throws InterruptedException, KeeperException;
    }

    /**
     * Execute a sync zookeeper operation with a given retry policy.
     *
     * @param client
     *          ZooKeeper client.
     * @param proc
     *          Synchronous zookeeper operation wrapped in a {@link Callable}.
     * @param retryPolicy
     *          Retry policy to execute the synchronous operation.
     * @param rateLimiter
     *          Rate limiter for zookeeper calls
     * @param statsLogger
     *          Stats Logger for zookeeper client.
     * @return result of the zookeeper operation
     * @throws KeeperException any non-recoverable exception or recoverable exception exhausted all retires.
     * @throws InterruptedException the operation is interrupted.
     */
    public static<T> T syncCallWithRetries(ZooKeeperClient client,
                                           ZooCallable<T> proc,
                                           RetryPolicy retryPolicy,
                                           RateLimiter rateLimiter,
                                           OpStatsLogger statsLogger)
    throws KeeperException, InterruptedException {
        T result = null;
        boolean isDone = false;
        int attempts = 0;
        long startTimeNanos = MathUtils.nowInNano();
        while (!isDone) {
            try {
                if (null != client) {
                    client.waitForConnection();
                }
                logger.debug("Execute {} at {} retry attempt.", proc, attempts);
                if (null != rateLimiter) {
                    rateLimiter.acquire();
                }
                result = proc.call();
                isDone = true;
                statsLogger.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startTimeNanos));
            } catch (KeeperException e) {
                ++attempts;
                boolean rethrow = true;
                long elapsedTime = MathUtils.elapsedMSec(startTimeNanos);
                if (((null != client && ZkUtils.isRecoverableException(e)) || null == client) &&
                        retryPolicy.allowRetry(attempts, elapsedTime)) {
                    rethrow = false;
                }
                if (rethrow) {
                    statsLogger.registerFailedEvent(MathUtils.elapsedMicroSec(startTimeNanos));
                    logger.debug("Stopped executing {} after {} attempts.", proc, attempts);
                    throw e;
                }
                long backoffMs = retryPolicy.nextRetryWaitTime(attempts, elapsedTime);
                logger.info("Backoff retry ({}) after {} ms.", proc, backoffMs);
                TimeUnit.MILLISECONDS.sleep(backoffMs);
            }
        }
        return result;
    }

}
