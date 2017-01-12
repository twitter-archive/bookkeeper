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
package org.apache.bookkeeper.util;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class provides 2 things over the java {@link ScheduledExecutorService}.
 *
 * 1. It takes {@link SafeRunnable objects} instead of plain Runnable objects.
 * This means that exceptions in scheduled tasks wont go unnoticed and will be
 * logged.
 *
 * 2. It supports submitting tasks with an ordering key, so that tasks submitted
 * with the same key will always be executed in order, but tasks across
 * different keys can be unordered. This retains parallelism while retaining the
 * basic amount of ordering we want (e.g. , per ledger handle). Ordering is
 * achieved by hashing the key objects to threads by their {@link #hashCode()}
 * method.
 *
 */
public class OrderedSafeExecutor {
    final String name;
    final ListeningExecutorService threads[];
    final long threadIds[];
    final BlockingQueue<Runnable> queues[];
    final Random rand = new Random();
    final OpStatsLogger taskExecutionStats;
    final OpStatsLogger taskPendingStats;
    final boolean traceTaskExecution;
    final long warnTimeMicroSec;

    final static long SECOND_MICROS = TimeUnit.SECONDS.toMicros(1);

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String name = "OrderedSafeExecutor";
        private int numThreads = Runtime.getRuntime().availableProcessors();
        private ThreadFactory threadFactory = null;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        private boolean traceTaskExecution = false;
        private long warnTimeMicroSec = SECOND_MICROS;

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder numThreads(int num) {
            this.numThreads = num;
            return this;
        }

        public Builder threadFactory(ThreadFactory threadFactory) {
            this.threadFactory = threadFactory;
            return this;
        }

        public Builder statsLogger(StatsLogger statsLogger) {
            this.statsLogger = statsLogger;
            return this;
        }

        public Builder traceTaskExecution(boolean enabled) {
            this.traceTaskExecution = enabled;
            return this;
        }

        public Builder traceTaskWarnTimeMicroSec(long warnTimeMicroSec) {
            this.warnTimeMicroSec = warnTimeMicroSec;
            return this;
        }

        public OrderedSafeExecutor build() {
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }
            return new OrderedSafeExecutor(name, numThreads, threadFactory,
                                           statsLogger, traceTaskExecution,
                                           warnTimeMicroSec);
        }

    }

    private class TimedRunnable extends SafeRunnable {

        final SafeRunnable runnable;
        final long initNanos;

        TimedRunnable(SafeRunnable runnable) {
            this.runnable = runnable;
            this.initNanos = MathUtils.nowInNano();
        }

        @Override
        public void safeRun() {
            long startNanos = MathUtils.nowInNano();
            taskPendingStats.registerSuccessfulEvent(MathUtils.elapsedMicroSec(initNanos));
            this.runnable.safeRun();
            long elapsedMicroSec = MathUtils.elapsedMicroSec(startNanos);
            taskExecutionStats.registerSuccessfulEvent(elapsedMicroSec);
            if (elapsedMicroSec >= warnTimeMicroSec) {
                logger.warn("Runnable {}:{} took too long {} micros to execute.",
                            new Object[] { runnable, runnable.getClass(), elapsedMicroSec });
            }
        }
    }

    public OrderedSafeExecutor(int numThreads) {
        this(numThreads, Executors.defaultThreadFactory());
    }

    public OrderedSafeExecutor(int numThreads, ThreadFactory threadFactory) {
        this("OrderedSafeExecutor", numThreads, threadFactory, NullStatsLogger.INSTANCE, false,
             SECOND_MICROS);
    }

    private OrderedSafeExecutor(String name, int numThreads, ThreadFactory threadFactory,
                                StatsLogger statsLogger, boolean traceTaskExecution,
                                long warnTimeMicroSec) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException();
        }
        this.warnTimeMicroSec = warnTimeMicroSec;
        this.name = name;
        threads = new ListeningExecutorService[numThreads];
        threadIds = new long[numThreads];
        queues = new BlockingQueue[numThreads];
        for (int i = 0; i < numThreads; i++) {
            queues[i] = new LinkedBlockingQueue<Runnable>();
            final ThreadPoolExecutor thread =  new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS, queues[i],
                    new ThreadFactoryBuilder()
                        .setNameFormat(name + "-orderedsafeexecutor-" + i + "-%d")
                        .setThreadFactory(threadFactory)
                        .build());
            threads[i] = MoreExecutors.listeningDecorator(thread);
            final int idx = i;
            try {
                threads[i].submit(new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        threadIds[idx] = Thread.currentThread().getId();
                    }
                }).get();
            } catch (InterruptedException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            } catch (ExecutionException e) {
                throw new RuntimeException("Couldn't start thread " + i, e);
            }
            statsLogger.registerGauge(String.format("%s-queue-%d", name, i), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return queues[idx].size();
                }
            });
            statsLogger.registerGauge(String.format("%s-completed-tasks-%d", name, i), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getCompletedTaskCount();
                }
            });
            statsLogger.registerGauge(String.format("%s-total-tasks-%d", name, i), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return thread.getTaskCount();
                }
            });
        }
        // stats
        statsLogger.registerGauge(String.format("%s-total-queue", name), new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                int totalSize = 0;
                for (BlockingQueue<Runnable> queue : queues) {
                    totalSize += queue.size();
                }
                return totalSize;
            }
        });
        this.taskExecutionStats = statsLogger.scope(name).getOpStatsLogger("task_execution");
        this.taskPendingStats = statsLogger.scope(name).getOpStatsLogger("task_queued");
        this.traceTaskExecution = traceTaskExecution;
    }

    public ListeningExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];

    }

    public ListeningExecutorService chooseThread(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[MathUtils.signSafeMod(orderingKey.hashCode(), threads.length)];

    }

    private SafeRunnable timedRunnable(SafeRunnable r) {
        if (traceTaskExecution) {
            return new TimedRunnable(r);
        } else {
            return r;
        }
    }

    /**
     * schedules a one time action to execute
     */
    public void submit(SafeRunnable r) {
        chooseThread().submit(timedRunnable(r));
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public void submitOrdered(Object orderingKey, SafeRunnable r) {
        chooseThread(orderingKey).submit(timedRunnable(r));
    }

    /**
     * schedules a one time action to execute with an ordering guarantee on the key
     * @param orderingKey
     * @param r
     */
    public <T> ListenableFuture<T> submitOrdered(Object orderingKey, java.util.concurrent.Callable<T> callable) {
        return chooseThread(orderingKey).submit(callable);
    }


    private long getThreadID(Object orderingKey) {
        // skip hashcode generation in this special case
        if (threadIds.length == 1) {
            return threadIds[0];
        }

        return threadIds[MathUtils.signSafeMod(orderingKey.hashCode(), threadIds.length)];
    }

    public void shutdown() {
        for (int i = 0; i < threads.length; i++) {
            threads[i].shutdown();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean ret = true;
        for (int i = 0; i < threads.length; i++) {
            ret = ret && threads[i].awaitTermination(timeout, unit);
        }
        return ret;
    }

    /**
     * Force threads shutdown (cancel active requests) after specified delay,
     * to be used after shutdown() rejects new requests.
     */
    public void forceShutdown(long timeout, TimeUnit unit) {
        for (int i = 0; i < threads.length; i++) {
            try {
                if (!threads[i].awaitTermination(timeout, unit)) {
                    threads[i].shutdownNow();
                }
            }
            catch (InterruptedException exception) {
                threads[i].shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Generic callback implementation which will run the
     * callback in the thread which matches the ordering key
     */
    public static abstract class OrderedSafeGenericCallback<T>
            implements GenericCallback<T> {
        private static final Logger LOG = LoggerFactory.getLogger(OrderedSafeGenericCallback.class);

        private final OrderedSafeExecutor executor;
        private final Object orderingKey;

        /**
         * @param executor The executor on which to run the callback
         * @param orderingKey Key used to decide which thread the callback
         *                    should run on.
         */
        public OrderedSafeGenericCallback(OrderedSafeExecutor executor, Object orderingKey) {
            this.executor = executor;
            this.orderingKey = orderingKey;
        }

        @Override
        public final void operationComplete(final int rc, final T result) {
            // during closing, callbacks that are error out might try to submit to
            // the scheduler again. if the submission will go to same thread, we
            // don't need to submit to executor again. this is also an optimization for
            // callback submission
            if (Thread.currentThread().getId() == executor.getThreadID(orderingKey)) {
                safeOperationComplete(rc, result);
            } else {
                try {
                    executor.submitOrdered(orderingKey, new SafeRunnable() {
                            @Override
                            public void safeRun() {
                                safeOperationComplete(rc, result);
                            }
                            @Override
                            public String toString() {
                                return String.format("Callback(key=%s, name=%s)",
                                                     orderingKey,
                                                     OrderedSafeGenericCallback.this);
                            }
                        });
                } catch (RejectedExecutionException re) {
                    LOG.warn("Failed to submit callback for {} : ", orderingKey, re);
                }
            }
        }

        public abstract void safeOperationComplete(int rc, T result);
    }
}
