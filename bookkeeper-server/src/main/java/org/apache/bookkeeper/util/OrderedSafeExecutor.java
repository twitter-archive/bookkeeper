package org.apache.bookkeeper.util;

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

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ScheduledExecutorService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

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
    final ThreadPoolExecutor threads[];
    final BlockingQueue<Runnable> queues[];
    final Random rand = new Random();
    final OpStatsLogger taskExecutionStats;
    final boolean traceTaskExecution;

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {

        private String name = "OrderedSafeExecutor";
        private int numThreads = Runtime.getRuntime().availableProcessors();
        private ThreadFactory threadFactory = null;
        private StatsLogger statsLogger = NullStatsLogger.INSTANCE;
        private boolean traceTaskExecution = false;

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

        public OrderedSafeExecutor build() {
            if (null == threadFactory) {
                threadFactory = Executors.defaultThreadFactory();
            }
            return new OrderedSafeExecutor(name, numThreads, threadFactory,
                                           statsLogger, traceTaskExecution);
        }

    }

    private class TimedRunnable extends SafeRunnable {

        SafeRunnable runnable;

        TimedRunnable(SafeRunnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void safeRun() {
            long startNanos = MathUtils.nowInNano();
            this.runnable.safeRun();
            taskExecutionStats.registerSuccessfulEvent(MathUtils.elapsedMicroSec(startNanos));
        }
    }

    public OrderedSafeExecutor(int numThreads) {
        this(numThreads, Executors.defaultThreadFactory());
    }

    public OrderedSafeExecutor(int numThreads, ThreadFactory threadFactory) {
        this("OrderedSafeExecutor", numThreads, threadFactory, NullStatsLogger.INSTANCE, false);
    }

    private OrderedSafeExecutor(String name, int numThreads, ThreadFactory threadFactory,
                                StatsLogger statsLogger, boolean traceTaskExecution) {
        if (numThreads <= 0) {
            throw new IllegalArgumentException();
        }
        this.name = name;
        threads = new ThreadPoolExecutor[numThreads];
        queues = new BlockingQueue[numThreads];
        for (int i = 0; i < numThreads; i++) {
            queues[i] = new LinkedBlockingQueue<Runnable>();
            threads[i] =  new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS, queues[i],
                    new ThreadFactoryBuilder()
                        .setNameFormat(name + "-orderedsafeexecutor-" + i + "-%d")
                        .setThreadFactory(threadFactory)
                        .build());
            final int idx = i;
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
                    return threads[idx].getCompletedTaskCount();
                }
            });
            statsLogger.registerGauge(String.format("%s-total-tasks-%d", name, i), new Gauge<Number>() {
                @Override
                public Number getDefaultValue() {
                    return 0;
                }

                @Override
                public Number getSample() {
                    return threads[idx].getTaskCount();
                }
            });
        }
        // stats
        this.taskExecutionStats = statsLogger.scope(name).getOpStatsLogger("task_execution");
        this.traceTaskExecution = traceTaskExecution;
    }

    public ExecutorService chooseThread() {
        // skip random # generation in this special case
        if (threads.length == 1) {
            return threads[0];
        }

        return threads[rand.nextInt(threads.length)];

    }

    public ExecutorService chooseThread(Object orderingKey) {
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
            executor.submitOrdered(orderingKey, new SafeRunnable() {
                    @Override
                    public void safeRun() {
                        safeOperationComplete(rc, result);
                    }
                });
        }

        public abstract void safeOperationComplete(int rc, T result);
    }
}
