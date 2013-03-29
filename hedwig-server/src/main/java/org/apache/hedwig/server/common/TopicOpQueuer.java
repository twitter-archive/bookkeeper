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
package org.apache.hedwig.server.common;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.server.persistence.BookkeeperPersistenceManager;
import org.apache.hedwig.server.stats.HedwigServerStatsLogger.HedwigServerSimpleStatType;
import org.apache.hedwig.server.stats.ServerStatsProvider;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

public class TopicOpQueuer {

    static final Logger logger = LoggerFactory.getLogger(TopicOpQueuer.class);

    /**
     * Map from topic to the queue of operations for that topic.
     */
    protected ConcurrentHashMap<ByteString, Queue<Runnable>> topic2ops =
        new ConcurrentHashMap<ByteString, Queue<Runnable>>();

    protected final OrderedSafeExecutor scheduler;

    public TopicOpQueuer(OrderedSafeExecutor scheduler) {
        this.scheduler = scheduler;
    }

    // We should not use SafeRunnable for Hedwig here
    // since SafeRunnable will caught and ignore all potention throwable
    // which is bad. Check 'TestPubSubServer' test cases for the reason.
    public interface Op extends Runnable {
    }

    /**
     * Update the persist queue size in ServerStatsProvider only for the necessary Op.
     * We do this all in one file so as not to distribute these calls to different operations.
     * @param op
     * @param increment if true, increment, else decrement
     */
    private void updatePersistQueueSize(Op op, boolean increment) {
        if (op instanceof BookkeeperPersistenceManager.ConsumeUntilOp
                || op instanceof BookkeeperPersistenceManager.PersistOp
                || op instanceof BookkeeperPersistenceManager.RangeScanOp
                || op instanceof BookkeeperPersistenceManager.UpdateLedgerOp) {
            if (increment) {
                ServerStatsProvider.getStatsLoggerInstance()
                        .getSimpleStatLogger(HedwigServerSimpleStatType.PERSIST_QUEUE).inc();
            } else {
                ServerStatsProvider.getStatsLoggerInstance()
                        .getSimpleStatLogger(HedwigServerSimpleStatType.PERSIST_QUEUE).dec();
            }
        }
    }
    public abstract class AsynchronousOp<T> implements Op {
        final public ByteString topic;
        final public Callback<T> cb;
        final public Object ctx;

        public AsynchronousOp(final ByteString topic, final Callback<T> cb, Object ctx) {
            this.topic = topic;
            this.cb = new Callback<T>() {
                @Override
                public void operationFailed(Object ctx, PubSubException exception) {
                    onOperationFailed();
                    cb.operationFailed(ctx, exception);
                    popAndRunNext(topic);
                }

                @Override
                public void operationFinished(Object ctx, T resultOfOperation) {
                    onOperationFinished();
                    cb.operationFinished(ctx, resultOfOperation);
                    popAndRunNext(topic);
                }
            };
            this.ctx = ctx;
        }

        protected void onOperationFailed() {
            // We finished running an async op. Decrement the persist queue size
            // if required
            updatePersistQueueSize(AsynchronousOp.this, false);
        }

        protected void onOperationFinished() {
            // We finished running an async op. Decrement the persist queue size
            // if required
            updatePersistQueueSize(AsynchronousOp.this, false);
        }
    }

    public abstract class TimedAsynchronousOp<T> extends AsynchronousOp<T> {
        final long enqueueTime;
        @SuppressWarnings("rawtypes")
        final Enum opType;

        @SuppressWarnings("rawtypes")
        public TimedAsynchronousOp(final ByteString topic, final Callback<T> cb, Object ctx, Enum opType) {
            super(topic, cb, ctx);
            this.enqueueTime = MathUtils.nowInNano();
            this.opType = opType;
        }

        @Override
        protected void onOperationFailed() {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(opType)
                    .registerFailedEvent(MathUtils.elapsedMSec(enqueueTime));
            super.onOperationFailed();
        }

        @Override
        protected void onOperationFinished() {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(opType)
                    .registerSuccessfulEvent(MathUtils.elapsedMSec(enqueueTime));
            super.onOperationFinished();
        }
    }

    public abstract class SynchronousOp implements Op {
        final public ByteString topic;

        public SynchronousOp(ByteString topic) {
            this.topic = topic;
        }

        @Override
        public final void run() {
            runInternal();
            onOperationFinished();
            popAndRunNext(topic);
        }

        protected abstract void runInternal();

        protected void onOperationFinished() {
            // We finished running a sync op. Decrement the persist queue size if
            // required.
            updatePersistQueueSize(SynchronousOp.this, false);
        }
    }

    public abstract class TimedSynchronousOp extends SynchronousOp {
        final long enqueueTime;
        @SuppressWarnings("rawtypes")
        final Enum opType;

        @SuppressWarnings("rawtypes")
        public TimedSynchronousOp(ByteString topic, Enum opType) {
            super(topic);
            this.enqueueTime = MathUtils.nowInNano();
            this.opType = opType;
        }

        @Override
        protected void onOperationFinished() {
            ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(opType)
                    .registerSuccessfulEvent(MathUtils.elapsedMSec(enqueueTime));
            super.onOperationFinished();
        }
    }

    protected void popAndRunNext(ByteString topic) {
        // since we used concurrent hash map, we could
        // get the queue without synchronized whole queue map
        Queue<Runnable> ops = topic2ops.get(topic);
        if (null == ops) {
            logger.error("No queuer found for topic {} to pop and run.", topic.toStringUtf8());
            return;
        }
        synchronized (ops) {
            if (!ops.isEmpty())
                ops.remove();
            if (!ops.isEmpty()) {
                scheduler.unsafeSubmitOrdered(topic, ops.peek());
            } else {
                // it's unsafe to remove topic queue here
                // since some other threads may already get ops
                // queue instance, remove it may cause some problems
                // TODO: need to think a better solution for it.
                // topic2ops.remove(topic, ops);
            }
        }
    }

    public void pushAndMaybeRun(ByteString topic, Op op) {
        int size;
        Queue<Runnable> ops = topic2ops.get(topic);
        if (null == ops) {
            Queue<Runnable> newOps = new LinkedList<Runnable>();
            Queue<Runnable> oldOps = topic2ops.putIfAbsent(topic, newOps);
            if (null == oldOps) {
                // no queue associated with the topic
                ops = newOps;
            } else {
                // someone already create a queue
                ops = oldOps;
            }
        }
        synchronized (ops) {
            ops.add(op);
            // increment = true
            updatePersistQueueSize(op, true);
            size = ops.size();
        }
        if (size == 1) {
            op.run();
        }
    }

}
