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
package org.apache.hedwig.server.netty;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.hedwig.server.common.ServerConfiguration;

class OrderedSafeExecutorFactory {

    static enum ExecutorType {
        TOPICMANAGER, SUBSCRIPTIONMANAGER, PERSISTENCEMANAGER, REGIONMANAGER
    }

    final boolean sharedExecutorEnabled;
    final ConcurrentMap<ExecutorType, OrderedSafeExecutor> executors;

    OrderedSafeExecutorFactory(ServerConfiguration conf) {
        this.sharedExecutorEnabled = conf.isSharedExecutorEnabled();
        executors = new ConcurrentHashMap<ExecutorType, OrderedSafeExecutor>();
        if (sharedExecutorEnabled) {
            OrderedSafeExecutor executor = new OrderedSafeExecutor(conf.getNumSharedQueuerThreads());
            for (ExecutorType type : ExecutorType.values()) {
                executors.put(type, executor);
            }
        }
    }

    /**
     * Get the executor used by given <i>type</i>. If there is no executor existed,
     * create a new executor with given <i>numThreads</i> threads and cache it for
     * future usage.
     *
     * @param type
     *          Executor type.
     * @param numThreads
     *          Num threads
     * @return executor for given <i>type</i>.
     */
    OrderedSafeExecutor getExecutor(ExecutorType type, int numThreads) {
        OrderedSafeExecutor executor = executors.get(type);
        if (null == executor) {
            executor = new OrderedSafeExecutor(numThreads);
            OrderedSafeExecutor oldExecutor = executors.putIfAbsent(type, executor);
            if (null != oldExecutor) {
                executor = oldExecutor;
            }
        }
        return executor;
    }

    void shutdown() {
        for (OrderedSafeExecutor executor : executors.values()) {
            executor.shutdown();
        }
    }
}
