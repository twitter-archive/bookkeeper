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

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This is a semaphore shared by the read and add operations in the bookkeeper client.
 * There are two throttling thresholds. The first one bounds the total number of permits available
 * and the second bounds the total number of permits available to read operations.
 *
 * If the number of add operations is below the add threshold, it will acquire a permit from addSem.
 * If not, it will go over to the read pool. While releasing an add operation semaphore, it will
 * release a read pool semaphore if it has acquired one (if outstandingAdd is greater than the add
 * threshold)
 */
public class BookKeeperSharedSemaphore {
    // The Semaphore used for bounding read Ops.
    private final Semaphore readSem;

    // The Semaphore that add operations should consume from by default
    // If no permits are available here, we acquire permits from the read pool.
    private final Semaphore addSem;
    private final int addThreshold;
    private final AtomicInteger readPoolAdd;

    public static enum BKSharedOp {
        READ_OP, ADD_OP
    }

    public BookKeeperSharedSemaphore(int totalPermits, int readPermits) {
        this.readSem = new Semaphore(readPermits);
        this.addSem = new Semaphore(totalPermits - readPermits);
        this.addThreshold = totalPermits - readPermits;
        this.readPoolAdd = new AtomicInteger(0);
    }

    public void acquire(BKSharedOp op) throws InterruptedException {
        switch (op) {
            case READ_OP:
                // If this is a read operation, we can only acquire a read semaphore
                readSem.acquire();
                break;
            case ADD_OP:
                // Check if we can acquire from addSem. If not, acquire from the read pool.
                if (!addSem.tryAcquire()) {
                    readSem.acquire();
                    readPoolAdd.incrementAndGet();
                }
                break;
            default:
                throw new RuntimeException("BookKeeperSharedSemaphore does not support operation:" + op.name());
        }
    }

    public void release(BKSharedOp op) {
        switch (op) {
            case READ_OP:
                readSem.release();
                break;
            case ADD_OP:
                // If we've acquired a read pool permit for an add operation, release it.
                int readsTaken;
                while ((readsTaken = readPoolAdd.get()) > 0) {
                    if (readPoolAdd.compareAndSet(readsTaken, readsTaken - 1)) {
                        // We had acquired a permit from the read pool and we're releasing it.
                        readSem.release();
                        break;
                    }
                }
                // readsTaken will be 0 if we haven't taken a read pool permit
                if (readsTaken == 0) {
                    addSem.release();
                }
                break;
            default:
                throw new RuntimeException("BookKeeperSharedSemaphore does not support operation:" + op.name());
        }
    }

    public int availablePermits() {
        return this.addSem.availablePermits() + this.readSem.availablePermits();
    }
}
