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
package org.apache.bookkeeper.client;

import junit.framework.TestCase;
import org.junit.Test;

import org.apache.bookkeeper.util.BookKeeperSharedSemaphore;
import org.apache.bookkeeper.util.BookKeeperSharedSemaphore.BKSharedOp;

public class BookKeeperSharedSemaphoreTest extends TestCase {

    public final int totalReadPermits = 8;
    public final int totalPermits = 10;
    /**
     * Returns a thread that interrupts the thread passed as a parameter after the timeout in milliseconds
     * @return
     */
    private Thread getInterruptingThread(final Thread threadToInterrupt, final long timeoutMillis) {
        Thread threadToReturn = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(timeoutMillis);
                } catch (InterruptedException e) {
                    // If this thread was interrupted before it got a chance to interrupt the target
                    // thread, we just return.
                    return;
                }
                threadToInterrupt.interrupt();
            }
        });
        threadToReturn.setDaemon(true);
        return threadToReturn;
    }

    private void release(BookKeeperSharedSemaphore sem, BKSharedOp op, int numToRelease) {
        while(numToRelease-- > 0) {
            sem.release(op);
        }
    }

    private void acquire(BookKeeperSharedSemaphore sem, BKSharedOp op, int numToAcquire) throws InterruptedException {
        while(numToAcquire-- > 0) {
            sem.acquire(op);
        }
    }

    private void acquireWithTimeout(BookKeeperSharedSemaphore sem, BKSharedOp op, int numToAcquire) throws InterruptedException {
        Thread t = getInterruptingThread(Thread.currentThread(), 2000);
        t.start();
        acquire(sem, op, numToAcquire);
        t.interrupt();
    }

    @Test
    public void testAllReadAndWrite() {
        BookKeeperSharedSemaphore sem = new BookKeeperSharedSemaphore(totalPermits, totalReadPermits);
        // We should be able to acquire all read permits.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, totalReadPermits);
        } catch (InterruptedException e) {
            fail("Couldn't acquire all read permits");
        }

        // Acquiring any more read permits should fail.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, 1);
            // Should not reach here
            fail("Acquired a read permit when we shouldn't have");
        } catch (InterruptedException e) {
            // This is expected.
        }

        // We should be able to acquire totalPermits - totalReadPermits number of add permits
        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, totalPermits - totalReadPermits);
        } catch (InterruptedException e) {
            fail("Could not acquire permits for add operations");
        }

        // No more permits should be available now.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, 1);
            // Should not reach here
            fail("Acquired read permit when we shouldn't have.");
        } catch (InterruptedException e) {
            // This is okay
        }
        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, 1);
            // Should not reach here
            fail("Acquired read permit when we shouldn't have.");
        } catch (InterruptedException e) {
            // This is expected.
        }
    }

    @Test
    public void testReadWriteInteraction() {
        BookKeeperSharedSemaphore sem = new BookKeeperSharedSemaphore(totalPermits, totalReadPermits);
        // Add ops should be able to acquire all permits
        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, totalPermits);
        } catch (InterruptedException e) {
            fail("Couldn't acquire all add permits");
        }

        // No read permits should be available now.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, totalPermits);
            // Should not reach here
            fail("Acquired a read permit when we shouldn't have.");
        } catch (InterruptedException e) {
            // This is expected.
        }

        // Release one add permit and we should now be able to get a read permit.
        release(sem, BKSharedOp.ADD_OP, 1);

        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, 1);
        } catch (InterruptedException e) {
            fail("Couldn't acquire read permit after releasing a write permit.");
        }
    }

    public void testNoWritePermits() {
        // A semaphore where the number of exclusive add permits is 0.
        BookKeeperSharedSemaphore sem = new BookKeeperSharedSemaphore(totalPermits, totalPermits);

        // Once we acquire all read permits, we should not be able to acquire an add permit
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, totalPermits);
        } catch (InterruptedException e) {
            fail("Couldn't acquire read permits.");
        }

        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, 1);
            // Should not reach here.
            fail("Acquired an add permit but we should not have.");
        } catch (InterruptedException e) {
            // This is expected.
        }

        release(sem, BKSharedOp.READ_OP, totalPermits);

        // Acquire all add permits.
        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, totalPermits);
        } catch (InterruptedException e) {
            fail("Couldn't acquire all add permits");
        }

        // No read permits should be available.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, 1);
            fail("Acquired a read permit but all permits were already taken.");
        } catch (InterruptedException e) {
            // This is expected.
        }
    }

    public void testNoReadPermits() {
        // A semaphore with no read permits
        BookKeeperSharedSemaphore sem = new BookKeeperSharedSemaphore(totalPermits, 0);
        // We should not be able to acquire any read permits.
        try {
            acquireWithTimeout(sem, BKSharedOp.READ_OP, 1);
            // Should not reach here.
            fail("Acquired a read permit when the value of allowed permits is 0");
        } catch (InterruptedException e) {
            // This is expected.
        }

        // We should be able to get all add permits.
        try {
            acquireWithTimeout(sem, BKSharedOp.ADD_OP, totalPermits);
        } catch (InterruptedException e) {
            fail("Couldn't acquire all add permits.");
        }
    }
}
