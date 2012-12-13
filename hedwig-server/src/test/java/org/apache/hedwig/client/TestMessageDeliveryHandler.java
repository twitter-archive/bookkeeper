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
package org.apache.hedwig.client;

import com.google.protobuf.ByteString;
import junit.framework.TestCase;
import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.data.MessageConsumeData;
import org.apache.hedwig.client.data.PubSubData;
import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.client.handlers.MessageConsumeCallback;
import org.apache.hedwig.client.handlers.MessageDeliveryHandler;
import org.apache.hedwig.client.handlers.MessageDeliveryHandler.MessageDeliveryException;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.apache.hedwig.util.Callback;
import org.junit.Test;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestMessageDeliveryHandler extends TestCase {

    private abstract static class MockMessageHandler implements MessageHandler {
        public volatile int nextExpected;
        public MockMessageHandler(int start) {
            nextExpected = start;
        }
        public abstract void deliver(ByteString byteString, ByteString byteString1, Message message, Callback<Void> voidCallback, Object o);
        public int getNextExpected() {
            return nextExpected;
        }
    }

    private PubSubData getSubData(int n) {
        return new PubSubData(getTopic(n), getMessage(n), getSubscriber(n),
                OperationType.SUBSCRIBE, null, null, null);
    }

    private MessageConsumeData getMessageData(int n, MessageConsumeCallback cb) {
        return new MessageConsumeData(new TopicSubscriber(getTopic(n), getSubscriber(n)),
                                      getMessage(n), cb);
    }

    private Message getMessage(int n) {
        return Message.newBuilder().setBody(ByteString.copyFromUtf8("Message-" + n)).build();
    }

    private ByteString getTopic(int n) {
        return ByteString.copyFromUtf8("Topic-" + n);
    }

    private ByteString getSubscriber(int n) {
        return ByteString.copyFromUtf8("Subscriber-" + n);
    }

    /**
     * Get a handler that will store the number of messages delivered to it
     * and also that they are in order. It should get messages starting with Message-(start)
     * till Message-(start+n-1) both inclusive. n is some number.
     * @return
     */
    private MockMessageHandler getMessageHandler(final int start) {
        return new MockMessageHandler(start) {
            @Override
            public void deliver(ByteString byteString, ByteString byteString1, Message message, Callback<Void> voidCallback, Object o) {
                int curNum = Integer.parseInt(message.getBody().toStringUtf8().split("-")[1]);
                if (curNum != nextExpected) {
                    throw new RuntimeException("Message not delivered in order");
                }
                nextExpected++;
            }
        };
    }
    @Test
    public void testExceptions() throws Exception {
        // deliver method throws a runtime exception
        MessageDeliveryHandler handler = new MessageDeliveryHandler();
        handler.setOrigSubData(getSubData(0));
        handler.setMessageHandlerOptionallyDeliver(new MessageHandler() {
            @Override
            public void deliver(ByteString byteString, ByteString byteString1, Message message, Callback<Void> voidCallback, Object o) {
                throw new RuntimeException("Test Exception");
            }
        });
        try {
            handler.offerAndOptionallyDeliver(getMessageData(0, null));
            fail("Did not catch thrown runtime exception.");
        } catch (MessageDeliveryException e) {
            // Should reach here.
        }
    }

    @Test
    public void testDeliveryWithHandlerSet() throws Exception {
        // Set the handler before invoking delivery
        int numMessages = 10;
        MockMessageHandler mHandler = getMessageHandler(0);
        MessageDeliveryHandler deliveryHandler = new MessageDeliveryHandler();
        deliveryHandler.setOrigSubData(getSubData(0));
        try {
            assertTrue("Delivered messages when it shouldn't have", deliveryHandler.setMessageHandlerOptionallyDeliver(mHandler) == 0);
        } catch (MessageDeliveryException e) {
            fail("Exception while setting message handler.");
        }
        for (int i = 0; i < numMessages; i++) {
            try {
                assertTrue("Did not deliver only one message on each invocation", deliveryHandler.offerAndOptionallyDeliver(
                        getMessageData(i, null)) == 1);
            } catch (MessageDeliveryException e) {
                fail("Caught exception while trying to deliver messages.");
            }
        }
        // Make sure we got all messages delivered.
        assertTrue("Did not deliver all messages.", mHandler.getNextExpected() == numMessages);
    }

    @Test
    public void testDeliveryWithoutHandler() throws Exception {
        int numMessages = 10;
        MockMessageHandler mHandler = getMessageHandler(0);
        MessageDeliveryHandler deliveryHandler = new MessageDeliveryHandler();
        deliveryHandler.setOrigSubData(getSubData(0));
        for (int i = 0; i < numMessages; i++) {
            try {
                assertTrue("Delivered a message although a handler is not set.", deliveryHandler.offerAndOptionallyDeliver(
                        getMessageData(i, null)) == 0);
            } catch (MessageDeliveryException e) {
                fail("Caught exception while trying to deliver messages.");
            }
        }
        // Now setting the message handler should deliver messages.
        try {
            assertTrue("Did not deliver all messages.",
                    deliveryHandler.setMessageHandlerOptionallyDeliver(mHandler) == numMessages);
        } catch (MessageDeliveryException e) {
            fail("Exception while setting message handler.");
        }
    }

    /**
     * First enqueue messages without setting message handler and then make sure that the thread that
     * sets the message handler delivers messages.
     * @throws Exception
     */
    @Test
    public void testDeliveryWithGaps() throws Exception {
        final AtomicReference<Thread> executorThread = new AtomicReference<Thread>();
        final MessageDeliveryHandler deliveryHandler = new MessageDeliveryHandler();
        deliveryHandler.setOrigSubData(getSubData(0));
        final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
        final SynchronousQueue<Boolean> statusQueue = new SynchronousQueue<Boolean>();

        final MockMessageHandler mHandler = new MockMessageHandler(0) {
            MockMessageHandler originalHandler = getMessageHandler(0);
            @Override
            public void deliver(ByteString byteString, ByteString byteString1, Message message, Callback<Void> voidCallback, Object o) {
                if (executorThread.get() != null && !Thread.currentThread().equals(executorThread.get())) {
                    throw new RuntimeException("The expected thread is not delivering messages.");
                }
                System.out.println("Thread : " + Thread.currentThread().getName());
                originalHandler.deliver(byteString, byteString1, message, voidCallback, o);
            }

            @Override
            public int getNextExpected() {
                return originalHandler.getNextExpected();
            }
        };

        final Thread t1, t2;
        t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    assertTrue(queue.poll(10000, TimeUnit.MILLISECONDS));
                    executorThread.set(Thread.currentThread());
                    try {
                        assertTrue("Set message handler thread did not deliver all messages",
                                deliveryHandler.setMessageHandlerOptionallyDeliver(mHandler) == 5);
                    } catch (MessageDeliveryException e) {
                        fail("Exception while setting message handler and delivering.");
                    }
                    // Notify the other thread thread so that it can start delivery again.
                    queue.put(true);
                    // Every 50 ms, 10 times, change the message handler from null to mHandler and back.
                    // Change to null only if shouldNull%3 == 0 except for the last run of the loop.
                    int shouldNull = 0;
                    int numMessage = 100;
                    for (int i = 0; i < numMessage; i++) {
                        Thread.sleep(10);
                        MockMessageHandler m;
                        // Make sure the last run of this loop doesn't leave the message handler null
                        if (i != numMessage-1 && shouldNull++%3 == 0) {
                            m = null;
                        } else {
                            m = mHandler;
                        }
                        try {
                            deliveryHandler.setMessageHandlerOptionallyDeliver(m);
                        } catch (MessageDeliveryException e) {
                            fail("Exception caught while setting message handler.");
                        }
                    }
                    queue.put(true);
                    statusQueue.put(true);
                } catch (InterruptedException e) {
                    fail("Interrupted Exception.");
                    Thread.currentThread().interrupt();
                }
            }
        });
        t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    executorThread.set(Thread.currentThread());
                    int nextMsg = 0;
                    // Enqueue 5 messages and then wait.
                    for (int i = 0; i < 5; i++) {
                        try {
                            assertTrue("Delivered a message although message handler is not set",
                                    deliveryHandler.offerAndOptionallyDeliver(getMessageData(nextMsg++, null)) == 0);
                        } catch (MessageDeliveryException e) {
                            fail("Exception while delivering messages.");
                        }
                    }
                    // Notify t2 so that it can go and deliver messages. And wait for it to notify.
                    queue.put(true);
                    assertTrue(queue.poll(10000, TimeUnit.MILLISECONDS));
                    // Remove the check for thread specific deliveries.
                    executorThread.set(null);
                    // Add 100 messages sleeping for 10 ms between each invocation.
                    for (int i = 0; i < 100; i++) {
                        try {
                            deliveryHandler.offerAndOptionallyDeliver(getMessageData(nextMsg++, null));
                        } catch (MessageDeliveryException e) {
                            fail("Exception while delivering a message.");
                        }
                        Thread.sleep(10);
                    }
                    // Wait for the other thread to complete then check how many messages were delivered.
                    assertTrue(queue.poll(10000, TimeUnit.MILLISECONDS));
                    assertTrue("Not all messages were delivered.", mHandler.getNextExpected() == nextMsg);
                    statusQueue.put(true);
                } catch (InterruptedException e) {
                    fail("Interrupted Exception");
                    Thread.currentThread().interrupt();
                }
            }
        });

        Thread.UncaughtExceptionHandler ueh = new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread thread, Throwable throwable) {
                try {
                    statusQueue.put(false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        t1.setUncaughtExceptionHandler(ueh);
        t2.setUncaughtExceptionHandler(ueh);
        t2.start(); t1.start();
        // Wait for threads to complete
        for (int i = 0; i < 2; i++) {
            if (!statusQueue.poll(10000, TimeUnit.MILLISECONDS)) {
                fail("Exception was thrown in the running thread.");
            }
        }
    }
}
