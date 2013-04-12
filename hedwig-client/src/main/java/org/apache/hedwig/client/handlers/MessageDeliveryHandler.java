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
package org.apache.hedwig.client.handlers;

import org.apache.hedwig.client.api.MessageHandler;
import org.apache.hedwig.client.data.MessageConsumeData;
import org.apache.hedwig.client.data.PubSubData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import static org.apache.hedwig.util.VarArgs.va;

/**
 * This class maintains a queue of outstanding messages to be delivered to a particular MessageHandler. If the message handler
 * is not set, the thread will just enqueue the message to be delivered. If the message handler is set, if a thread
 * that enqueues a message has enqueued it at the head of the queue, this thread will drain the queue. If any thread
 * wants to deliver while this is in progress, it will simply enqueue the message.
 */
public class MessageDeliveryHandler {

    private static final Logger logger = LoggerFactory.getLogger(MessageDeliveryHandler.class);

    // The pubsubdata that this message delivery handler is responsible for.
    private PubSubData origSubData = null;

    /**
     * The message handler to which messages should be delivered. Note that the message handler present while
     * queuing the message *might not* be the one to which messages are delivered. This can happen if setMessageHandlerOptionallyDeliver
     * is invoked while messages are in queue. The reason for this behavior is that a setMessageHandlerOptionallyDeliver invocation
     * implies that something went wrong and the existing message handler will fail.
     */
    private AtomicReference<MessageHandler> messageHandler = new AtomicReference<MessageHandler>();

    /**
     * The FIFO queue for all messages that are pending delivery to the client application. We store the
     * message consume data as this is the context that needs to be passed with the deliver method and
     * this has a reference to the message.
     */
    private LinkedBlockingDeque<MessageConsumeData> messageQueue = new LinkedBlockingDeque<MessageConsumeData>();

    private AtomicBoolean delivering = new AtomicBoolean(false);

    /**
     * This exception is thrown if there is an error while queuing the message or if
     * the deliver() function on the message handler results in a runtime exception.
     */
    public static class MessageDeliveryException extends Exception {
        public MessageDeliveryException(String message) {
            super(message);
        }
    }

    /**
     * Set the original subscription data that we received.
     * @param pubSubData
     */
    synchronized public void setOrigSubData(PubSubData pubSubData) {
        // We don't do a deep copy because the topic and subscriberId fields that we access should not
        // change and if they change, then we assume that the class using this is expecting this behavior.
        this.origSubData = pubSubData;
    }

    /**
     * Offer a message to the message queue and optionally deliver all outstanding messages. While delivery is in
     * progress, any thread that invokes this function will just enqueue the message and go away.
     * @param message This should not be null while invoking this function.
     * @return The number of messages delivered on this invocation
     */
    public int offerAndOptionallyDeliver(MessageConsumeData message) throws MessageDeliveryException {
        // First put the message in the queue so that we don't lose it.
        if (!messageQueue.offer(message)) {
            // We should never reach here since the queue is unbounded.
            logger.error("Could not add message: {} to an unbounded queue while delivering" +
                    " messages for topic:{} to subscriber:{}",
                va(message.toString(),
                    this.origSubData.topic.toStringUtf8(),
                    this.origSubData.subscriberId.toStringUtf8()));
            throw new MessageDeliveryException("Couldn't add message to an unbounded queue in the MessageDeliveryHandler.") ;
        }
        return tryDelivery();
    }

    /**
     * Try to delivery any outstanding messages in the queue.
     * @return
     */
    private int tryDelivery() throws MessageDeliveryException {
        int numDelivered = 0;

        // If original sub data is not set, we can't deliver.
        if (null == origSubData) {
            logger.error("tryDelivery invoked when we haven't set the original subscription data.");
            return numDelivered;
        }

        // We try to deliver the message to the latest message handler, always.
        MessageHandler handler = null;
        MessageConsumeData messageConsumeDataToDeliver = null;
        while(null != (handler = messageHandler.get())
                && null != (messageConsumeDataToDeliver = messageQueue.peek())
                && delivering.compareAndSet(false, true)) {
            // Get the message to be delivered while you're holding the lock.
            if (null != (messageConsumeDataToDeliver = messageQueue.peek())) {
                // The message handler could have changed since we acquired the reference. But this is okay
                // since we don't make any guarantees when these operations happen in parallel.
                try {
                    handler.deliver(origSubData.topic, origSubData.subscriberId, messageConsumeDataToDeliver.msg,
                            messageConsumeDataToDeliver.cb, messageConsumeDataToDeliver);
                    messageQueue.poll();
                    numDelivered++;
                } catch (RuntimeException e) {
                   logger.error("RuntimeException thrown while calling deliver on message:{} for topic:{}, subscriber:{}, Exception: {}",
                       va(messageConsumeDataToDeliver.msg.toString(),
                           origSubData.topic.toStringUtf8(),
                           origSubData.subscriberId.toStringUtf8()),e);
                    // We don't set delivering to false in case we throw an exception so as to stop anyone else
                    // from accidentally delivering messages while the exception is handled.
                    throw new MessageDeliveryException("RuntimeException while delivering message");
                }
            }

            // give up delivery to give other threads a chance to take this up.
            delivering.set(false);
        }

        // If we did not delivery because the handler was not set, log this.
        if (null == handler && numDelivered == 0) {
            logger.warn("Could not attempt delivery for topic: {}, subscriber: {} because a message handler was not set.",
                origSubData.topic.toStringUtf8(), origSubData.subscriberId.toStringUtf8());
        }
        return numDelivered;
    }

    /**
     * Forces delivering to be set to false.
     */
    public void resetDelivering() {
        delivering.set(false);
    }

    /**
     * Sets the message handler and delivers any outstanding messages if it can.
     * @param handler
     * @return Return the number of messages delivered on this invocation.
     */
    public int setMessageHandlerOptionallyDeliver(MessageHandler handler) throws MessageDeliveryException {
        // Set the message handler and then try to deliver any outstanding messages
        messageHandler.set(handler);
        return tryDelivery();
    }

    /**
     * Get the registered message handler.
     * @return
     */
    public MessageHandler getMessageHandler() {
        return messageHandler.get();
    }

}
