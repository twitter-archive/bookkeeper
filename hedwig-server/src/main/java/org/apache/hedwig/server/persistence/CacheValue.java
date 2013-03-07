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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hedwig.protocol.PubSubProtocol.Message;

/**
 * This class is NOT thread safe. It need not be thread-safe because our
 * read-ahead cache will operate with only 1 thread
 *
 */
public class CacheValue {

    // Actually we don't care the order of callbacks
    // when a scan callback, it should be delivered to both callbacks
    Set<ScanCallbackWithContext> callbacks;
    Message message;

    public CacheValue() {
        this.callbacks = Collections.synchronizedSet(new HashSet<ScanCallbackWithContext>());
    }

    public CacheValue(Message message) {
        this.message = message;
    }

    public boolean isStub() {
        return message == null;
    }

    public boolean wasStub() {
        return callbacks != null;
    }

    // Cache weight static (loading cache)
    public int getCacheWeight() {
        return wasStub()? 0 : message.getBody().size();
    }

    public void setMessageAndInvokeCallbacks(Message message) {
        if (this.message != null) {
            return;
        }

        this.message = message;
        synchronized (callbacks) {
            for (ScanCallbackWithContext callbackWithCtx : callbacks) {
                if (null != callbackWithCtx) {
                    callbackWithCtx.getScanCallback().messageScanned(callbackWithCtx.getCtx(), message);
                }
            }
        }
    }

    public boolean removeCallback(ScanCallback callback, Object ctx) {
        if (null == callbacks) {
            return false;
        }
        return callbacks.remove(new ScanCallbackWithContext(callback, ctx));
    }

    public void addCallback(ScanCallback callback, Object ctx) {
        if (!isStub()) {
            // call the callback right away
            callback.messageScanned(ctx, message);
            return;
        }

        callbacks.add(new ScanCallbackWithContext(callback, ctx));
    }

    public Message getMessage() {
        return message;
    }

    public void setErrorAndInvokeCallbacks(Exception exception) {
        if (this.message != null) {
            return;
        }

        for (ScanCallbackWithContext callbackWithCtx : callbacks) {
            if (null != callbackWithCtx) {
                callbackWithCtx.getScanCallback().scanFailed(callbackWithCtx.getCtx(), exception);
            }
        }
    }
}
