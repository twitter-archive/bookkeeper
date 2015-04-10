package org.apache.bookkeeper.proto;

/*
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

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.Executors;

import com.google.common.base.Optional;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.proto.PerChannelBookieClient.ConnectionState;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.OrderedSafeExecutor;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.Channel;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for PerChannelBookieClient. Historically, this class has
 * had a few race conditions, so this is what these tests focus on.
 */
public class TestPerChannelBookieClient extends BookKeeperClusterTestCase {
    static Logger LOG = LoggerFactory.getLogger(TestPerChannelBookieClient.class);

    public TestPerChannelBookieClient() {
        super(1);
    }

    /**
     * Test that a race does not exist between connection completion
     * and client closure. If a race does exist, this test will simply
     * hang at releaseExternalResources() as it is uninterruptible.
     * This specific race was found in
     * {@link https://issues.apache.org/jira/browse/BOOKKEEPER-485}.
     */
    @Test(timeout=60000)
    public void testConnectCloseRace() {
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);

        BookieSocketAddress addr = getBookie(0);
        for (int i = 0; i < 1000; i++) {
            PerChannelBookieClient client = new PerChannelBookieClient(executor, channelFactory, addr);
            client.connectIfNeededAndDoOp(new GenericCallback<PerChannelBookieClient>() {
                    @Override
                    public void operationComplete(int rc, PerChannelBookieClient client) {
                        // do nothing, we don't care about doing anything with the connection,
                        // we just want to trigger it connecting.
                    }
                });
            client.close();
        }
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    @Test(timeout=60000)
    public void testDisconnectRace() throws Exception {
        final GenericCallback<PerChannelBookieClient> nullop = new GenericCallback<PerChannelBookieClient>() {
            @Override
            public void operationComplete(int rc, PerChannelBookieClient client) {
                // do nothing, we don't care about doing anything with the connection,
                // we just want to trigger it connecting.
            }
        };
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
            Executors.newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        BookieSocketAddress addr = getBookie(0);

        final PerChannelBookieClient client = new PerChannelBookieClient(executor, channelFactory, addr);
        final AtomicBoolean inconsistent = new AtomicBoolean(false);
        final AtomicBoolean running = new AtomicBoolean(true);
        Thread connectThread = new Thread() {
            public void run() {
                while (running.get()) {
                    client.connectIfNeededAndDoOp(nullop);
                }
            }
        };
        Thread disconnectThread = new Thread() {
            public void run() {
                while (running.get()) {
                    client.disconnect();
                    try {
                        Thread.sleep(100);
                    } catch (Exception exc) {
                        //
                    }
                }
            }
        };
        Thread checkThread = new Thread() {
            public void run() {
                ConnectionState state;
                Channel channel;
                while (running.get()) {
                    synchronized (client) {
                        state = client.state;
                        channel = client.channel;
                    }
                    if (state == ConnectionState.CONNECTED
                        && !channel.isConnected()) {
                        inconsistent.set(true);
                    } else if (state != ConnectionState.CONNECTED
                        && channel.isConnected()) {
                        inconsistent.set(true);
                    }
                    try {
                        Thread.sleep(1);
                    } catch (Exception exc) {
                      //
                    }
                }
            }
        };
        connectThread.start();
        disconnectThread.start();
        checkThread.start();

        Thread.sleep(5000);
        running.set(false);
        connectThread.join();
        disconnectThread.join();
        checkThread.join();
        assertFalse("state and channel inconsistent", inconsistent.get());

        client.close();
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }

    WriteCallback wrcb = new WriteCallback() {
        public void writeComplete(int rc, long ledgerId, long entryId, BookieSocketAddress addr, Object ctx) {
            if (ctx != null) {
                synchronized (ctx) {
                    ctx.notifyAll();
                }
            }
        }
    };

    /**
     * Just push a write through the async channel write path to ensure this config option is working.
     */
    @Test(timeout=60000)
    public void testAsyncWriteToChannel() throws Exception {
        ClientSocketChannelFactory channelFactory
            = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
                                                Executors.newCachedThreadPool());

        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        final byte[] passwd = new byte[20];
        Arrays.fill(passwd, (byte) 'a');
        final Object ctx = new Object();
        ClientConfiguration conf = new ClientConfiguration();
        conf.setWriteToChannelAsync(true);

        BookieSocketAddress addr = getBookie(0);
        PerChannelBookieClient client = new PerChannelBookieClient(
            conf, executor, channelFactory, addr, null, NullStatsLogger.INSTANCE, Optional.<String>absent());
        client.connectIfNeededAndDoOp(new GenericCallback<PerChannelBookieClient>() {
            @Override
            public void operationComplete(int rc, PerChannelBookieClient client) {
                ChannelBuffer bb = ChannelBuffers.buffer(128);
                client.addEntry(1, passwd, 1, bb, wrcb, ctx, BookieProtocol.FLAG_NONE);
            }
        });
        synchronized (ctx) {
            ctx.wait();
        }
        client.close();
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }
}
