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
package org.apache.bookkeeper.proto;

import static com.google.common.base.Charsets.UTF_8;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.NetUtils;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.ReadEntryCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.WriteCallback;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.OrderedSafeExecutor;
import org.apache.bookkeeper.util.SafeRunnable;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implements the client-side part of the BookKeeper protocol.
 *
 */
public class BookieClient implements PerChannelBookieClientFactory {
    static final Logger LOG = LoggerFactory.getLogger(BookieClient.class);

    final OrderedSafeExecutor executor;
    final ClientSocketChannelFactory channelFactory;
    final ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool> channels =
            new ConcurrentHashMap<BookieSocketAddress, PerChannelBookieClientPool>();
    final HashedWheelTimer requestTimer;
    private final ClientConfiguration conf;
    private volatile boolean closed;
    private ReentrantReadWriteLock closeLock;
    private final StatsLogger statsLogger;
    private final int numConnectionsPerBookie;
    // whether the timer is one we created, or is owned by whoever
    // instantiated us
    private final boolean ownTimer;
    private final Optional<DNSToSwitchMapping> dnsResolver;

    public BookieClient(ClientConfiguration conf, ClientSocketChannelFactory channelFactory, OrderedSafeExecutor executor) {
        this(conf, channelFactory, executor, NullStatsLogger.INSTANCE, null, Optional.<DNSToSwitchMapping>absent());
    }

    public BookieClient(ClientConfiguration conf, ClientSocketChannelFactory channelFactory, OrderedSafeExecutor executor,
                        StatsLogger statsLogger, HashedWheelTimer requestTimer, Optional<DNSToSwitchMapping> dnsResolver) {
        if (null == channelFactory) {
            throw new NullPointerException();
        }

        this.conf = conf;
        this.channelFactory = channelFactory;
        this.executor = executor;
        this.closed = false;
        this.closeLock = new ReentrantReadWriteLock();
        this.statsLogger = statsLogger;
        this.numConnectionsPerBookie = conf.getNumChannelsPerBookie();
        this.dnsResolver = dnsResolver;
        if (null == requestTimer) {
            this.requestTimer = new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("BookieClientTimer-%d").build(),
                conf.getTimeoutTimerTickDurationMs(), TimeUnit.MILLISECONDS,
                conf.getTimeoutTimerNumTicks());
            this.ownTimer = true;
        } else {
            this.requestTimer = requestTimer;
            this.ownTimer = false;
        }
    }

    private int getRc(int rc) {
        if (BKException.Code.OK == rc) {
            return rc;
        } else {
            if (closed) {
                return BKException.Code.ClientClosedException;
            } else {
                return rc;
            }
        }
    }

    @Override
    public PerChannelBookieClient create(BookieSocketAddress address) {
        Optional<String> networkLocation = Optional.absent();
        try {
            if (dnsResolver.isPresent()) {
                networkLocation = Optional.of(NetUtils.resolveNetworkLocation(dnsResolver.get(), address.getSocketAddress()));
            }
        } catch (Exception exc) {
            LOG.info("Failed to resolve the network location", exc);
        }
        return new PerChannelBookieClient(conf, executor, channelFactory, address,
                                          requestTimer, statsLogger, networkLocation);
    }

    private PerChannelBookieClientPool lookupClient(BookieSocketAddress addr, Object key) {
        PerChannelBookieClientPool clientPool = channels.get(addr);
        if (null == clientPool) {
            closeLock.readLock().lock();
            try {
                if (closed) {
                    return null;
                }
                PerChannelBookieClientPool newClientPool =
                        new DefaultPerChannelBookieClientPool(this, addr, numConnectionsPerBookie);
                PerChannelBookieClientPool oldClientPool = channels.putIfAbsent(addr, newClientPool);
                if (null == oldClientPool) {
                    clientPool = newClientPool;
                    // initialize the pool only after we put the pool into the map
                    clientPool.intialize();
                } else {
                    clientPool = oldClientPool;
                    newClientPool.close(false);
                }
            } finally {
                closeLock.readLock().unlock();
            }
        }
        return clientPool;
    }

    public void closeClients(final Set<BookieSocketAddress> addrs) {
        final HashSet<PerChannelBookieClientPool> clients =
                new HashSet<PerChannelBookieClientPool>();
        closeLock.readLock().lock();
        try {
            for (BookieSocketAddress a : addrs) {
                PerChannelBookieClientPool c = channels.get(a);
                if (c != null) {
                    clients.add(c);
                }
            }

            if (clients.size() == 0) {
                return;
            }
        } finally {
            closeLock.readLock().unlock();
        }
        for (PerChannelBookieClientPool c : clients) {
            c.disconnect(false);
        }
    }

    private void completeWrite(final int rc,
                               final long ledgerId,
                               final long entryId,
                               final BookieSocketAddress addr,
                               final WriteCallback cb,
                               final Object ctx) {
        try {
            executor.submitOrdered(ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    cb.writeComplete(rc, ledgerId, entryId, addr, ctx);
                }
                @Override
                public String toString() {
                    return String.format("CompleteWrite(ledgerId=%d, entryId=%d, addr=%s)", ledgerId, entryId, addr);
                }
            });
        } catch (RejectedExecutionException ree) {
            cb.writeComplete(getRc(BKException.Code.InterruptedException), ledgerId, entryId, addr, ctx);
        }
    }

    public void addEntry(final BookieSocketAddress addr,
                         final long ledgerId,
                         final byte[] masterKey,
                         final long entryId,
                         final ChannelBuffer toSend,
                         final WriteCallback cb,
                         final Object ctx,
                         final int options) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                completeWrite(BKException.Code.BookieHandleNotAvailableException,
                              ledgerId, entryId, addr, cb, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        completeWrite(rc, ledgerId, entryId, addr, cb, ctx);
                        return;
                    }
                    pcbc.addEntry(ledgerId, masterKey, entryId, toSend, cb, ctx, options);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    private void completeRead(final int rc,
                              final long ledgerId,
                              final long entryId,
                              final ChannelBuffer entry,
                              final ReadEntryCallback cb,
                              final Object ctx) {
        try {
            executor.submitOrdered(ledgerId, new SafeRunnable() {
                @Override
                public void safeRun() {
                    cb.readEntryComplete(rc, ledgerId, entryId, entry, ctx);
                }
            });
        } catch (RejectedExecutionException ree) {
            cb.readEntryComplete(getRc(BKException.Code.InterruptedException),
                                 ledgerId, entryId, entry, ctx);
        }
    }

    public void readEntryAndFenceLedger(final BookieSocketAddress addr,
                                        final long ledgerId,
                                        final byte[] masterKey,
                                        final long entryId,
                                        final ReadEntryCallback cb,
                                        final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                completeRead(BKException.Code.BookieHandleNotAvailableException,
                             ledgerId, entryId, null, cb, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {
                    if (rc != BKException.Code.OK) {
                        completeRead(rc, ledgerId, entryId, null, cb, ctx);
                        return;
                    }
                    pcbc.readEntryAndFenceLedger(ledgerId, masterKey, entryId, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void readEntry(final BookieSocketAddress addr,
                          final long ledgerId,
                          final long entryId,
                          final ReadEntryCallback cb,
                          final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                completeRead(BKException.Code.BookieHandleNotAvailableException,
                        ledgerId, entryId, null, cb, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {

                    if (rc != BKException.Code.OK) {
                        completeRead(rc, ledgerId, entryId, null, cb, ctx);
                        return;
                    }
                    pcbc.readEntry(ledgerId, entryId, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public void readEntryWaitForLACUpdate(final BookieSocketAddress addr,
                                          final long ledgerId,
                                          final long entryId,
                                          final long previousLAC,
                                          final long timeOutInMillis,
                                          final boolean piggyBackEntry,
                                          final ReadEntryCallback cb,
                                          final Object ctx) {
        closeLock.readLock().lock();
        try {
            final PerChannelBookieClientPool client = lookupClient(addr, entryId);
            if (client == null) {
                completeRead(BKException.Code.BookieHandleNotAvailableException,
                        ledgerId, entryId, null, cb, ctx);
                return;
            }

            client.obtain(new GenericCallback<PerChannelBookieClient>() {
                @Override
                public void operationComplete(final int rc, PerChannelBookieClient pcbc) {

                    if (rc != BKException.Code.OK) {
                        completeRead(rc, ledgerId, entryId, null, cb, ctx);
                        return;
                    }
                    pcbc.readEntryWaitForLACUpdate(ledgerId, entryId, previousLAC, timeOutInMillis, piggyBackEntry, cb, ctx);
                }
            });
        } finally {
            closeLock.readLock().unlock();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public Timeout scheduleTimeout(TimerTask task, long timeoutSec, TimeUnit timeUnit) {
        return requestTimer.newTimeout(task, timeoutSec, timeUnit);
    }

    public void close() {
        closeLock.writeLock().lock();
        try {
            closed = true;
            for (PerChannelBookieClientPool pool : channels.values()) {
                pool.close(true);
            }
        } finally {
            closeLock.writeLock().unlock();
        }
        // Shut down the timeout executor if we created it.
        if (ownTimer) {
            this.requestTimer.stop();
        }
    }

    private static class Counter {
        int i;
        int total;

        synchronized void inc() {
            i++;
            total++;
        }

        synchronized void dec() {
            i--;
            notifyAll();
        }

        synchronized void wait(int limit) throws InterruptedException {
            while (i > limit) {
                wait();
            }
        }

        synchronized int total() {
            return total;
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws NumberFormatException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws NumberFormatException, IOException, InterruptedException {
        if (args.length != 3) {
            System.err.println("USAGE: BookieClient bookieHost port ledger#");
            return;
        }
        WriteCallback cb = new WriteCallback() {

            public void writeComplete(int rc, long ledger, long entry, BookieSocketAddress addr, Object ctx) {
                Counter counter = (Counter) ctx;
                counter.dec();
                if (rc != 0) {
                    System.out.println("rc = " + rc + " for " + entry + "@" + ledger);
                }
            }
        };
        Counter counter = new Counter();
        byte hello[] = "hello".getBytes(UTF_8);
        long ledger = Long.parseLong(args[2]);
        ClientSocketChannelFactory channelFactory = new NioClientSocketChannelFactory(Executors.newCachedThreadPool(), Executors
                .newCachedThreadPool());
        OrderedSafeExecutor executor = new OrderedSafeExecutor(1);
        BookieClient bc = new BookieClient(new ClientConfiguration(), channelFactory, executor);
        BookieSocketAddress addr = new BookieSocketAddress(args[0], Integer.parseInt(args[1]));

        for (int i = 0; i < 100000; i++) {
            counter.inc();
            bc.addEntry(addr, ledger, new byte[0], i, ChannelBuffers.wrappedBuffer(hello), cb, counter, 0);
        }
        counter.wait(0);
        System.out.println("Total = " + counter.total());
        channelFactory.releaseExternalResources();
        executor.shutdown();
    }
}
