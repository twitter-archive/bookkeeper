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
package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.MathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.bookkeeper.replication.ReplicationStats.ACTIVE_BOOKIES;
import static org.apache.bookkeeper.replication.ReplicationStats.AVAILABLE_BOOKIES;
import static org.apache.bookkeeper.replication.ReplicationStats.LOST_BOOKIES;
import static org.apache.bookkeeper.replication.ReplicationStats.READ_ONLY_BOOKIES;
import static org.apache.bookkeeper.replication.ReplicationStats.REGISTERED_BOOKIES;
import static org.apache.bookkeeper.replication.ReplicationStats.STALE_BOOKIES;

/**
 * This class will hold the facts for the bookie cluster.
 */
public class BookieClusterManager implements BookiesListener {

    static final Logger logger = LoggerFactory.getLogger(BookieClusterManager.class);


    static class UpdateStatus {

        long lastUpdatedTimestamp;

        UpdateStatus() {
            this.lastUpdatedTimestamp = MathUtils.now();
        }

        UpdateStatus updateLastTimestamp() {
            this.lastUpdatedTimestamp = MathUtils.now();
            return this;
        }

        long getLastTimestamp() {
            return this.lastUpdatedTimestamp;
        }
    }

    protected final ServerConfiguration conf;
    protected final BookKeeper bkc;
    protected final long staleBookieIntervalInMs;
    protected final Map<BookieSocketAddress, UpdateStatus> bookieStatuses =
            new HashMap<BookieSocketAddress, UpdateStatus>();
    protected AtomicBoolean isStarted = new AtomicBoolean();
    protected  Set<BookieSocketAddress> registeredBookies = new HashSet<BookieSocketAddress>();
    protected  Set<BookieSocketAddress> availableBookies = new HashSet<BookieSocketAddress>();
    protected  Set<BookieSocketAddress> readOnlyBookies = new HashSet<BookieSocketAddress>();
    protected  Set<BookieSocketAddress> staleBookies = new HashSet<BookieSocketAddress>();
    protected  Set<BookieSocketAddress> activeBookies = new HashSet<BookieSocketAddress>();
    protected  Set<BookieSocketAddress> lostBookies = new HashSet<BookieSocketAddress>();

    // stats for bookie cluster
    private StatsLogger statsLogger;

    public BookieClusterManager(BookKeeper bkc){
        this(new ServerConfiguration(), bkc, NullStatsLogger.INSTANCE);
    }

    public BookieClusterManager(ServerConfiguration conf, BookKeeper bkc){
        this(conf, bkc, NullStatsLogger.INSTANCE);
    }

    public BookieClusterManager(ServerConfiguration conf, BookKeeper bkc, StatsLogger statsLogger) {
        this.conf = conf;
        this.bkc = bkc;
        this.staleBookieIntervalInMs =
                TimeUnit.MILLISECONDS.convert(conf.getAuditorStaleBookieInterval(), TimeUnit.SECONDS);
        this.statsLogger = statsLogger;
    }

    public void start() throws BKException {
        if (isStarted.compareAndSet(false, true)) {
            this.bkc.bookieWatcher.registerBookiesListener(this);
            fetchRegisteredBookies();
            fetchAvailableBookies();
            fetchReadOnlyBookies();
        }
    }

    /**
     * Fetch stale bookies and update active bookies for the current bookie cluster.
     * Each time you call this function, stale bookies will be recalculated based on
     * the current bookie statuses, and these stale bookie will be removed from the bookie statuses.
     * So if you keep calling this function, ideally there should be fewer stale bookies fetched each time.
     * @throws BKException
     */
    public Set<BookieSocketAddress> fetchStaleBookies() throws BKException {
        updateBookiesStatuses(this.availableBookies);
        updateBookiesStatuses(this.readOnlyBookies);
        Set<BookieSocketAddress> staleBookies = new HashSet<>();
        Set<BookieSocketAddress> activeBookies = new HashSet<>();
        long now = MathUtils.now();
        synchronized (this) {
            Iterator<Map.Entry<BookieSocketAddress, UpdateStatus>> iter =
                bookieStatuses.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<BookieSocketAddress, UpdateStatus> entry = iter.next();
                long millisSinceLastSeen = now - entry.getValue().getLastTimestamp();
                if (millisSinceLastSeen > staleBookieIntervalInMs) {
                    logger.info("Bookie {} (seen @ {}) become stale for {} ms, remove it.",
                        new Object[] { entry.getKey(), entry.getValue().getLastTimestamp(),
                            millisSinceLastSeen });
                    iter.remove();
                    staleBookies.add(entry.getKey());
                } else {
                    activeBookies.add(entry.getKey());
                }
            }
            updateBookies(this.staleBookies, staleBookies);
            updateBookies(this.activeBookies, activeBookies);
        }
        return staleBookies;
    }

    public void close() {
        if (isStarted.compareAndSet(true, false)) {
            this.bkc.bookieWatcher.unregisterBookiesListener(this);
        }
    }

    /**
     * Fetch all registered bookies and update bookie status
     * @throws BKException
     */
    private void fetchRegisteredBookies() throws BKException {
        Collection<BookieSocketAddress> registeredBookies = this.bkc.bookieWatcher.getRegisteredBookies();
        logger.info("Fetch all registered bookies : {}", registeredBookies);
        updateBookiesStatuses(registeredBookies);
        updateBookies(this.registeredBookies, registeredBookies);
    }

    /**
     * Fetch all available bookies and update bookie status
     * @throws BKException
     */
    private void fetchAvailableBookies() throws BKException {
        Collection<BookieSocketAddress> availableBookies = this.bkc.bookieWatcher.getAvailableBookies();
        logger.info("Fetch all available bookies: {}", availableBookies);
        updateBookiesStatuses(availableBookies);
        updateBookies(this.availableBookies, availableBookies);
    }

    /**
     * fetch all readonly bookies and update bookie status
     * @throws BKException
     */
    private void fetchReadOnlyBookies() throws BKException {
        Collection<BookieSocketAddress> readOnlyBookies = this.bkc.bookieWatcher.getReadOnlyBookies();
        logger.info("Fetch all readonly bookies: {}", readOnlyBookies);
        updateBookiesStatuses(readOnlyBookies);
        updateBookies(this.readOnlyBookies, readOnlyBookies);
    }

    /**
     * Update the bookie cluster status. This is used to keep track of the update time
     * for the active bookies so that we can calculate stale bookies.
     *
     * @param bookies bookies to update the lastUpdateTime
     */
    private synchronized void updateBookiesStatuses(Collection<BookieSocketAddress> bookies) {
        for (BookieSocketAddress bookie : bookies) {
            UpdateStatus bs = bookieStatuses.get(bookie);
            if (null == bs) {
                bs = new UpdateStatus();
                bookieStatuses.put(bookie, bs);
            } else {
                bs.updateLastTimestamp();
            }
        }
    }

    /**
     * This is to update the bookies stored in cluster manage without change the reference. Because
     * changing the reference would break the stats logger.
     * @param oldBookies old bookies to be updated
     * @param newBookies new bookies
     */
    private synchronized void updateBookies(Collection<BookieSocketAddress> oldBookies,
                                            Collection<BookieSocketAddress> newBookies) {
        oldBookies.clear();
        oldBookies.addAll(newBookies);
    }

    @Override
    public void availableBookiesChanged(Set<BookieSocketAddress> bookies) {
        updateBookiesStatuses(bookies);
        updateBookies(this.availableBookies, bookies);
    }

    @Override
    public void readOnlyBookiesChanged(Set<BookieSocketAddress> bookies) {
        updateBookiesStatuses(bookies);
        updateBookies(this.readOnlyBookies, bookies);
    }

    public void lostBookiesChanged(Set<BookieSocketAddress> bookies) {
        updateBookies(this.lostBookies, bookies);
    }


    public Set<BookieSocketAddress> getAvailableBookies() {
        return availableBookies;
    }

    public Set<BookieSocketAddress> getReadOnlyBookies() {
        return readOnlyBookies;
    }

    public Set<BookieSocketAddress> getActiveBookies() {
        return activeBookies;
    }

    /**
     * We probably only want the node who becomes the auditor to expose the bookie cluster stats.
     * Because only auditor will keep fetching stale and lost bookies.
     */
    public void enableStats(StatsLogger statsLogger) {
        if(statsLogger == null) {
            return;
        }
        this.statsLogger = statsLogger;
        registerCollectionGauges(REGISTERED_BOOKIES, this.registeredBookies);
        registerCollectionGauges(AVAILABLE_BOOKIES, this.availableBookies);
        registerCollectionGauges(READ_ONLY_BOOKIES, this.readOnlyBookies);
        registerCollectionGauges(STALE_BOOKIES, this.staleBookies);
        registerCollectionGauges(ACTIVE_BOOKIES, this.activeBookies);
        registerCollectionGauges(LOST_BOOKIES, this.lostBookies);
    }

    private void registerCollectionGauges(String scopeName, final Collection collection) {
        Gauge<Number> gauge = new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                return collection.size();
            }
        };
        this.statsLogger.registerGauge(scopeName, gauge);
    }

}
