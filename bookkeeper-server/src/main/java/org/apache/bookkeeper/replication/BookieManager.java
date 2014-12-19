package org.apache.bookkeeper.replication;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.BookiesListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * It manages healthy bookies.
 */
class BookieManager implements BookiesListener {

    static final Logger logger = LoggerFactory.getLogger(BookieManager.class);

    static class BookieStatus {

        long lastUpdatedTimestamp;

        BookieStatus() {
            this.lastUpdatedTimestamp = MathUtils.now();
        }

        BookieStatus updateLastTimestamp() {
            this.lastUpdatedTimestamp = MathUtils.now();
            return this;
        }

        long getLastTimestamp() {
            return this.lastUpdatedTimestamp;
        }
    }

    protected final ServerConfiguration conf;
    protected final BookKeeperAdmin admin;
    protected final Map<InetSocketAddress, BookieStatus> bookieStatuses =
            new HashMap<InetSocketAddress, BookieStatus>();
    protected final long staleBookieIntervalInMs;

    BookieManager(ServerConfiguration conf,
                  BookKeeperAdmin admin) {
        this.conf = conf;
        this.admin = admin;
        this.staleBookieIntervalInMs =
                TimeUnit.MILLISECONDS.convert(conf.getAuditorStaleBookieInterval(), TimeUnit.SECONDS);
    }

    public void start() throws BKException {
        fetchRegisteredBookies();
        this.admin.registerBookiesListener(this);
        fetchBookies();
    }

    public Pair<Set<String>, Set<String>> getAvailableAndStaleBookies() throws BKException {
        fetchBookies();
        Set<String> availableBookies = new HashSet<String>();
        Set<String> staleBookies = new HashSet<String>();
        long now = MathUtils.now();
        synchronized (this) {
            Iterator<Map.Entry<InetSocketAddress, BookieStatus>> iter =
                    bookieStatuses.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<InetSocketAddress, BookieStatus> entry = iter.next();
                long millisSinceLastSeen = now - entry.getValue().getLastTimestamp();
                if (millisSinceLastSeen > staleBookieIntervalInMs) {
                    logger.info("Bookie {} (seen @ {}) become stale for {} ms, remove it.",
                                new Object[] { entry.getKey(), entry.getValue().getLastTimestamp(),
                                        millisSinceLastSeen });
                    iter.remove();
                    staleBookies.add(StringUtils.addrToString(entry.getKey()));
                } else {
                    availableBookies.add(StringUtils.addrToString(entry.getKey()));
                }
            }
        }
        return Pair.of(availableBookies, staleBookies);
    }

    private void fetchRegisteredBookies() throws BKException {
        Collection<InetSocketAddress> registeredBookies =
                this.admin.getRegisteredBookies();
        logger.info("Fetch registered bookies : {}", registeredBookies);
        updateBookies(registeredBookies);
    }

    private void fetchBookies() throws BKException {
        Collection<InetSocketAddress> availableBookies =
                this.admin.getAvailableBookies();
        logger.info("Fetch available bookies: {}", availableBookies);
        Collection<InetSocketAddress> readOnlyBookies =
                this.admin.getReadOnlyBookies();
        logger.info("Fetch readonly bookies: {}", readOnlyBookies);
        updateBookies(availableBookies);
        updateBookies(readOnlyBookies);
    }

    private synchronized void updateBookies(Collection<InetSocketAddress> bookies) {
        for (InetSocketAddress bookie : bookies) {
            updateBookie(bookie);
        }
    }

    private synchronized void updateBookie(InetSocketAddress bookie) {
        BookieStatus bs = bookieStatuses.get(bookie);
        if (null == bs) {
            bs = new BookieStatus();
            bookieStatuses.put(bookie, bs);
        } else {
            bs.updateLastTimestamp();
        }
    }

    @Override
    public void availableBookiesChanged(Set<InetSocketAddress> bookies) {
        updateBookies(bookies);
    }

    @Override
    public void readOnlyBookiesChanged(Set<InetSocketAddress> bookies) {
        updateBookies(bookies);
    }

}
