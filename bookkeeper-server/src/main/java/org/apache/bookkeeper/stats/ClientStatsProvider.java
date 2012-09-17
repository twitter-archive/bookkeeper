package org.apache.bookkeeper.stats;

import com.twitter.common.stats.Stats;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides an instance of a bookkeeper client stats logger and a per channel
 * bookie client stats logger
 */
public class ClientStatsProvider {
    private static AtomicInteger loggerIndex = new AtomicInteger(0);
    private static final ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger> pcbookieLoggerMap
            = new ConcurrentHashMap<InetSocketAddress, PCBookieClientStatsLogger>();

    public static BookkeeperClientStatsLogger getStatsLoggerInstance() {
        Logger.getLogger(Stats.class.getName()).setLevel(Level.SEVERE);
        return new BookkeeperClientStatsImpl("bookkeeper_client_" + loggerIndex.getAndIncrement());
    }

    /**
     * @param addr
     * @return Get the instance of the per channel bookie client logger responsible for this addr.
     */
    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(InetSocketAddress addr) {
        Logger.getLogger(Stats.class.getName()).setLevel(Level.SEVERE);
        String statName = new StringBuilder("per_channel_bookie_client_")
                .append(addr.getHostName().replace('.', '_'))
                .append("_")
                .append(addr.getPort())
                .toString();
        PCBookieClientStatsLogger logger = pcbookieLoggerMap
                .putIfAbsent(addr, new PCBookieClientStatsImpl(statName));
        if (null == logger) {
            return pcbookieLoggerMap.get(addr);
        } else {
            return logger;
        }
    }
}
