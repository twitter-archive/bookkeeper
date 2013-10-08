package org.apache.bookkeeper.stats;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides an instance of a bookkeeper client stats logger and a per channel
 * bookie client stats logger
 */
public class ClientStatsProvider {
    private static final ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger> pcbookieLoggerMap
            = new ConcurrentHashMap<InetSocketAddress, PCBookieClientStatsLogger>();

    public static BookkeeperClientStatsLogger createBookKeeperClientStatsLogger(StatsLogger statsLogger) {
        StatsLogger underlying = statsLogger.scope("bookkeeper_client");
        return new BookkeeperClientStatsLogger(underlying);
    }

    /**
     * @param addr
     * @return Get the instance of the per channel bookie client logger responsible for this addr.
     */
    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(InetSocketAddress addr,
                                                                           StatsLogger parentStatsLogger) {
        PCBookieClientStatsLogger statsLogger = pcbookieLoggerMap.get(addr);
        if (null == statsLogger) {
            StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(addr.getHostName().replace('.', '_').replace('-', '_'))
                .append("_").append(addr.getPort());
            StatsLogger underlying =
                parentStatsLogger.scope("per_channel_bookie_client").scope(nameBuilder.toString());
            PCBookieClientStatsLogger newStatsLogger = new PCBookieClientStatsLogger(underlying);
            PCBookieClientStatsLogger oldStatsLogger = pcbookieLoggerMap.putIfAbsent(addr, newStatsLogger);
            if (null == oldStatsLogger) {
                statsLogger = newStatsLogger;
            } else {
                statsLogger = oldStatsLogger;
            }
        }
        return statsLogger;
    }
}
