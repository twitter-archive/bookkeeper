package org.apache.bookkeeper.stats;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides an instance of a bookkeeper client stats logger and a per channel
 * bookie client stats logger
 */
public class ClientStatsProvider {
    private static final ConcurrentMap<String, ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger>> pcBookieLoggerMaps =
            new ConcurrentHashMap<String, ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger>>();

    public static BookkeeperClientStatsLogger createBookKeeperClientStatsLogger(StatsLogger statsLogger) {
        StatsLogger underlying = statsLogger.scope("bookkeeper_client");
        return new BookkeeperClientStatsLogger(underlying);
    }

    private static ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger> getLoggerMap(String scope) {
        ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger> loggerMap = pcBookieLoggerMaps.get(scope);
        if (null == loggerMap) {
            loggerMap = new ConcurrentHashMap<BookieSocketAddress, PCBookieClientStatsLogger>();
            ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger> oldLoggerMap =
                pcBookieLoggerMaps.putIfAbsent(scope, loggerMap);
            if (null != oldLoggerMap) {
                loggerMap = oldLoggerMap;
            }
        }
        return loggerMap;
    }

    /**
     * @param addr
     * @return Get the instance of the per channel bookie client logger responsible for this addr.
     */
    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(ClientConfiguration conf,
                                                                           BookieSocketAddress addr,
                                                                           StatsLogger parentStatsLogger) {
        return getPCBookieStatsLoggerInstance("", conf, addr, parentStatsLogger);
    }

    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(String scope,
                                                                           ClientConfiguration conf,
                                                                           BookieSocketAddress addr,
                                                                           StatsLogger parentStatsLogger) {
        StatsLogger underlyingLogger = parentStatsLogger.scope("per_channel_bookie_client");
        if (!"".equals(scope)) {
            underlyingLogger = underlyingLogger.scope(scope);
        }
        if (!conf.getEnablePerHostStats()) {
            return new PCBookieClientStatsLogger(underlyingLogger);
        }
        ConcurrentMap<BookieSocketAddress, PCBookieClientStatsLogger> loggerMap = getLoggerMap(scope);
        PCBookieClientStatsLogger statsLogger = loggerMap.get(addr);
        if (null == statsLogger) {
            StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(addr.getHostName().replace('.', '_').replace('-', '_'))
                .append("_").append(addr.getPort());
            StatsLogger underlying = underlyingLogger.scope(nameBuilder.toString());
            PCBookieClientStatsLogger newStatsLogger = new PCBookieClientStatsLogger(underlying);
            PCBookieClientStatsLogger oldStatsLogger = loggerMap.putIfAbsent(addr, newStatsLogger);
            if (null == oldStatsLogger) {
                statsLogger = newStatsLogger;
            } else {
                statsLogger = oldStatsLogger;
            }
        }
        return statsLogger;
    }
}
