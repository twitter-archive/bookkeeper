package org.apache.bookkeeper.stats;

import org.apache.bookkeeper.conf.ClientConfiguration;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Provides an instance of a bookkeeper client stats logger and a per channel
 * bookie client stats logger
 */
public class ClientStatsProvider {
    private static final ConcurrentMap<String, ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger>> pcBookieLoggerMaps =
            new ConcurrentHashMap<String, ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger>>();

    public static BookkeeperClientStatsLogger createBookKeeperClientStatsLogger(StatsLogger statsLogger) {
        StatsLogger underlying = statsLogger.scope("bookkeeper_client");
        return new BookkeeperClientStatsLogger(underlying);
    }

    private static ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger> getLoggerMap(String scope) {
        ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger> loggerMap = pcBookieLoggerMaps.get(scope);
        if (null == loggerMap) {
            loggerMap = new ConcurrentHashMap<InetSocketAddress, PCBookieClientStatsLogger>();
            pcBookieLoggerMaps.putIfAbsent(scope, loggerMap);
        }
        return loggerMap;
    }

    /**
     * @param addr
     * @return Get the instance of the per channel bookie client logger responsible for this addr.
     */
    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(ClientConfiguration conf,
                                                                           InetSocketAddress addr,
                                                                           StatsLogger parentStatsLogger) {
        return getPCBookieStatsLoggerInstance("", conf, addr, parentStatsLogger);
    }

    public static PCBookieClientStatsLogger getPCBookieStatsLoggerInstance(String scope,
                                                                           ClientConfiguration conf,
                                                                           InetSocketAddress addr,
                                                                           StatsLogger parentStatsLogger) {
        StatsLogger underlyingLogger = parentStatsLogger.scope("per_channel_bookie_client");
        if (!"".equals(scope)) {
            underlyingLogger = underlyingLogger.scope(scope);
        }
        if (!conf.getEnablePerHostStats()) {
            return new PCBookieClientStatsLogger(underlyingLogger);
        }
        ConcurrentMap<InetSocketAddress, PCBookieClientStatsLogger> loggerMap = getLoggerMap(scope);
        PCBookieClientStatsLogger statsLogger = loggerMap.get(addr);
        if (null == statsLogger) {
            StringBuilder nameBuilder = new StringBuilder();
            nameBuilder.append(addr.getAddress().getHostAddress().replace('.', '_').replace('-', '_'))
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
