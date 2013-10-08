package org.apache.bookkeeper.stats;

import java.util.*;

/**
 * A base class for all implementations of stats using twitter-common stats.
 */
public class BookkeeperStatsLogger {
    protected final StatsLogger underlying;
    private final Map<Enum, OpStatsLogger> opStatsLoggerMap = new HashMap<Enum, OpStatsLogger>();
    private final Map<Enum, Counter> counterMap = new HashMap<Enum, Counter>();
    private final Set<Enum> gauges = new HashSet<Enum>();

    public BookkeeperStatsLogger(StatsLogger statsLogger, Enum[] opStatsEnums,
                                 Enum[] counterEnums, Enum[] guageEnums) {
        this.underlying = statsLogger;
        if (null != opStatsEnums) {
            for (Enum e : opStatsEnums) {
                this.opStatsLoggerMap.put(e, underlying.getOpStatsLogger(getStatName(e)));
            }
        }
        if (null != counterEnums) {
            for (Enum e : counterEnums) {
                this.counterMap.put(e, underlying.getCounter(getStatName(e)));
            }
        }
        if (null != guageEnums) {
            Collections.addAll(this.gauges, guageEnums);
        }
    }

    public OpStatsLogger getOpStatsLogger(Enum e) {
        OpStatsLogger logger = this.opStatsLoggerMap.get(e);
        if (null == logger) {
            throw new RuntimeException("OpStat " + e.name() + " is not supported by this logger");
        }
        return logger;
    }

    public <T extends Number> void registerGauge(Enum e, Gauge<T> gauge) {
        if (!gauges.contains(e)) {
            throw new RuntimeException("Gauge is not supported by this logger");
        }
        underlying.registerGauge(getStatName(e), gauge);
    }

    public Counter getCounter(Enum e) {
        Counter counter = this.counterMap.get(e);
        if (null == counter) {
            throw new RuntimeException("Counter " + e.name() + " is not supported by this logger");
        }
        return counter;
    }

    public void clear() {
        for (Counter counter : this.counterMap.values()) {
            counter.clear();
        }
        for (OpStatsLogger statLogger : this.opStatsLoggerMap.values()) {
            statLogger.clear();
        }
    }

    private String getStatName(Enum type) {
        return type.name();
    }
}
