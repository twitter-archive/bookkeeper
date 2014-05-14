package org.apache.bookkeeper.stats;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachingStatsLogger implements StatsLogger {

    protected final StatsLogger underlying;
    protected final ConcurrentMap<String, Counter> counters;
    protected final ConcurrentMap<String, OpStatsLogger> opStatsLoggers;
    protected final ConcurrentMap<String, StatsLogger> scopeStatsLoggers;

    public CachingStatsLogger(StatsLogger statsLogger) {
        this.underlying = statsLogger;
        this.counters = new ConcurrentHashMap<String, Counter>();
        this.opStatsLoggers = new ConcurrentHashMap<String, OpStatsLogger>();
        this.scopeStatsLoggers = new ConcurrentHashMap<String, StatsLogger>();
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        OpStatsLogger opStatsLogger = opStatsLoggers.get(name);
        if (null == opStatsLogger) {
            OpStatsLogger newOpStatsLogger = underlying.getOpStatsLogger(name);
            OpStatsLogger oldOpStatsLogger = opStatsLoggers.putIfAbsent(name, newOpStatsLogger);
            opStatsLogger = (null == oldOpStatsLogger) ? newOpStatsLogger : oldOpStatsLogger;
        }
        return opStatsLogger;
    }

    @Override
    public Counter getCounter(String name) {
        Counter counter = counters.get(name);
        if (null == counter) {
            Counter newCounter = underlying.getCounter(name);
            Counter oldCounter = counters.putIfAbsent(name, newCounter);
            counter = (null == oldCounter) ? newCounter : oldCounter;
        }
        return counter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        underlying.registerGauge(name, gauge);
    }

    @Override
    public StatsLogger scope(String name) {
        StatsLogger statsLogger = scopeStatsLoggers.get(name);
        if (null == statsLogger) {
            StatsLogger newStatsLogger = new CachingStatsLogger(underlying.scope(name));
            StatsLogger oldStatsLogger = scopeStatsLoggers.putIfAbsent(name, newStatsLogger);
            statsLogger = (null == oldStatsLogger) ? newStatsLogger : oldStatsLogger;
        }
        return statsLogger;
    }
}
