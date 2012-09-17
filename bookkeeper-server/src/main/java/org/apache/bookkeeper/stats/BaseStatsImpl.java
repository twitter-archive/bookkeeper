package org.apache.bookkeeper.stats;

import java.util.HashMap;
import java.util.Map;

/**
 * A base class for all implementations of stats using twitter-common stats.
 */
public class BaseStatsImpl implements StatsLogger {
    protected final String name;
    private final Map<Enum, OpStatsLogger> opStatsLoggerMap = new HashMap<Enum, OpStatsLogger>();
    private final Map<Enum, SimpleStat> simpleStatLoggerMap = new HashMap<Enum, SimpleStat>();

    public BaseStatsImpl(String name, Enum[] opStatsEnums, Enum[] simpleStatEnums) {
        this.name = name;
        if (null != opStatsEnums) {
            for (Enum e : opStatsEnums) {
                this.opStatsLoggerMap.put(e, new OpStatsLoggerImpl(this.name + "_" + e.name().toLowerCase()));
            }
        }
        if (null != simpleStatEnums) {
            for (Enum e : simpleStatEnums) {
                this.simpleStatLoggerMap.put(e, new SimpleStatImpl(this.name + "_" + e.name().toLowerCase()));
            }
        }
    }

    @Override
    public OpStatsLogger getOpStatsLogger(Enum e) {
        OpStatsLogger logger = this.opStatsLoggerMap.get(e);
        if (null == logger) {
            throw new RuntimeException("OpStat " + e.name() + " is not supported by this logger");
        }
        return logger;
    }

    @Override
    public SimpleStat getSimpleStatLogger(Enum e) {
        SimpleStat logger = this.simpleStatLoggerMap.get(e);
        if (null == logger) {
            throw new RuntimeException("SimpleStat " + e.name() + " is not supported by this logger");
        }
        return logger;
    }

    @Override
    public void clear() {
        for (SimpleStat statLogger : this.simpleStatLoggerMap.values()) {
            statLogger.clear();
        }
        for (OpStatsLogger statLogger : this.opStatsLoggerMap.values()) {
            statLogger.clear();
        }
    }
}
