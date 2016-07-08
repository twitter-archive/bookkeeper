package org.apache.bookkeeper.stats;

public class NullStatsLogger implements StatsLogger {

    public static final NullStatsLogger INSTANCE = new NullStatsLogger();

    static class NullOpStatsLogger implements OpStatsLogger {
        final OpStatsData nullOpStats = new OpStatsData(0, 0, 0, new long[6]);

        @Override
        public void registerFailedEvent(long eventLatencyMicros) {
            // nop
        }

        @Override
        public void registerSuccessfulEvent(long eventLatencyMicros) {
            // nop
        }

        @Override
        public OpStatsData toOpStatsData() {
            return nullOpStats;
        }

        @Override
        public void clear() {
            // nop
        }
    }
    static NullOpStatsLogger nullOpStatsLogger = new NullOpStatsLogger();

    static class NullCounter implements Counter {
        @Override
        public void clear() {
            // nop
        }

        @Override
        public void inc() {
            // nop
        }

        @Override
        public void dec() {
            // nop
        }

        @Override
        public void add(long delta) {
            // nop
        }

        @Override
        public Long get() {
            return 0L;
        }
    }
    static NullCounter nullCounter = new NullCounter();

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return nullOpStatsLogger;
    }

    @Override
    public Counter getCounter(String name) {
        return nullCounter;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        // nop
    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        // nop
    }

    @Override
    public StatsLogger scope(String name) {
        return this;
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {
        // nop
    }
}
