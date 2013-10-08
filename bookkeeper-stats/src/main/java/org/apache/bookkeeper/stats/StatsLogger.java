package org.apache.bookkeeper.stats;

/**
 * A simple interface that exposes just 2 useful methods. One to get the logger for an Op stat
 * and another to get the logger for a simple stat
 */
public interface StatsLogger {
    /**
     * @param name
     *          Stats Name
     * @return Get the logger for an OpStat described by the <i>name</i>.
     */
    public OpStatsLogger getOpStatsLogger(String name);

    /**
     * @param name
     *          Stats Name
     * @return Get the logger for a simple stat described by the <i>name</i>
     */
    public Counter getCounter(String name);

    /**
     * Register given <i>guage</i> as name <i>name</i>.
     *
     * @param name
     *          gauge name
     */
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge);

    /**
     * Provide the stats logger under scope <i>name</i>.
     *
     * @param name
     *          scope name.
     * @return stats logger under scope <i>name</i>.
     */
    public StatsLogger scope(String name);

    /**
     * Clear state
     */
    public void clear();
}
