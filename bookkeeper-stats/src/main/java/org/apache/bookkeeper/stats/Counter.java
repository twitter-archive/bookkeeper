package org.apache.bookkeeper.stats;

/**
 * Simple stats that require only increment and decrement
 * functions on a Long. Metrics like the number of topics, persist queue size
 * etc. should use this.
 */
public interface Counter {
    /**
     * Clear this stat.
     */
    public void clear();

    /**
     * Increment the value associated with this stat.
     */
    public void inc();

    /**
     * Decrement the value associated with this stat.
     */
    public void dec();

    /**
     * Add delta to the value associated with this stat.
     * @param delta
     */
    public void add(long delta);

    /**
     * Get the value associated with this stat.
     */
    public Long get();
}
