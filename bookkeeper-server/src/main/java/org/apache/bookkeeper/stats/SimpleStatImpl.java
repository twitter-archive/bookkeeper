package org.apache.bookkeeper.stats;

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Stats;

import java.util.concurrent.atomic.AtomicLong;
/**
 * This will export the value and the rate (per sec) to {@link Stats}
 */
public class SimpleStatImpl implements SimpleStat {
    // The name used to export this stat
    private String name;
    private AtomicLong value;

    public SimpleStatImpl(String name) {
        this.name = name;
        value = new AtomicLong(0);
        setUpStatsExport();
    }

    @Override
    public synchronized void clear() {
        value.getAndSet(0);
    }

    @Override
    public Long get() {
        return value.get();
    }

    private void setUpStatsExport() {
        // Export the value.
        Stats.export(name, value);
        // Export the rate of this value.
        Stats.export(Rate.of(name + "_per_sec", value).build());
    }

    @Override
    public void inc() {
        value.incrementAndGet();
    }

    @Override
    public void dec() {
        value.decrementAndGet();
    }

    @Override
    public void add(long delta) {
        value.addAndGet(delta);
    }
}
