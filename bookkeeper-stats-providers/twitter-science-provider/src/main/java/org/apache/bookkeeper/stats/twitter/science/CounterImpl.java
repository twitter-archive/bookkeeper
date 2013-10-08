package org.apache.bookkeeper.stats.twitter.science;

import com.twitter.common.stats.Rate;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.stats.Counter;

import java.util.concurrent.atomic.AtomicLong;
/**
 * This will export the value and the rate (per sec) to {@link org.apache.bookkeeper.stats.Stats}
 */
public class CounterImpl implements Counter {
    // The name used to export this stat
    private String name;
    private AtomicLong value;

    public CounterImpl(String name) {
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
