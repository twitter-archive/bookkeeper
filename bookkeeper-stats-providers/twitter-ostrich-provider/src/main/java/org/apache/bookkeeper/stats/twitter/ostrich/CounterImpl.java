package org.apache.bookkeeper.stats.twitter.ostrich;

import org.apache.bookkeeper.stats.Counter;

class CounterImpl implements Counter {

    private final com.twitter.ostrich.stats.Counter ostrichCounter;

    CounterImpl(com.twitter.ostrich.stats.Counter ostrichCounter) {
        this.ostrichCounter = ostrichCounter;
    }

    @Override
    public void clear() {
        this.ostrichCounter.reset();
    }

    @Override
    public void inc() {
        this.ostrichCounter.incr();
    }

    @Override
    public void dec() {
        this.ostrichCounter.incr(-1);
    }

    @Override
    public void add(long delta) {
        this.ostrichCounter.incr((int)delta);
    }

    @Override
    public Long get() {
        return this.ostrichCounter.apply();
    }
}
