package org.apache.bookkeeper.stats.twitter.science;

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

/**
 * Implementation of twitter-stats logger.
 */
public class TwitterStatsLoggerImpl implements StatsLogger {

    protected final String name;

    public TwitterStatsLoggerImpl(String name) {
        this.name = name;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String statName) {
        return new OpStatsLoggerImpl(getStatName(statName));
    }

    @Override
    public Counter getCounter(String statName) {
        return new CounterImpl(getStatName(statName));
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, final Gauge<T> gauge) {
        Stats.export(new SampledStat<Number>(getStatName(statName), gauge.getDefaultValue()) {
            @Override
            public T doSample() {
                return gauge.getSample();
            }
        });
    }

    private String getStatName(String statName) {
        return (name + "_" + statName).toLowerCase();
    }

    @Override
    public StatsLogger scope(String scope) {
        String scopeName;
        if (0 == name.length()) {
            scopeName = scope;
        } else {
            scopeName = name + "_" + scope;
        }
        return new TwitterStatsLoggerImpl(scopeName);
    }

    @Override
    public void clear() {
        // nop
    }
}
