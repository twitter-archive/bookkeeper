package org.apache.bookkeeper.stats.twitter.ostrich;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import scala.Function0;
import scala.runtime.AbstractFunction0;

/**
 * Implementation of ostrich logger.
 */
class OstrichStatsLoggerImpl implements StatsLogger {

    protected final String scope;
    protected final com.twitter.ostrich.stats.StatsProvider ostrichProvider;

    OstrichStatsLoggerImpl(String scope, com.twitter.ostrich.stats.StatsProvider ostrichProvider) {
        this.scope = scope;
        this.ostrichProvider = ostrichProvider;
    }

    @Override
    public OpStatsLogger getOpStatsLogger(String statName) {
        return new OpStatsLoggerImpl(getStatName(statName), ostrichProvider);
    }

    @Override
    public Counter getCounter(String statName) {
        return new CounterImpl(ostrichProvider.getCounter(getStatName(statName)));
    }

    @Override
    public <T extends Number> void registerGauge(final String statName, final Gauge<T> gauge) {
        Function0<Object> gaugeFunc = new AbstractFunction0<Object>() {
            @Override
            public Object apply() {
                return gauge.getSample().doubleValue();
            }
        };
        ostrichProvider.addGauge(getStatName(statName), gaugeFunc);
    }

    private String getStatName(String statName) {
        return String.format("%s/%s", scope, statName);
    }

    @Override
    public StatsLogger scope(String scope) {
        return new OstrichStatsLoggerImpl(getStatName(scope), ostrichProvider);
    }

}
