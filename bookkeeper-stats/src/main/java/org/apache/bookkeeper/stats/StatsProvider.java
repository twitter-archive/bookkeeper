package org.apache.bookkeeper.stats;

import org.apache.commons.configuration.Configuration;

/**
 * Provider to provide stats logger for different scopes.
 */
public interface StatsProvider {
    /**
     * Intialize the stats provider by loading the given configuration <i>conf</i>.
     *
     * @param conf
     *          Configuration to configure the stats provider.
     */
    public void start(Configuration conf);

    /**
     * Close the stats provider
     */
    public void stop();

    /**
     * Return the stats logger to a given <i>scope</i>
     * @param scope
     *          Scope for the given stats
     * @return stats logger for the given <i>scope</i>
     */
    public StatsLogger getStatsLogger(String scope);
}
