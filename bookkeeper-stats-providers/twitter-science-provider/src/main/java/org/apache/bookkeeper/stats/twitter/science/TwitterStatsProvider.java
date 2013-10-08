package org.apache.bookkeeper.stats.twitter.science;

import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterStatsProvider implements StatsProvider {

    static final Logger LOG = LoggerFactory.getLogger(TwitterStatsProvider.class);

    protected final static String STATS_EXPORT = "statsExport";
    protected final static String STATS_HTTP_PORT = "statsHttpPort";

    private HTTPStatsExporter statsExporter = null;

    @Override
    public void start(Configuration conf) {
        if (conf.getBoolean(STATS_EXPORT, false)) {
            statsExporter = new HTTPStatsExporter(conf.getInt(STATS_HTTP_PORT, 9002));
        }
        if (null != statsExporter) {
            try {
                statsExporter.start();
            } catch (Exception e) {
                LOG.error("Fail to start stats exporter : ", e);
            }
        }
    }

    @Override
    public void stop() {
        if (null != statsExporter) {
            try {
                statsExporter.stop();
            } catch (Exception e) {
                LOG.error("Fail to stop stats exporter : ", e);
            }
        }
    }

    @Override
    public StatsLogger getStatsLogger(String name) {
        return new TwitterStatsLoggerImpl(name);
    }
}
