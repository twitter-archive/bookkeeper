package org.apache.hedwig.client.loadtest;

import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.Stats;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class LoadTestBase {
    private static Logger logger = LoggerFactory.getLogger(LoadTestBase.class);
    protected TopicProvider topicProvider;
    protected LoadTestUtils ltUtil;
    protected RequestStats stat;
    protected ClientConfiguration conf;
    public LoadTestBase(TopicProvider topicProvider, LoadTestUtils
                        ltUtil, ClientConfiguration conf, String statName) {
        this.topicProvider = topicProvider;
        this.ltUtil = ltUtil;
        this.conf = conf;
        this.stat = new RequestStats(statName);
    }

    public String getStats() {
        // TODO: Print only the required stats.
        StringBuilder sb = new StringBuilder();
        for (Stat s : Stats.getVariables()) {
            sb.append(s.getName())
                    .append(" : ")
                    .append(s.read())
                    .append("\n");
        }
        return sb.toString();
    }

    public abstract void start();
    public abstract void stop();
}
