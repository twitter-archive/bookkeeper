package org.apache.hedwig.client.loadtest;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

public class UniformDistMessageProvider extends MessageProvider {

    private IntegerDistribution distribution;
    public UniformDistMessageProvider(TopicProvider topicProvider, int messageSize,
                           LoadTestUtils ltUtil) {
        super(topicProvider, messageSize, ltUtil);
        if (topicProvider.numTopics() > 1) {
            this.distribution = new UniformIntegerDistribution(0, topicProvider.numTopics() - 1);
        } else {
            // This is a hack because UniformIntegerDistribution doesn't support (0,0)
            this.distribution = new UniformIntegerDistribution(0, 2) {
                @Override
                public int sample() {
                    return 0;
                }
            };
        }
    }

    @Override
    protected IntegerDistribution getDistribution() {
        return distribution;
    }
}
