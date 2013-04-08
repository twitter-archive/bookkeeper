package org.apache.hedwig.client.loadtest;

import java.util.Random;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

public class RandomDistMessageProvider extends MessageProvider {
    private IntegerDistribution distribution;
    public RandomDistMessageProvider(final TopicProvider topicProvider, int messageSize,
                                      LoadTestUtils ltUtil) {
        super(topicProvider, messageSize, ltUtil);
        final Random rand = new Random();
        this.distribution = new UniformIntegerDistribution(0, 2) {
            @Override
            public int sample() {
                return rand.nextInt(topicProvider.numTopics());
            }
        };
    }

    protected IntegerDistribution getDistribution() {
        return this.distribution;
    }
}
