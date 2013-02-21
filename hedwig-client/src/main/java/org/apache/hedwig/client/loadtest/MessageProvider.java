package org.apache.hedwig.client.loadtest;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.LoadTest.MessageProviderValue;
import org.apache.hedwig.protocol.LoadTest.LoadTestMessage;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;

import java.util.List;
import java.util.Random;

/**
 * Provides a message that should be published to a particular
 * topic depending on the desired distribution. This should perhaps
 * be an interface in future.
 *
 * For now, this uses UniformIntegerDistribution
 */
public class MessageProvider {
    private LoadTestUtils ltUtil;
    private int messageSize;
    private IntegerDistribution distribution;
    private Random rGen = new Random();
    private List<ByteString> topicList;
    public MessageProvider(TopicProvider topicProvider, int messageSize,
                           LoadTestUtils ltUtil) {
        this.messageSize = messageSize;
        int numTopics = topicProvider.numTopics();
        if (numTopics > 1) {
            this.distribution = new UniformIntegerDistribution(0,
                    topicProvider.numTopics() - 1);
        } else {
            // This is a hack because UniformIntegerDistribution doesn't support
            // (0, 0)
            this.distribution = new UniformIntegerDistribution(0, 2) {
                @Override
                public int sample() {
                    return 0;
                }
            };
        }
        this.topicList = topicProvider.getTopicList();
        this.ltUtil = ltUtil;
    }

    private ByteString getTopic() {
        return topicList.get(distribution.sample());
    }

    private LoadTestMessage getLoadTestMessage(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append(rGen.nextInt(26) + 'a');
        }
        ByteString retMessage = ByteString.copyFromUtf8(sb.toString());
        LoadTestMessage message = LoadTestMessage.newBuilder()
                .setBody(retMessage)
                .setTimestamp(System.currentTimeMillis())
                .build();
        return message;
    }

    public MessageProviderValue getMessage() {
        MessageProviderValue message = MessageProviderValue.newBuilder()
                .setTopic(getTopic())
                .setMessage(getLoadTestMessage(messageSize))
                .build();
        return message;
    }
}
