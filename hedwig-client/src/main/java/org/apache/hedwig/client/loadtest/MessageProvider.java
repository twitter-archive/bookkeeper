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
public abstract class MessageProvider {
    private LoadTestUtils ltUtil;
    private int messageSize;
    private List<ByteString> topicList;
    private ByteString retMessage;
    public MessageProvider(TopicProvider topicProvider, int messageSize,
                           LoadTestUtils ltUtil) {
        this.messageSize = messageSize;
        byte[] message = new byte[messageSize];
        new Random().nextBytes(message);
        this.retMessage = ByteString.copyFrom(message);
        this.topicList = topicProvider.getTopicList();
        this.ltUtil = ltUtil;
    }

    private ByteString getTopic() {
        return topicList.get(getDistribution().sample());
    }

    private LoadTestMessage getLoadTestMessage(int size) {
        LoadTestMessage message = LoadTestMessage.newBuilder()
                .setBody(this.retMessage)
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

    protected abstract IntegerDistribution getDistribution();
}
