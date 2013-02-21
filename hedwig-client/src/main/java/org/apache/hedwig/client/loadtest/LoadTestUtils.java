package org.apache.hedwig.client.loadtest;

import com.google.protobuf.ByteString;
import org.apache.commons.cli.CommandLine;

public class LoadTestUtils {
    private String topicPrefix;
    private String subscriberPrefix;
    public LoadTestUtils(String topicPrefix, String subscriberPrefix) {
        this.topicPrefix = topicPrefix;
        this.subscriberPrefix = subscriberPrefix;
    }

    public ByteString getTopicFromNumber(int topicNum) {
        StringBuilder sb = new StringBuilder()
                .append(topicPrefix)
                .append("-")
                .append(topicNum);
        return ByteString.copyFromUtf8(sb.toString());
    }

    public ByteString getSubscriberFromNumber(int subNum) {
        StringBuilder sb = new StringBuilder()
                .append(subscriberPrefix)
                .append("-")
                .append(subNum);
        return ByteString.copyFromUtf8(sb.toString());
    }
}
