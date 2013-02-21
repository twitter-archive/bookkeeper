package org.apache.hedwig.client.loadtest;

import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

public class TopicProvider {
    private LoadTestUtils ltUtil;
    private int numTopics;
    private int topicStartIndex;
    public TopicProvider(int numTopics, int topicStartIndex,
                         LoadTestUtils ltUtil) {
        this.numTopics = numTopics;
        this.topicStartIndex = topicStartIndex;
        this.ltUtil = ltUtil;
    }
    public List<ByteString> getTopicList() {
        List<ByteString> topicList = new ArrayList<ByteString>();
        for (int i = topicStartIndex; i < topicStartIndex+numTopics; i++) {
            topicList.add(ltUtil.getTopicFromNumber(i));
        }
        return topicList;
    }

    public int numTopics() {
        return numTopics;
    }
}
