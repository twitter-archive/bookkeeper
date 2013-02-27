/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hedwig.protoextensions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.exceptions.PubSubException.UnexpectedConditionException;
import org.apache.hedwig.protocol.PubSubProtocol.MessageSeqId;
import org.apache.hedwig.protocol.PubSubProtocol.RegionSpecificSeqId;

public class MessageIdUtils {

    private static void appendComponents(StringBuilder sb, List<RegionSpecificSeqId> components) {
        for (RegionSpecificSeqId regionId : components) {
            sb.append(':');
            sb.append(regionId.getRegion().toStringUtf8());
            sb.append(':');
            sb.append(regionId.getSeqId());
        }
    }

    public static String msgIdToReadableString(MessageSeqId seqId) {
        StringBuilder sb = new StringBuilder();
        sb.append("local:");
        sb.append(seqId.getLocalComponent());
        sb.append(";region");
        appendComponents(sb, seqId.getRegionComponentsList());
        sb.append(";remote");
        appendComponents(sb, seqId.getRemoteComponentsList());
        return sb.toString();
    }

    public static Map<ByteString, RegionSpecificSeqId> inMapForm(List<RegionSpecificSeqId> components) {
        Map<ByteString, RegionSpecificSeqId> map = new HashMap<ByteString, RegionSpecificSeqId>();
        for (RegionSpecificSeqId lmsid : components) {
            map.put(lmsid.getRegion(), lmsid);
        }
        return map;
    }

    public static boolean areEqual(RegionSpecificSeqId seqId1, RegionSpecificSeqId seqId2) {
        return seqId1.getSeqId() == seqId2.getSeqId() &&
                seqId1.getRegion().equals(seqId2.getRegion());
    }

    public static boolean areEqual(List<RegionSpecificSeqId> list1,
                                 List<RegionSpecificSeqId> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }

        final Map<ByteString, RegionSpecificSeqId> map = inMapForm(list1);
        for (RegionSpecificSeqId lmsid1 : list2) {
            RegionSpecificSeqId lmsid2 = map.get(lmsid1.getRegion());
            if (lmsid2 == null) {
                return false;
            }
            if (lmsid1.getSeqId() != lmsid2.getSeqId()) {
                return false;
            }
        }

        return true;
    }

    public static boolean areEqual(MessageSeqId m1, MessageSeqId m2) {
        if (m1.getLocalComponent() != m2.getLocalComponent()) {
            return false;
        }

        return areEqual(m1.getRegionComponentsList(), m2.getRegionComponentsList()) &&
                areEqual(m1.getRemoteComponentsList(), m2.getRemoteComponentsList());
    }

    public static Message mergeLocalSeqId(Message.Builder messageBuilder, long localSeqId) {
        MessageSeqId.Builder msidBuilder = MessageSeqId.newBuilder(messageBuilder.getMsgId());
        msidBuilder.setLocalComponent(localSeqId);
        messageBuilder.setMsgId(msidBuilder);
        return messageBuilder.build();
    }

    public static Message mergeLocalSeqId(Message orginalMessage, long localSeqId) {
        return mergeLocalSeqId(Message.newBuilder(orginalMessage), localSeqId);
    }

    /**
     * Compares two seq numbers represented as lists of longs.
     *
     * @param l1
     * @param l2
     * @return 1 if the l1 is greater, 0 if they are equal, -1 if l2 is greater
     * @throws UnexpectedConditionException
     *             If the lists are of unequal length
     */
    public static int compare(List<Long> l1, List<Long> l2) throws UnexpectedConditionException {
        if (l1.size() != l2.size()) {
            throw new UnexpectedConditionException("Seq-ids being compared have different sizes: " + l1.size()
                                                   + " and " + l2.size());
        }

        for (int i = 0; i < l1.size(); i++) {
            long v1 = l1.get(i);
            long v2 = l2.get(i);

            if (v1 == v2) {
                continue;
            }

            return v1 > v2 ? 1 : -1;
        }

        // All components equal
        return 0;
    }

    /**
     * Build message locally generated such that its remote component is merged from remote components
     * and region components of lastPushedSeqId with local component removed, and its region component
     * just copy over from region components of lastPushedSeqId.
     */
    public static void buildMessageGenerated(MessageSeqId.Builder newIdBuilder, MessageSeqId lastSeqIdPushed,
                                             Message messageRequested) {
        assert !messageRequested.hasMsgId();    // Should not have msg-id set

        final ByteString localRegion = messageRequested.getSrcRegion();
        final Map<ByteString, RegionSpecificSeqId> map = new HashMap<ByteString, RegionSpecificSeqId>();

        // Add region vector to map and build region components
        for (RegionSpecificSeqId seqId : lastSeqIdPushed.getRegionComponentsList()) {
            newIdBuilder.addRegionComponents(seqId);
            map.put(seqId.getRegion(), seqId);
        }

        // Merge region and remote components
        for (RegionSpecificSeqId seqId : lastSeqIdPushed.getRemoteComponentsList()) {
            if (!localRegion.equals(seqId.getRegion())) {  // skip localRegion
                RegionSpecificSeqId seqId2 = map.get(seqId.getRegion());
                if (seqId2 == null || seqId2.getSeqId() < seqId.getSeqId()) {
                    map.put(seqId.getRegion(), seqId);
                }
            }
        }

        // Build remote components to indicate causal relation
        for (RegionSpecificSeqId seqId : map.values()) {
            newIdBuilder.addRemoteComponents(seqId);
        }
    }

    /**
     * Build message received from other region such that its remote component is copied from remote components
     * of message received, and its region component is merged from region components of lastPushedSeqId and
     * local component of message received.
     */
    public static void buildMessageReceived(MessageSeqId.Builder newIdBuilder,  MessageSeqId lastSeqIdPushed,
                                            Message messageRequested) {
        assert messageRequested.hasMsgId();    // Should have msg-id set

        final ByteString srcRegion = messageRequested.getSrcRegion();
        final RegionSpecificSeqId srcSeqId = RegionSpecificSeqId.newBuilder()
                    .setRegion(srcRegion)
                    .setSeqId(messageRequested.getMsgId().getLocalComponent())
                    .build();

        // Build region component by merging region components of lastPushedSeqId and local component
        for (RegionSpecificSeqId rsid : lastSeqIdPushed.getRegionComponentsList()) {
            if (!srcRegion.equals(rsid.getRegion())) {  // Skip srcRegion
                newIdBuilder.addRegionComponents(rsid);
            }
        }
        newIdBuilder.addRegionComponents(srcSeqId);   // add (srcRegion, local component)

        // Build remote components by copying from message received
        for (RegionSpecificSeqId rsid : messageRequested.getMsgId().getRemoteComponentsList()) {
            if (!srcRegion.equals(rsid.getRegion())) {  // skip srcRegion
                newIdBuilder.addRemoteComponents(rsid);
            }
        }
    }
}
