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
package org.apache.hedwig.server.integration;

import org.apache.hedwig.client.data.TopicSubscriber;
import org.apache.hedwig.server.regions.HedwigHubClient;
import org.apache.hedwig.server.regions.HedwigHubSubscriber;
import org.jboss.netty.channel.Channel;
import org.joor.Reflect;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hedwig.server.netty.PubSubServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.client.netty.CleanupChannelMap;
import org.apache.hedwig.client.netty.HChannel;
import org.apache.hedwig.client.netty.HChannelManager;
import org.apache.hedwig.client.netty.impl.multiplex.MultiplexHChannelManager;
import org.apache.hedwig.client.netty.impl.simple.SimpleHChannelManager;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest.CreateOrAttach;
import org.apache.hedwig.server.HedwigRegionTestBase;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.server.integration.TestHedwigHub.TestCallback;
import org.apache.hedwig.server.integration.TestHedwigHub.TestMessageHandler;
import org.apache.hedwig.util.HedwigSocketAddress;

@RunWith(Parameterized.class)
public class TestHedwigRegion extends HedwigRegionTestBase {

    // SynchronousQueues to verify async calls
    private final SynchronousQueue<Boolean> queue = new SynchronousQueue<Boolean>();
    private final SynchronousQueue<Boolean> consumeQueue = new SynchronousQueue<Boolean>();

    private static final int TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE = 3000;

    protected class NewRegionServerConfiguration extends RegionServerConfiguration {

        public NewRegionServerConfiguration(int serverPort, int sslServerPort,
                String regionName) {
            super(serverPort, sslServerPort, regionName);
        }

        @Override
        public int getRetryRemoteSubscribeThreadRunInterval() {
            return TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE;
        }

    }

    protected class NewRegionClientConfiguration extends ClientConfiguration {
        @Override
        public boolean isSubscriptionChannelSharingEnabled() {
            return isSubscriptionChannelSharingEnabled;
        }
        @Override
        public HedwigSocketAddress getDefaultServerHedwigSocketAddress() {
            return regionHubAddresses.get(0).get(0);
        }
    }

    protected ServerConfiguration getServerConfiguration(int serverPort, int sslServerPort, String regionName) {
        return new NewRegionServerConfiguration(serverPort, sslServerPort, regionName);
    }

    protected ClientConfiguration getRegionClientConfiguration() {
        return new NewRegionClientConfiguration();
    }

    @Parameters
    public static Collection<Object[]> configs() {
        return Arrays.asList(new Object[][] { { false }, { true } });
    }

    protected boolean isSubscriptionChannelSharingEnabled;

    public TestHedwigRegion(boolean isSubscriptionChannelSharingEnabled) {
        this.isSubscriptionChannelSharingEnabled = isSubscriptionChannelSharingEnabled;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        numRegions = 3;
        numServersPerRegion = 4;
        super.setUp();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Sets all channels in every alternate client in the server's RegionManager's client list
     * to the value indicated by setReadable.
     * @param server
     */
    public void setRegionManagerAlternateRegionClientsUnreadable(PubSubServer server, boolean setReadable) {
        try {
            List<HedwigHubClient> clients =  Reflect
                    .on(server)
                    .field("rm")
                    .get("clients");
            int md = 0;
            for (HedwigHubClient client : clients) {

                // Set every alternate region unreadable
                if (md++ % 2 == 1){
                    continue;
                }
                HChannelManager channelManager = client.getHChannelManager();
                if (channelManager instanceof SimpleHChannelManager) {
                    // JOOR doesn't let you access private members of a superclass. Use
                    // good ol' reflection instead
                    Field f = channelManager.getClass().getDeclaredField("topicSubscriber2Channel");
                    f.setAccessible(true);
                    CleanupChannelMap<TopicSubscriber> sub2channel = (CleanupChannelMap<TopicSubscriber>)f.get(channelManager);

                    for (HChannel c : sub2channel.getChannels()) {
                        c.getChannel().setReadable(setReadable);
                    }
                } else if (channelManager instanceof MultiplexHChannelManager) {
                    Field f = channelManager.getClass().getDeclaredField("subscriptionChannels");
                    f.setAccessible(true);
                    CleanupChannelMap<InetSocketAddress> subscriptionChannels =
                        (CleanupChannelMap<InetSocketAddress>)f.get(channelManager);

                    for (HChannel c : subscriptionChannels.getChannels()) {
                        c.getChannel().setReadable(setReadable);
                    }
                } else {
                    fail("Unknown channel manager : " + channelManager.getClass().getName());
                }
            }
        } catch (Exception e) {
            logger.error("Error while using reflections", e);
            fail("Failed to operate on subscription channels");
        }
    }

    /**
     * We set the link from region-0 to region-2 unreadable before we publish. We publish a message in region-0.
     * This message reaches the subscriber in region-0 and in region-1, but not in region-2. We then publish a message
     * in region-1. This reaches the subscribers in region-0 and region-2. When it reaches the subscriber in region-2,
     * the remote component should only contain region-1 and not region-0. This is checked in the TestMessageHandler.
     * Finally the last message in consumed and it should have both the IDs, and the region-0 remote component should be
     * 1.
     * TODO(Aniruddha): Add a more robust test case.
     */
    @Test
    public void testMultiRegionSequenceIdsDeterministic() throws Exception {
        // We need at least 3 regions for any meaningful test.
        assertTrue(numRegions >= 3);
        ByteString topic = ByteString.copyFromUtf8("Topic-0");
        List<HedwigClient> clientList = new ArrayList<HedwigClient>(regionClientsMap.values());

        // The regions are inserted in reverse order in the startUp function. For sanity we do the same
        // here. Set up local subscriptions.
        int subCount = 2;
        for (HedwigClient client : regionClientsMap.values()) {
            client.getSubscriber().subscribe(topic,
                    ByteString.copyFromUtf8("LocalSubscriber-"+subCount), CreateOrAttach.CREATE_OR_ATTACH);
            subCount--;
        }

        // Now start delivery.
        subCount = 2;
        for (HedwigClient client : regionClientsMap.values()) {
            client.getSubscriber().startDelivery(topic,
                    ByteString.copyFromUtf8("LocalSubscriber-"+subCount), new TestMessageHandler(consumeQueue));
            subCount--;
        }

        // Now, for region-2, we set the connection to region-0 unreadable.
        final List<PubSubServer> serverList = regionServersMap.values().iterator().next();
        for (PubSubServer server : serverList) {
            setRegionManagerAlternateRegionClientsUnreadable(server, false);
        }

        // Publish in region-0
        clientList.get(2).getPublisher().publish(topic,
                Message.newBuilder().setBody(ByteString.copyFromUtf8("Message-"+(0))).build());

        // Make sure that the first published message reaches the client in region-0 and in region-1
        // But not in region-2
        for (int i = 0; i < 2; i++) {
            assertTrue(consumeQueue.take());
        }
        assertTrue(consumeQueue.poll(1, TimeUnit.SECONDS) == null);

        // Publish in region-1
        clientList.get(1).getPublisher().publish(topic,
                Message.newBuilder().setBody(ByteString.copyFromUtf8("Message-"+(1))).build());

        // 3 messages should be consumed.
        for (int i = 0; i < 3; i++) {
            assertTrue(consumeQueue.take());
        }
        assertTrue(consumeQueue.poll(1, TimeUnit.SECONDS) == null);
        // Now set the channels readable on the first region. The last message should be consumed.
        for(PubSubServer server : serverList) {
            setRegionManagerAlternateRegionClientsUnreadable(server, true);
        }
        assertTrue(consumeQueue.take());
    }

    @Test
    public void testMultiRegionSubscribeAndConsume() throws Exception {
        int batchSize = 10;
        // Subscribe to topics for clients in all regions
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for the local subscribers in all regions
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }
        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.
        Publisher publisher = regionClientsMap.values().iterator().next().getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                       ByteString.copyFromUtf8("Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }

    /**
     * Test region shuts down when first subscription.
     *
     * @throws Exception
     */
    @Test
    public void testSubscribeAndConsumeWhenARegionDown() throws Exception {
        int batchSize = 10;

        // first shut down a region
        Random r = new Random();
        int regionId = r.nextInt(numRegions);
        stopRegion(regionId);
        // subscribe to topics when a region shuts down
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertFalse(queue.take());
            }
        }

        // start region gain
        startRegion(regionId);

        // sub it again
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for local subscribers in all regions
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }

        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.
        int rid = r.nextInt(numRegions);
        String regionName = REGION_PREFIX + rid;
        Publisher publisher = regionClientsMap.get(regionName).getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8(regionName + "-Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }

    /**
     * Test region shuts down when attaching existing subscriptions.
     *
     * @throws Exception
     */
    @Test
    public void testAttachExistingSubscriptionsWhenARegionDown() throws Exception {
        int batchSize = 10;
        
        // sub it remotely to make subscriptions existed
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // stop regions
        for (int i=0; i<numRegions; i++) {
            stopRegion(i);
        }
        // start regions again
        for (int i=0; i<numRegions; i++) {
            startRegion(i);
        }

        // first shut down a region
        Random r = new Random();
        int regionId = r.nextInt(numRegions);
        stopRegion(regionId);
        // subscribe to topics when a region shuts down
        // it should succeed since the subscriptions existed before
        for (HedwigClient client : regionClientsMap.values()) {
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                      ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                      new TestCallback(queue), null);
                assertTrue(queue.take());
            }
        }

        // Start delivery for local subscribers in all regions
        for (Map.Entry<String, HedwigClient> entry : regionClientsMap.entrySet()) {
            HedwigClient client = entry.getValue();
            for (int i = 0; i < batchSize; i++) {
                client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                                                     ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
            }
        }

        // start region again
        startRegion(regionId);
        // wait for retry
        Thread.sleep(3 * TEST_RETRY_REMOTE_SUBSCRIBE_INTERVAL_VALUE);

        String regionName = REGION_PREFIX + regionId;
        HedwigClient client = regionClientsMap.get(regionName);
        for (int i = 0; i < batchSize; i++) {
            client.getSubscriber().asyncSubscribe(ByteString.copyFromUtf8("Topic" + i),
                                                  ByteString.copyFromUtf8("LocalSubscriber"), CreateOrAttach.CREATE_OR_ATTACH,
                                                  new TestCallback(queue), null);
            assertTrue(queue.take());
            client.getSubscriber().startDelivery(ByteString.copyFromUtf8("Topic" + i),
                    ByteString.copyFromUtf8("LocalSubscriber"), new TestMessageHandler(consumeQueue));
        }

        // Now start publishing messages for the subscribed topics in one of the
        // regions and verify that it gets delivered and consumed in all of the
        // other ones.        
        Publisher publisher = client.getPublisher();
        for (int i = 0; i < batchSize; i++) {
            publisher.asyncPublish(ByteString.copyFromUtf8("Topic" + i), Message.newBuilder().setBody(
                                   ByteString.copyFromUtf8(regionName + "-Message" + i)).build(), new TestCallback(queue), null);
            assertTrue(queue.take());
        }
        // Make sure each region consumes the same set of published messages.
        for (int i = 0; i < regionClientsMap.size(); i++) {
            for (int j = 0; j < batchSize; j++) {
                assertTrue(consumeQueue.take());
            }
        }
    }
}
