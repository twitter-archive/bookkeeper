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
package org.apache.hedwig.server.netty;

import java.net.InetSocketAddress;

import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import junit.framework.TestCase;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.server.common.ServerConfiguration;
import org.apache.hedwig.util.Callback;
import org.apache.zookeeper.ZooKeeper;
import org.apache.hedwig.zookeeper.ZkUtils;
import org.apache.hedwig.zookeeper.ZooKeeperTestBase;
import org.apache.hedwig.client.api.Subscriber;
import org.apache.hedwig.protocol.PubSubProtocol.SubscribeRequest;
import org.apache.hedwig.server.topics.TopicManager;
import org.apache.hedwig.server.topics.ZkTopicManager;
import org.apache.hedwig.server.regions.RegionManager;
import org.apache.hedwig.server.regions.HedwigHubClient;
import org.apache.hedwig.util.HedwigSocketAddress;

public class TestRegionSubscribe extends ZooKeeperTestBase {
    final int port = 4080;  // Local region port
    final ByteString topic = ByteString.copyFromUtf8("topic");
    final ByteString subscriberId = ByteString.copyFromUtf8("1");
    final String hubAdr = "localhost:4090";
    final HedwigSocketAddress hub = new HedwigSocketAddress(hubAdr);

    class ZkPubSubServer extends PubSubServer {
        public ZkPubSubServer(ServerConfiguration serverConfiguration, ZooKeeper zk) throws Exception {
            super(serverConfiguration);
            setZookeeperClient(zk);
        }

        @Override
        protected TopicManager instantiateTopicManager() throws java.io.IOException {
            TopicManager tm;

            try {
                tm = new ZkTopicManager(zk, conf, scheduler);
            } catch (PubSubException e) {
                logger.error("Could not instantiate Zk based topic manager", e);
                throw new java.io.IOException(e);
            }

            return tm;
        }
    }

    protected ZkPubSubServer server;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        server = new ZkPubSubServer(new ServerConfiguration() {
            @Override
            public int getServerPort() {
                return port;
            }

            @Override
            public boolean isStandalone() {
                return true;
            }

            @Override
            protected void refreshRegionList() {
                regionList = new java.util.LinkedList<String>();
                regionList.add(hubAdr);
            }
        }, zk);
        server.start();
        LOG.info("Zk PubSubServer test setup finished");
    }

    @Override
    @After
    public void tearDown() throws Exception {
        LOG.info("tearDown starting");
        server.shutdown();
        super.tearDown();
    }

    @Test
    public void testSubscribeAndPublish() throws Exception {
        // Mark topic subscribed at remote region
        server.getTopicManager().setTopicSubscribedFromRegion(topic, hubAdr, new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                }

                @Override
                public void operationFailed(Object ctx, org.apache.hedwig.exceptions.PubSubException exception) {
                    Assert.assertTrue("Topic subscription failed to be marked in zookeeper", false);
                }
        }, null);

        HedwigClient client = new HedwigClient(new ClientConfiguration());
        Subscriber subscriber = client.getSubscriber();

        // First subscribe (trigger remote region subscribe)
        subscriber.asyncSubscribe(topic, subscriberId, SubscribeRequest.CreateOrAttach.CREATE_OR_ATTACH, cb, null);
        Assert.assertTrue(queue.take());

        // Unsubscribe (trigger remote region unsubscribe)
        boolean unsubscribeSuccess = true;
        try {
            subscriber.unsubscribe(topic, subscriberId);
        } catch (Exception ex) {
            unsubscribeSuccess = false;
        }
        Assert.assertTrue(unsubscribeSuccess);

        // Check marker of topic-subscribed at remote region
        server.getTopicManager().checkTopicSubscribedFromRegion(topic, hubAdr, new Callback<Void>() {
                @Override
                public void operationFinished(Object ctx, Void resultOfOperation) {
                    Assert.assertTrue("Topic subscription is still marked in zookeeper", false);
                }

                @Override
                public void operationFailed(Object ctx, org.apache.hedwig.exceptions.PubSubException exception) {
                }
        }, null, null);

        client.close();
    }
}
