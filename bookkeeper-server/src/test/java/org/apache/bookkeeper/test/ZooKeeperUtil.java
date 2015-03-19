/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.test;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShimFactory;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.bookkeeper.zookeeper.ZooKeeperWatcherBase;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.test.ClientBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperUtil {
    static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtil.class);

    // ZooKeeper related variables
    protected final static Integer zooKeeperPort = PortManager.nextFreePort();

    protected ZooKeeper zkc; // zookeeper client
    protected ZooKeeperServerShim zks;
    protected File ZkTmpDir;
    private final String connectString;

    public ZooKeeperUtil() {
        connectString= "localhost:" + zooKeeperPort;
    }

    public ZooKeeper getZooKeeperClient() {
        return zkc;
    }

    public String getZooKeeperConnectString() {
        return connectString;
    }

    public void startServer() throws Exception {
        // create a ZooKeeper server(dataDir, dataLogDir, port)
        LOG.debug("Running ZK server");
        // ServerStats.registerAsConcrete();
        ClientBase.setupTestEnv();
        ZkTmpDir = IOUtils.createTempDir("zookeeper", "test");

        // start the server and client.
        restartServer();

        // initialize the zk client with values
        zkc.create("/ledgers", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zkc.create("/ledgers/available", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public void restartServer() throws Exception {
        zks = ZooKeeperServerShimFactory.createServer(ZkTmpDir, ZkTmpDir, zooKeeperPort, 100);
        zks.start();

        boolean b = ClientBase.waitForServerUp(getZooKeeperConnectString(),
                ClientBase.CONNECTION_TIMEOUT);
        LOG.debug("Server up: " + b);

        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        zkc = ZooKeeperClient.createConnectedZooKeeper(getZooKeeperConnectString(), 10000);
    }

    public void sleepServer(final int seconds, final CountDownLatch l)
            throws InterruptedException, IOException {
        Thread[] allthreads = new Thread[Thread.activeCount()];
        Thread.enumerate(allthreads);
        for (final Thread t : allthreads) {
            if (t.getName().contains("SyncThread:0")) {
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            t.suspend();
                            l.countDown();
                            Thread.sleep(seconds*1000);
                            t.resume();
                        } catch (Exception e) {
                            LOG.error("Error suspending thread", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("ZooKeeper thread not found");
    }

    public void expireSession(ZooKeeper zk) throws Exception {
        long id = zk.getSessionId();
        byte[] password = zk.getSessionPasswd();
        ZooKeeperWatcherBase w = new ZooKeeperWatcherBase(10000);
        ZooKeeper zk2 = new ZooKeeper(getZooKeeperConnectString(),
                zk.getSessionTimeout(), w, id, password);
        w.waitForConnection();
        zk2.close();
    }

    public void stopServer() throws Exception {
        if (zkc != null) {
            zkc.close();
        }

        // shutdown ZK server
        if (zks != null) {
            zks.stop();
        }
    }

    public void killServer() throws Exception {
        stopServer();
        // ServerStats.unregister();
        FileUtils.deleteDirectory(ZkTmpDir);
    }
}
