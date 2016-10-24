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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.CheckpointSource.Checkpoint;
import org.apache.bookkeeper.client.BookKeeperTestClient;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.proto.ReadOnlyBookieServer;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.Auditor;
import org.apache.bookkeeper.replication.ReplicationException.CompatibilityException;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.util.BookKeeperConstants.*;

/**
 * A class runs several bookie servers for testing.
 */
public abstract class BookKeeperClusterTestCase extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(BookKeeperClusterTestCase.class);

    // ZooKeeper related variables
    protected ZooKeeperUtil zkUtil = new ZooKeeperUtil();
    protected ZooKeeper zkc;

    // BookKeeper related variables
    protected List<File> tmpDirs = new LinkedList<File>();
    protected List<BookieServer> bs = new LinkedList<BookieServer>();
    protected List<ServerConfiguration> bsConfs = new LinkedList<ServerConfiguration>();
    protected int numBookies;
    protected BookKeeperTestClient bkc;

    protected ServerConfiguration baseConf = TestBKConfiguration.newServerConfiguration();
    protected ClientConfiguration baseClientConf = TestBKConfiguration.newClientConfiguration();

    private Map<BookieServer, AutoRecoveryMain> autoRecoveryProcesses = new HashMap<BookieServer, AutoRecoveryMain>();

    private boolean isAutoRecoveryEnabled;

    public BookKeeperClusterTestCase(int numBookies) {
        this.numBookies = numBookies;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        LOG.info("Setting up test {}", getName());
        try {
            // start zookeeper service
            startZKCluster();
            // start bookkeeper service
            startBKCluster();
        } catch (Exception e) {
            LOG.error("Error setting up", e);
            throw e;
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        LOG.info("TearDown");
        // stop bookkeeper service
        stopBKCluster();
        // stop zookeeper service
        stopZKCluster();
        // cleanup temp dirs
        cleanupTempDirs();
        LOG.info("Tearing down test {}", getName());
    }

    protected File createTempDir(String prefix, String suffix) throws IOException {
        File dir = IOUtils.createTempDir(prefix, suffix + getName());
        tmpDirs.add(dir);
        return dir;
    }

    /**
     * Start zookeeper cluster
     *
     * @throws Exception
     */
    protected void startZKCluster() throws Exception {
        zkUtil.startServer();
        zkc = zkUtil.getZooKeeperClient();
    }

    /**
     * Stop zookeeper cluster
     *
     * @throws Exception
     */
    protected void stopZKCluster() throws Exception {
        zkUtil.killServer();
    }

    /**
     * Start cluster. Also, starts the auto recovery process for each bookie, if
     * isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void startBKCluster() throws Exception {
        baseClientConf.setZkServers(zkUtil.getZooKeeperConnectString());
        if (numBookies > 0) {
            bkc = new BookKeeperTestClient(baseClientConf);
        }

        // Create Bookie Servers (B1, B2, B3)
        for (int i = 0; i < numBookies; i++) {
            startNewBookie();
        }
    }

    /**
     * Stop cluster. Also, stops all the auto recovery processes for the bookie
     * cluster, if isAutoRecoveryEnabled is true.
     *
     * @throws Exception
     */
    protected void stopBKCluster() throws Exception {
        if (bkc != null) {
            bkc.close();
        }

        for (BookieServer server : bs) {
            server.shutdown();
            AutoRecoveryMain autoRecovery = autoRecoveryProcesses.get(server);
            if (autoRecovery != null && isAutoRecoveryEnabled()) {
                autoRecovery.shutdown();
                LOG.debug("Shutdown auto recovery for bookieserver:"
                        + server.getLocalAddress());
            }
        }
        bs.clear();
    }

    protected void cleanupTempDirs() throws Exception {
        for (File f : tmpDirs) {
            FileUtils.deleteDirectory(f);
        }
    }

    protected ServerConfiguration newServerConfiguration() throws IOException {
        File f = createTempDir("bookie", getName());

        int port = PortManager.nextFreePort();
        return newServerConfiguration(port, zkUtil.getZooKeeperConnectString(),
                                      f, new File[] { f });
    }

    protected ServerConfiguration newServerConfiguration(int port, String zkServers, File journalDir, File[] ledgerDirs) {
        ServerConfiguration conf = new ServerConfiguration(baseConf);
        conf.setBookiePort(port);
        conf.setZkServers(zkServers);
        conf.setJournalDirName(journalDir.getPath());
        String[] ledgerDirNames = new String[ledgerDirs.length];
        for (int i=0; i<ledgerDirs.length; i++) {
            ledgerDirNames[i] = ledgerDirs[i].getPath();
        }
        conf.setLedgerDirNames(ledgerDirNames);
        return conf;
    }

    protected void stopAllBookies() throws Exception {
        for (BookieServer server : bs) {
            server.shutdown();
        }
        bs.clear();
    }

    protected void startAllBookies() throws Exception {
        for (ServerConfiguration conf : bsConfs) {
            bs.add(startBookie(conf));
        }
    }

    /**
     * Get bookie address for bookie at index
     */
    public BookieSocketAddress getBookie(int index) throws IllegalArgumentException {
        if (bs.size() <= index || index < 0) {
            throw new IllegalArgumentException("Invalid index, there are only " + bs.size()
                                               + " bookies. Asked for " + index);
        }
        return bs.get(index).getLocalAddress();
    }

    /**
     * Kill a bookie by its socket address. Also, stops the autorecovery process
     * for the corresponding bookie server, if isAutoRecoveryEnabled is true.
     *
     * @param addr
     *            Socket Address
     * @return the configuration of killed bookie
     * @throws InterruptedException
     */
    public ServerConfiguration killBookie(BookieSocketAddress addr) throws InterruptedException {
        BookieServer toRemove = null;
        int toRemoveIndex = 0;
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.shutdown();
                toRemove = server;
                break;
            }
            ++toRemoveIndex;
        }
        if (toRemove != null) {
            stopAutoRecoveryService(toRemove);
            bs.remove(toRemove);
            return bsConfs.remove(toRemoveIndex);
        }
        return null;
    }

    /**
     * Set the bookie identified by its socket address to readonly
     *
     * @param addr
     *          Socket Address
     * @return the configuration of killed bookie
     * @throws InterruptedException
     */
    public void setBookieToReadOnly(BookieSocketAddress addr) throws InterruptedException {
        for (BookieServer server : bs) {
            if (server.getLocalAddress().equals(addr)) {
                server.getBookie().doTransitionToReadOnlyMode();
                break;
            }
        }
    }

    /**
     * Kill a bookie by index. Also, stops the respective auto recovery process
     * for this bookie, if isAutoRecoveryEnabled is true.
     *
     * @param index
     *            Bookie Index
     * @return the configuration of killed bookie
     * @throws InterruptedException
     * @throws IOException
     */
    public ServerConfiguration killBookie(int index) throws InterruptedException, IOException {
        if (index >= bs.size()) {
            throw new IOException("Bookie does not exist");
        }
        BookieServer server = bs.get(index);
        server.shutdown();
        stopAutoRecoveryService(server);
        bs.remove(server);
        return bsConfs.remove(index);
    }

    /**
     * Sleep a bookie
     *
     * @param addr
     *          Socket Address
     * @param seconds
     *          Sleep seconds
     * @return Count Down latch which will be counted down when sleep finishes
     * @throws InterruptedException
     * @throws IOException
     */
    public CountDownLatch sleepBookie(BookieSocketAddress addr, final int seconds)
            throws InterruptedException, IOException {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                final CountDownLatch l = new CountDownLatch(1);
                Thread sleeper = new Thread() {
                        @Override
                        public void run() {
                            try {
                                bookie.suspendProcessing();
                                l.countDown();
                                Thread.sleep(seconds*1000);
                                bookie.resumeProcessing();
                            } catch (Exception e) {
                                LOG.error("Error suspending bookie", e);
                            }
                        }
                    };
                sleeper.start();
                return l;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Sleep a bookie until I count down the latch
     *
     * @param addr
     *          Socket Address
     * @param l
     *          Latch to wait on
     * @throws InterruptedException
     * @throws IOException
     */
    public void sleepBookie(BookieSocketAddress addr, final CountDownLatch l)
            throws InterruptedException, IOException {
        final CountDownLatch suspendLatch = new CountDownLatch(1);
        sleepBookie(addr, l, suspendLatch);
        suspendLatch.await();
    }

    public void sleepBookie(BookieSocketAddress addr, final CountDownLatch l, final CountDownLatch suspendLatch)
            throws InterruptedException, IOException {
        for (final BookieServer bookie : bs) {
            if (bookie.getLocalAddress().equals(addr)) {
                LOG.info("Sleep bookie {}.", addr);
                Thread sleeper = new Thread() {
                    @Override
                    public void run() {
                        try {
                            bookie.suspendProcessing();
                            if (null != suspendLatch) {
                                suspendLatch.countDown();
                            }
                            l.await();
                            bookie.resumeProcessing();
                        } catch (Exception e) {
                            LOG.error("Error suspending bookie", e);
                        }
                    }
                };
                sleeper.start();
                return;
            }
        }
        throw new IOException("Bookie not found");
    }

    /**
     * Restart bookie servers. Also restarts all the respective auto recovery
     * process, if isAutoRecoveryEnabled is true.
     *
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies()
            throws InterruptedException, IOException, KeeperException, BookieException {
        restartBookies(null);
    }

    /**
     * Checkpoint all bookies
     */
    public void checkpointBookies() {
        for (BookieServer server : bs) {
            server.getBookie().getSyncThread().checkpoint(Checkpoint.MAX);
        }
    }


    /**
     * Restart bookie servers using new configuration settings. Also restart the
     * respective auto recovery process, if isAutoRecoveryEnabled is true.
     *
     * @param newConf
     *            New Configuration Settings
     * @throws InterruptedException
     * @throws IOException
     * @throws KeeperException
     * @throws BookieException
     */
    public void restartBookies(ServerConfiguration newConf)
        throws InterruptedException, IOException, KeeperException, BookieException {
        // shut down bookie server
        for (BookieServer server : bs) {
            server.shutdown();
            stopAutoRecoveryService(server);
        }
        bs.clear();
        Thread.sleep(1000);
        // restart them to ensure we can't
        int j = 0;
        for (ServerConfiguration conf : bsConfs) {
            if (null != newConf) {
                conf.loadConf(newConf);
            }
            bs.add(startBookie(conf));
            j++;
        }
    }

    /**
     * Helper method to startup a new bookie server with the indicated port
     * number. Also, starts the auto recovery process, if the
     * isAutoRecoveryEnabled is set true.
     *
     * @throws IOException
     */
    public int startNewBookie()
            throws IOException, InterruptedException, KeeperException, BookieException {
        ServerConfiguration conf = newServerConfiguration();
        bsConfs.add(conf);
        bs.add(startBookie(conf));

        return conf.getBookiePort();
    }

    /**
     * Helper method to startup a bookie server using a configuration object.
     * Also, starts the auto recovery process if isAutoRecoveryEnabled is true.
     *
     * @param conf
     *            Server Configuration Object
     *
     */
    protected BookieServer startBookie(ServerConfiguration conf)
            throws IOException, InterruptedException, KeeperException, BookieException {
        return startBookie(conf, false);
    }

    protected BookieServer startBookie(ServerConfiguration conf, boolean readOnly)
        throws IOException, InterruptedException, KeeperException, BookieException {
        BookieServer server;
        if (readOnly) {
            server = new ReadOnlyBookieServer(conf);
        } else {
            server = new BookieServer(conf);
        }
        server.start();

        int port = conf.getBookiePort();
        String zkHostPath;
        if (readOnly) {
            zkHostPath = "/ledgers/available/" + READONLY + "/"
                    + InetAddress.getLocalHost().getHostAddress() + ":" + port;
        } else {
            zkHostPath = "/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port;
        }
        while(bkc.getZkHandle().exists(zkHostPath, false) == null) {
            Thread.sleep(500);
        }

        bkc.readBookiesBlocking();
        LOG.info("New bookie (readonly = {}) on port {} has been created.", readOnly, port);

        try {
            startAutoRecovery(server, conf);
        } catch (CompatibilityException ce) {
            LOG.error("Exception while starting AutoRecovery!", ce);
        } catch (UnavailableException ue) {
            LOG.error("Exception while starting AutoRecovery!", ue);
        }
        return server;
    }

    /**
     * Start a bookie with the given bookie instance. Also, starts the auto
     * recovery for this bookie, if isAutoRecoveryEnabled is true.
     */
    protected BookieServer startBookie(ServerConfiguration conf, final Bookie b)
            throws IOException, InterruptedException, KeeperException, BookieException {
        BookieServer server = new BookieServer(conf) {
            @Override
            protected Bookie newBookie(ServerConfiguration conf) {
                return b;
            }
        };
        server.start();

        int port = conf.getBookiePort();
        while(bkc.getZkHandle().exists("/ledgers/available/" + InetAddress.getLocalHost().getHostAddress() + ":" + port, false) == null) {
            Thread.sleep(500);
        }

        bkc.readBookiesBlocking();
        LOG.info("New bookie on port " + port + " has been created.");
        try {
            startAutoRecovery(server, conf);
        } catch (CompatibilityException ce) {
            LOG.error("Exception while starting AutoRecovery!", ce);
        } catch (UnavailableException ue) {
            LOG.error("Exception while starting AutoRecovery!", ue);
        }
        return server;
    }

    /**
     * Flags used to enable/disable the auto recovery process. If it is enabled,
     * starting the bookie server will starts the auto recovery process for that
     * bookie. Also, stopping bookie will stops the respective auto recovery
     * process.
     *
     * @param isAutoRecoveryEnabled
     *            Value true will enable the auto recovery process. Value false
     *            will disable the auto recovery process
     */
    public void setAutoRecoveryEnabled(boolean isAutoRecoveryEnabled) {
        this.isAutoRecoveryEnabled = isAutoRecoveryEnabled;
    }

    /**
     * Flag used to check whether auto recovery process is enabled/disabled. By
     * default the flag is false.
     *
     * @return true, if the auto recovery is enabled. Otherwise return false.
     */
    public boolean isAutoRecoveryEnabled() {
        return isAutoRecoveryEnabled;
    }

    private void startAutoRecovery(BookieServer bserver,
            ServerConfiguration conf) throws CompatibilityException,
            KeeperException, InterruptedException, IOException,
            UnavailableException {
        if (isAutoRecoveryEnabled()) {
            AutoRecoveryMain autoRecoveryProcess = new AutoRecoveryMain(conf);
            autoRecoveryProcess.start();
            autoRecoveryProcesses.put(bserver, autoRecoveryProcess);
            LOG.debug("Starting Auditor Recovery for the bookie:"
                    + bserver.getLocalAddress());
        }
    }

    private void stopAutoRecoveryService(BookieServer toRemove) {
        AutoRecoveryMain autoRecoveryMain = autoRecoveryProcesses
                .remove(toRemove);
        if (null != autoRecoveryMain && isAutoRecoveryEnabled()) {
            autoRecoveryMain.shutdown();
            LOG.debug("Shutdown auto recovery for bookieserver:"
                    + toRemove.getLocalAddress());
        }
    }

    /**
     * Will starts the auto recovery process for the bookie servers. One auto
     * recovery process per each bookie server, if isAutoRecoveryEnabled is
     * enabled.
     *
     * @throws CompatibilityException
     *             - Compatibility error
     * @throws KeeperException
     *             - ZK exception
     * @throws InterruptedException
     *             - interrupted exception
     * @throws IOException
     *             - IOException
     * @throws UnavailableException
     *             - replication service has become unavailable
     */
    public void startReplicationService() throws CompatibilityException,
            KeeperException, InterruptedException, IOException,
            UnavailableException {
        int index = -1;
        for (BookieServer bserver : bs) {
            startAutoRecovery(bserver, bsConfs.get(++index));
        }
    }

    /**
     * Will stops all the auto recovery processes for the bookie cluster, if
     * isAutoRecoveryEnabled is true.
     */
    public void stopReplicationService() {
        if(false == isAutoRecoveryEnabled()){
            return;
        }
        for (Entry<BookieServer, AutoRecoveryMain> autoRecoveryProcess : autoRecoveryProcesses
                .entrySet()) {
            autoRecoveryProcess.getValue().shutdown();
            LOG.debug("Shutdown Auditor Recovery for the bookie:"
                    + autoRecoveryProcess.getKey().getLocalAddress());
        }
    }

    public Auditor getAuditor() throws Exception {
        for (AutoRecoveryMain p : autoRecoveryProcesses.values()) {
            Auditor a = p.getAuditor();
            if (a != null) {
                return a;
            }
        }
        throw new Exception("No auditor found");
    }
}
