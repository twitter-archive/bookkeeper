/**
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
package org.apache.bookkeeper.bookie;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.BindException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.bookkeeper.proto.ReadOnlyBookieServer;
import org.jboss.netty.channel.ChannelException;
import junit.framework.Assert;

import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.DiskChecker.DiskErrorException;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.BOOKIE_STATUS_FILENAME;

/**
 * Testing bookie initialization cases
 */
public class BookieInitializationTest extends BookKeeperClusterTestCase {
    private static final Logger LOG = LoggerFactory
            .getLogger(BookieInitializationTest.class);

    ZooKeeper newzk = null;

    public BookieInitializationTest() {
        super(0);
    }

    @Override
    public void tearDown() throws Exception {
        if (null != newzk) {
            newzk.close();
        }
        super.tearDown();
    }

    private static class MockBookie extends Bookie {
        MockBookie(ServerConfiguration conf) throws IOException,
                KeeperException, InterruptedException, BookieException {
            super(conf);
        }

        void testRegisterBookie(ServerConfiguration conf) throws IOException {
            super.doRegisterBookie();
        }
    }

    /**
     * Verify the bookie server exit code. On ZooKeeper exception, should return
     * exit code ZK_REG_FAIL = 4
     */
    @Test(timeout = 20000)
    public void testExitCodeZK_REG_FAIL() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null)
                .setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        // simulating ZooKeeper exception by assigning a closed zk client to bk
        BookieServer bkServer = new BookieServer(conf) {
            protected Bookie newBookie(ServerConfiguration conf)
                    throws IOException, KeeperException, InterruptedException,
                    BookieException {
                return new MockBookie(conf);
            }
        };
        bkServer.getBookie().zk = zkc;
        zkc.close();

        bkServer.start();
        Assert.assertEquals("Failed to return ExitCode.ZK_REG_FAIL",
                ExitCode.ZK_REG_FAIL, bkServer.getExitCode());
    }

    @Test(timeout = 20000)
    public void testBookieRegistrationWithSameZooKeeperClient() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.initialize();
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                             zkc.exists(bkRegPath, false));

        // test register bookie again if the registeration node is created by itself.
        b.testRegisterBookie(conf);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                zkc.exists(bkRegPath, false));
    }

    /**
     * Verify the bookie reg. Restarting bookie server will wait for the session
     * timeout when previous reg node exists in zk. On zNode delete event,
     * should continue startup
     */
    @Test(timeout = 20000)
    public void testBookieRegistration() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(null).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });

        final String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.initialize();
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        createNewZKClient();
        b.zk = newzk;

        // deleting the znode, so that the bookie registration should
        // continue successfully on NodeDeleted event
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(conf.getZkTimeout() / 3);
                    zkc.delete(bkRegPath, -1);
                } catch (Exception e) {
                    // Not handling, since the testRegisterBookie will fail
                    LOG.error("Failed to delete the znode :" + bkRegPath, e);
                }
            }
        }.start();
        try {
            b.testRegisterBookie(conf);
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException) {
                KeeperException ke = (KeeperException) t;
                Assert.assertTrue("ErrorCode:" + ke.code()
                        + ", Registration node exists",
                        ke.code() != KeeperException.Code.NODEEXISTS);
            }
            throw e;
        }

        // verify ephemeral owner of the bkReg znode
        Stat bkRegNode2 = newzk.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration has been failed", bkRegNode2);
        Assert.assertTrue("Bookie is referring to old registration znode:"
                + bkRegNode1 + ", New ZNode:" + bkRegNode2, bkRegNode1
                .getEphemeralOwner() != bkRegNode2.getEphemeralOwner());
    }

    /**
     * Verify the bookie registration, it should throw
     * KeeperException.NodeExistsException if the znode still exists even after
     * the zk session timeout.
     */
    @Test(timeout = 30000)
    public void testRegNodeExistsAfterSessionTimeOut() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration().setZkServers(null)
                .setJournalDirName(tmpDir.getPath()).setLedgerDirNames(
                        new String[] { tmpDir.getPath() });

        String bkRegPath = conf.getZkAvailableBookiesPath() + "/"
                + InetAddress.getLocalHost().getHostAddress() + ":"
                + conf.getBookiePort();

        MockBookie b = new MockBookie(conf);
        b.initialize();
        b.zk = zkc;
        b.testRegisterBookie(conf);
        Stat bkRegNode1 = zkc.exists(bkRegPath, false);
        Assert.assertNotNull("Bookie registration node doesn't exists!",
                bkRegNode1);

        // simulating bookie restart, on restart bookie will create new
        // zkclient and doing the registration.
        createNewZKClient();
        b.zk = newzk;
        try {
            b.testRegisterBookie(conf);
            fail("Should throw NodeExistsException as the znode is not getting expired");
        } catch (IOException e) {
            Throwable t = e.getCause();
            if (t instanceof KeeperException) {
                KeeperException ke = (KeeperException) t;
                Assert.assertTrue("ErrorCode:" + ke.code()
                        + ", Registration node doesn't exists",
                        ke.code() == KeeperException.Code.NODEEXISTS);

                // verify ephemeral owner of the bkReg znode
                Stat bkRegNode2 = newzk.exists(bkRegPath, false);
                Assert.assertNotNull("Bookie registration has been failed",
                        bkRegNode2);
                Assert.assertTrue(
                        "Bookie wrongly registered. Old registration znode:"
                                + bkRegNode1 + ", New znode:" + bkRegNode2,
                        bkRegNode1.getEphemeralOwner() == bkRegNode2
                                .getEphemeralOwner());
                return;
            }
            throw e;
        }
    }

    /**
     * Verify duplicate bookie server startup. Should throw
     * java.net.BindException if already BK server is running
     */
    @Test(timeout = 20000)
    public void testDuplicateBookieServerStartup() throws Exception {
        File tmpDir = createTempDir("bookie", "test");

        ServerConfiguration conf = TestBKConfiguration.newServerConfiguration();
        int port = 12555;
        conf.setZkServers(null).setBookiePort(port).setJournalDirName(
                tmpDir.getPath()).setLedgerDirNames(
                new String[] { tmpDir.getPath() });
        BookieServer bs1 = new BookieServer(conf);
        bs1.start();
        BookieServer bs2 = null;
        // starting bk server with same conf
        try {
            bs2 = new BookieServer(conf);
            bs2.start();
            fail("Should throw BindException, as the bk server is already running!");
        } catch (ChannelException ce) {
            Assert.assertTrue("Should be caused by a bind exception",
                              ce.getCause() instanceof BindException);
            Assert.assertTrue("BKServer allowed duplicate startups!",
                    ce.getCause().getMessage().contains("Address already in use"));
        } finally {
            bs1.shutdown();
            if (bs2 != null) {
                bs2.shutdown();
            }
        }
    }

    /**
     * Verify bookie start behaviour when ZK Server is not running.
     */
    @Test(timeout = 20000)
    public void testStartBookieWithoutZKServer() throws Exception {
        zkUtil.killServer();

        File tmpDir = createTempDir("bookie", "test");

        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tmpDir.getPath())
                .setLedgerDirNames(new String[] { tmpDir.getPath() });
        try {
            Bookie b = new Bookie(conf);
            b.initialize();
            fail("Should throw ConnectionLossException as ZKServer is not running!");
        } catch (KeeperException.ConnectionLossException e) {
            // expected behaviour
        } finally {
            FileUtils.deleteDirectory(tmpDir);
        }
    }

    /**
     * Check disk full. Expected to throw NoWritableLedgerDirException
     * during bookie initialisation.
     */
    @Test(timeout = 30000)
    public void testWithDiskFull() throws Exception {
        File tempDir = createTempDir("DiskCheck", "test");

        long usableSpace = tempDir.getUsableSpace();
        long totalSpace = tempDir.getTotalSpace();
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(tempDir.getPath())
                .setLedgerDirNames(new String[] { tempDir.getPath() });
        conf.setDiskUsageThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.05f);
        conf.setDiskUsageWarnThreshold((1f - ((float) usableSpace / (float) totalSpace)) - 0.25f);
        try {
            new Bookie(conf).initialize();
            fail("Should fail with NoWritableLedgerDirException");
        } catch (NoWritableLedgerDirException nlde) {
            // expected
        }
    }

    /**
     * Check disk error for file. Expected to throw DiskErrorException.
     */
    @Test(timeout = 30000)
    public void testWithDiskError() throws Exception {
        File parent = createTempDir("DiskCheck", "test");

        File child = File.createTempFile("DiskCheck", "test", parent);
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
                .setZkServers(zkUtil.getZooKeeperConnectString())
                .setZkTimeout(5000).setJournalDirName(child.getPath())
                .setLedgerDirNames(new String[] { child.getPath() });
        // LedgerDirsManager#init() is used in Bookie instantiation.
        // Simulating disk errors by directly calling #init
        LedgerDirsManager ldm = new LedgerDirsManager(conf, conf.getLedgerDirs());
        try {
            ldm.checkAllDirs();
            fail("Should fail with DiskErrorException");
        } catch (DiskErrorException dee) {
            // expected
        }
    }

    /**
     * Check bookie status should be able to persist on disk and retrieve when restart the bookie.
     */
    @Test(timeout = 10000)
    public void testPersistBookieStatus() throws Exception {
        // enable persistent bookie status
        File tmpDir = createTempDir("bookie", "test");
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true);
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        // transition to readonly mode, bookie status should be persisted in ledger disks
        bookie.doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());

        // restart bookie should start in read only mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        // transition to writable mode
        bookie.doTransitionToWritableMode();
        // restart bookie should start in writable mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check when we start a ReadOnlyBookie, we should ignore bookie status
     */
    @Test(timeout = 10000)
    public void testReadOnlyBookieShouldIgnoreBookieStatus() throws Exception {
        File tmpDir = createTempDir("bookie", "test");
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(tmpDir.getPath())
            .setLedgerDirNames(new String[] { tmpDir.getPath() })
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true);
        // start new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        Bookie bookie = bookieServer.getBookie();
        // persist bookie status
        bookie.doTransitionToReadOnlyMode();
        bookie.doTransitionToWritableMode();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
        // start read only bookie
        bookieServer = new ReadOnlyBookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        // transition to writable should fail
        bookie.doTransitionToWritableMode();
        assertTrue(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check that if there's multiple bookie status copies, as long as not all of them are corrupted,
     * the bookie status should be retrievable.
     */
    public void testRetrieveBookieStatusWhenStatusFileIsCorrupted() throws Exception {
        File[] tmpLedgerDirs = new File[3];
        String[] filePath = new String[tmpLedgerDirs.length];
        for (int i = 0; i < tmpLedgerDirs.length; i++) {
            tmpLedgerDirs[i] = createTempDir("bookie", "test" + i);
            filePath[i] = tmpLedgerDirs[i].getPath();
        }
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(filePath[0])
            .setLedgerDirNames(filePath)
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true);
        // start a new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        // transition in to read only and persist the status on disk
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookie.doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());
        // corrupt status file
        List<File> ledgerDirs = bookie.getLedgerDirsManager().getAllLedgerDirs();
        corrupteFile(new File(ledgerDirs.get(0), BOOKIE_STATUS_FILENAME));
        corrupteFile(new File(ledgerDirs.get(1), BOOKIE_STATUS_FILENAME));
        // restart the bookie should be in read only mode
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertTrue(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    /**
     * Check if the bookie would read the latest status if the status files are not consistent.
     * @throws Exception
     */
    @Test(timeout = 10000)
    public void testReadLatestBookieStatus() throws Exception {
        File[] tmpLedgerDirs = new File[3];
        String[] filePath = new String[tmpLedgerDirs.length];
        for (int i = 0; i < tmpLedgerDirs.length; i++) {
            tmpLedgerDirs[i] = createTempDir("bookie", "test" + i);
            filePath[i] = tmpLedgerDirs[i].getPath();
        }
        final ServerConfiguration conf = TestBKConfiguration.newServerConfiguration()
            .setZkServers(zkUtil.getZooKeeperConnectString())
            .setJournalDirName(filePath[0])
            .setLedgerDirNames(filePath)
            .setReadOnlyModeEnabled(true)
            .setPersistBookieStatusEnabled(true);
        // start a new bookie
        BookieServer bookieServer = new BookieServer(conf);
        bookieServer.start();
        // transition in to read only and persist the status on disk
        Bookie bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookie.doTransitionToReadOnlyMode();
        assertTrue(bookie.isReadOnly());
        // Manually update a status file, so it becomes the latest
        Thread.sleep(1);
        BookieStatus status = new BookieStatus();
        List<File> dirs = new ArrayList<File>();
        dirs.add(bookie.getLedgerDirsManager().getAllLedgerDirs().get(0));
        status.writeToDirectories(dirs);
        // restart the bookie should start in writable state
        bookieServer.shutdown();
        bookieServer = new BookieServer(conf);
        bookieServer.start();
        bookie = bookieServer.getBookie();
        assertFalse(bookie.isReadOnly());
        bookieServer.shutdown();
    }

    private void corrupteFile(File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
            byte[] bytes = new byte[64];
            new Random().nextBytes(bytes);
            bw.write(new String(bytes));
        } finally {
            if (bw != null) {
                bw.close();
            }
            fos.close();
        }
    }


    private void createNewZKClient() throws Exception {
        // create a zookeeper client
        LOG.debug("Instantiate ZK Client");
        newzk = ZooKeeperClient.createConnectedZooKeeperClient(
                zkUtil.getZooKeeperConnectString(), 10000);
    }

}
