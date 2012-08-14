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

package org.apache.bookkeeper.bookie;

import org.apache.commons.io.FileUtils;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.ZooKeeperUtil;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import static org.junit.Assert.*;

import static org.apache.bookkeeper.bookie.UpgradeTest.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CookieTest {
    ZooKeeperUtil zkutil;
    ZooKeeper zkc = null;

    @Before
    public void setupZooKeeper() throws Exception {
        zkutil = new ZooKeeperUtil();
        zkutil.startServer();
        zkc = zkutil.getZooKeeperClient();
    }

    @After
    public void tearDownZooKeeper() throws Exception {
        zkutil.killServer();
    }

    private static String newDirectory() throws IOException {
        return newDirectory(true);
    }

    private static String newDirectory(boolean createCurDir) throws IOException {
        File d = File.createTempFile("bookie", "tmpdir");
        d.delete();
        d.mkdirs();
        if (createCurDir) {
            new File(d, "current").mkdirs();
        }
        return d.getPath();
    }

    /**
     * Test starting bookie with clean state.
     */
    @Test
    public void testCleanStart() throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory(false))
            .setLedgerDirNames(new String[] { newDirectory(false) })
            .setBookiePort(3181);
        try {
            Bookie b = new Bookie(conf);
        } catch (Exception e) {
            fail("Should not reach here.");
        }
    }

    /**
     * Test that if a zookeeper cookie
     * is different to a local cookie, the bookie
     * will fail to start
     */
    @Test
    public void testBadJournalCookie() throws Exception {
        ServerConfiguration conf1 = new ServerConfiguration()
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() })
            .setBookiePort(3181);
        Cookie c = Cookie.generateCookie(conf1);
        c.writeToZooKeeper(zkc, conf1);

        String journalDir = newDirectory();
        String ledgerDir = newDirectory();
        ServerConfiguration conf2 = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir })
            .setBookiePort(3181);
        Cookie c2 = Cookie.generateCookie(conf2);
        c2.writeToDirectory(new File(journalDir, "current"));
        c2.writeToDirectory(new File(ledgerDir, "current"));

        try {
            Bookie b = new Bookie(conf2);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a directory is removed from
     * the configuration, the bookie will fail to
     * start
     */
    @Test
    public void testDirectoryMissing() throws Exception {
        String[] ledgerDirs = new String[] {
            newDirectory(), newDirectory(), newDirectory() };
        String journalDir = newDirectory();
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(ledgerDirs)
            .setBookiePort(3181);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDirs[0], ledgerDirs[1] });
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(newDirectory()).setLedgerDirNames(ledgerDirs);
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setJournalDirName(journalDir);
        b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a directory is added to a
     * preexisting bookie, the bookie will fail
     * to start
     */
    @Test
    public void testDirectoryAdded() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 })
            .setBookiePort(3181);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setLedgerDirNames(new String[] { ledgerDir0, newDirectory() });
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }

        conf.setLedgerDirNames(new String[] { ledgerDir0 });
        b = new Bookie(conf);
        b.start();
        b.shutdown();
    }

    /**
     * Test that if a directory's contents
     * are emptied, the bookie will fail to start
     */
    @Test
    public void testDirectoryCleared() throws Exception {
        String ledgerDir0 = newDirectory();
        String journalDir = newDirectory();
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(journalDir)
            .setLedgerDirNames(new String[] { ledgerDir0 , newDirectory() })
            .setBookiePort(3181);

        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        FileUtils.deleteDirectory(new File(ledgerDir0));
        try {
            Bookie b2 = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie's port is changed
     * the bookie will fail to start
     */
    @Test
    public void testBookiePortChanged() throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(3181);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf.setBookiePort(3182);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie tries to start
     * with the address of a bookie which has already
     * existed in the system, then the bookie will fail
     * to start
     */
    @Test
    public void testNewBookieStartingWithAnotherBookiesPort() throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(3181);
        Bookie b = new Bookie(conf); // should work fine
        b.start();
        b.shutdown();

        conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newDirectory())
            .setLedgerDirNames(new String[] { newDirectory() , newDirectory() })
            .setBookiePort(3181);
        try {
            b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
        }
    }

    /**
     * Test that if a bookie is started with directories with
     * version 2 data, that it will fail to start (it needs upgrade)
     */
    @Test
    public void testV2data() throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newV2JournalDirectory())
            .setLedgerDirNames(new String[] { newV2LedgerDirectory() })
            .setBookiePort(3181);
        try {
            Bookie b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception", ice.getCause().getMessage().contains("upgrade needed"));
        }
    }

    /**
     * Test that if a bookie is started with directories with
     * version 1 data, that it will fail to start (it needs upgrade)
     */
    @Test
    public void testV1data() throws Exception {
        ServerConfiguration conf = new ServerConfiguration()
            .setZkServers(zkutil.getZooKeeperConnectString())
            .setJournalDirName(newV1JournalDirectory())
            .setLedgerDirNames(new String[] { newV1LedgerDirectory() })
            .setBookiePort(3181);
        try {
            Bookie b = new Bookie(conf);
            fail("Shouldn't have been able to start");
        } catch (BookieException.InvalidCookieException ice) {
            // correct behaviour
            assertTrue("wrong exception", ice.getCause().getMessage().contains("upgrade needed"));
        }
    }
}
