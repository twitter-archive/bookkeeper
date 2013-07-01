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
package org.apache.bookkeeper.proto;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.stats.HTTPStatsExporter;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer {
    final ServerConfiguration conf;
    NIOServerFactory nioServerFactory;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;

    private HTTPStatsExporter statsExporter;
    static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    int exitCode = ExitCode.OK;

    // operation stats
    final BKStats bkStats = BKStats.getInstance();
    final boolean isStatsEnabled;
    protected BookieServerBean jmxBkServerBean;

    public BookieServer(ServerConfiguration conf) 
            throws IOException, KeeperException, InterruptedException, BookieException {
        this.conf = conf;

        if (conf.getStatsExport()) {
            this.statsExporter = new HTTPStatsExporter(conf.getStatsHttpPort());
        } else {
            this.statsExporter = null;
        }
        isStatsEnabled = conf.isStatisticsEnabled();

        // Restart sequence
        // 1. First instantiate the server factory and bind to the port
        //    --- if using ephemeral ports this is where a port will be picked and
        //        used for rest of the startup
        // 2. Initialise the bookie - using the port that the connection bound to
        // 3. Set the packet processor in the server (this is only used after the server starts running
        // 4. Start the bookie - read the journal, replay, recover
        // 5. Start the server and accept connections
        //
        nioServerFactory = new NIOServerFactory(conf);
        this.bookie = newBookie(conf);
        nioServerFactory.setProcessor(new MultiPacketProcessor(this.conf, this.bookie));
    }

    protected Bookie newBookie(ServerConfiguration conf)
        throws IOException, KeeperException, InterruptedException, BookieException {
        return new Bookie(conf);
    }

    public void start() throws IOException {
        bookie.start();
        nioServerFactory.start();

        // Start stats exporter.
        try {
            if (null != statsExporter) {
                statsExporter.start();
            }
        } catch (Exception e) {
            LOG.error("Exception while starting stats exporter", e);
        }

        running = true;
        deathWatcher = new DeathWatcher(conf);
        deathWatcher.start();

        // register jmx
        registerJMX();
    }

    public InetSocketAddress getLocalAddress() {
        try {
            return Bookie.getBookieAddress(conf);
        } catch (UnknownHostException uhe) {
            return nioServerFactory.getLocalAddress();
        }
    }

    @VisibleForTesting
    public Bookie getBookie() {
        return bookie;
    }

    /**
     * Suspend processing of requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void suspendProcessing() {
        nioServerFactory.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void resumeProcessing() {
        nioServerFactory.resumeProcessing();
    }

    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        nioServerFactory.shutdown();

        // Stop stats exporter.
        try {
            if (null != statsExporter) {
                statsExporter.stop();
            }
        } catch (Exception e) {
            LOG.error("Exception while shutting down stats exporter.");
        }

        exitCode = bookie.shutdown();
        running = false;

        // unregister JMX
        unregisterJMX();
    }

    protected void registerJMX() {
        try {
            jmxBkServerBean = new BookieServerBean(conf, this);
            BKMBeanRegistry.getInstance().register(jmxBkServerBean, null);

            bookie.registerJMX(jmxBkServerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            jmxBkServerBean = null;
        }
    }

    protected void unregisterJMX() {
        try {
            bookie.unregisterJMX();
            if (jmxBkServerBean != null) {
                BKMBeanRegistry.getInstance().unregister(jmxBkServerBean);
            }
        } catch (Exception e) {
            LOG.warn("Failed to unregister with JMX", e);
        }
        jmxBkServerBean = null;
    }

    public boolean isRunning() {
        return bookie.isRunning() && nioServerFactory.isRunning() && running;
    }

    /**
     * Whether bookie is running?
     *
     * @return true if bookie is running, otherwise return false
     */
    public boolean isBookieRunning() {
        return bookie.isRunning();
    }

    /**
     * Whether nio server is running?
     *
     * @return true if nio server is running, otherwise return false
     */
    public boolean isNioServerRunning() {
        return nioServerFactory.isRunning();
    }

    public void join() throws InterruptedException {
        nioServerFactory.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    class DeathWatcher extends Thread {

        final int watchInterval;

        DeathWatcher(ServerConfiguration conf) {
            super("BookieDeathWatcher-" + conf.getBookiePort());
            watchInterval = conf.getDeathWatchInterval();
        }

        @Override
        public void run() {
            while(true) {
                try {
                    Thread.sleep(watchInterval);
                } catch (InterruptedException ie) {
                    // do nothing
                }
                if (!isBookieRunning() || !isNioServerRunning()) {
                    shutdown();
                    break;
                }
            }
        }
    }

    static final Options bkOpts = new Options();
    static {
        bkOpts.addOption("c", "conf", true, "Configuration for Bookie Server");
        bkOpts.addOption("h", "help", false, "Print help message");
    }

    /**
     * Print usage
     */
    private static void printUsage() {
        HelpFormatter hf = new HelpFormatter();
        hf.printHelp("BookieServer [options]\n\tor\n"
                   + "BookieServer <bookie_port> <zk_servers> <journal_dir> <ledger_dir [ledger_dir]>", bkOpts);
    }

    private static void loadConfFile(ServerConfiguration conf, String confFile)
        throws IllegalArgumentException {
        try {
            conf.loadConf(new File(confFile).toURI().toURL());
            conf.validate();
        } catch (MalformedURLException e) {
            LOG.error("Could not open configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        } catch (ConfigurationException e) {
            LOG.error("Malformed configuration file: " + confFile, e);
            throw new IllegalArgumentException();
        }
        LOG.info("Using configuration file " + confFile);
    }

    private static ServerConfiguration parseArgs(String[] args)
        throws IllegalArgumentException {
        try {
            BasicParser parser = new BasicParser();
            CommandLine cmdLine = parser.parse(bkOpts, args);

            if (cmdLine.hasOption('h')) {
                throw new IllegalArgumentException();
            }

            ServerConfiguration conf = new ServerConfiguration();
            String[] leftArgs = cmdLine.getArgs();

            if (cmdLine.hasOption('c')) {
                if (null != leftArgs && leftArgs.length > 0) {
                    throw new IllegalArgumentException();
                }
                String confFile = cmdLine.getOptionValue("c");
                loadConfFile(conf, confFile);
                return conf;
            }

            if (leftArgs.length < 4) {
                throw new IllegalArgumentException();
            }

            // command line arguments overwrite settings in configuration file
            conf.setBookiePort(Integer.parseInt(leftArgs[0]));
            conf.setZkServers(leftArgs[1]);
            conf.setJournalDirName(leftArgs[2]);
            String[] ledgerDirNames = new String[leftArgs.length - 3];
            System.arraycopy(leftArgs, 3, ledgerDirNames, 0, ledgerDirNames.length);
            conf.setLedgerDirNames(ledgerDirNames);

            return conf;
        } catch (ParseException e) {
            LOG.error("Error parsing command line arguments : ", e);
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) {
        ServerConfiguration conf = null;
        try {
            conf = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            LOG.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            System.exit(ExitCode.INVALID_CONF);
        }

        StringBuilder sb = new StringBuilder();
        String[] ledgerDirNames = conf.getLedgerDirNames();
        for (int i = 0; i < ledgerDirNames.length; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(ledgerDirNames[i]);
        }

        String hello = String.format(
                           "Hello, I'm your bookie, listening on port %1$s. ZKServers are on %2$s. Journals are in %3$s. Ledgers are stored in %4$s.",
                           conf.getBookiePort(), conf.getZkServers(),
                           conf.getJournalDirName(), sb);
        try {
            final BookieServer bs = new BookieServer(conf);
            bs.start();
            LOG.info(hello);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    bs.shutdown();
                    LOG.info("Shut down bookie server successfully");
                }
            });
            LOG.info("Register shutdown hook successfully");
            bs.join();

            System.exit(bs.getExitCode());
        } catch (Exception e) {
            LOG.error("Exception running bookie server : ", e);
            System.exit(ExitCode.SERVER_EXCEPTION);
        }
    }

}
