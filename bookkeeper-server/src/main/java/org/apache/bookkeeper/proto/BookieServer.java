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

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieCriticalThread;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ExitCode;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.jmx.BKMBeanRegistry;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.replication.AutoRecoveryMain;
import org.apache.bookkeeper.replication.ReplicationException;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.bookkeeper.replication.ReplicationStats.*;

/**
 * Implements the server-side part of the BookKeeper protocol.
 *
 */
public class BookieServer {
    static Logger LOG = LoggerFactory.getLogger(BookieServer.class);

    final ServerConfiguration conf;
    final BookieNettyServer nettyServer;
    private volatile boolean running = false;
    Bookie bookie;
    DeathWatcher deathWatcher;

    int exitCode = ExitCode.OK;

    // operation stats
    final private StatsProvider statsProvider;
    final BKStats bkStats = BKStats.getInstance();
    final boolean isStatsEnabled;
    protected BookieServerBean jmxBkServerBean;
    AutoRecoveryMain autoRecoveryMain = null;
    private boolean isAutoRecoveryDaemonEnabled;

    public BookieServer(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        this.conf = conf;
        this.statsProvider = ServerStatsProvider.initialize(conf);
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
        this.bookie = newBookie(conf);
        this.nettyServer = new BookieNettyServer(this.conf, this.bookie);
        this.bookie.initialize();

        isAutoRecoveryDaemonEnabled = conf.isAutoRecoveryDaemonEnabled();
        if (isAutoRecoveryDaemonEnabled) {
            try {
                this.autoRecoveryMain = new AutoRecoveryMain(conf, statsProvider.getStatsLogger(REPLICATION_SCOPE));
            } catch (ReplicationException.UnavailableException e) {
                throw new IOException("Failed to create auto recovery daemon : ", e);
            } catch (ReplicationException.CompatibilityException e) {
                throw new IOException("Failed to create auto recovery daemon : ", e);
            }
        }
    }

    protected Bookie newBookie(ServerConfiguration conf)
        throws IOException, KeeperException, InterruptedException, BookieException {
        return new Bookie(conf);
    }

    public void start() throws IOException {
        this.bookie.start();

        // fail fast, when bookie startup is not successful
        if (!this.bookie.isRunning()) {
            LOG.info("Bookie exit code : {}", bookie.getExitCode());
            exitCode = bookie.getExitCode();
            return;
        }
        if (isAutoRecoveryDaemonEnabled && this.autoRecoveryMain != null) {
            try {
                this.autoRecoveryMain.start();
            } catch (ReplicationException.UnavailableException e) {
                throw new IOException("Failed to start auto recovery daemon : ", e);
            }
        }

        this.nettyServer.start();

        // Start stats provider.
        statsProvider.start(conf);

        running = true;
        deathWatcher = new DeathWatcher(conf);
        deathWatcher.start();

        // register jmx
        registerJMX();
    }

    @VisibleForTesting
    public BookieSocketAddress getLocalAddress() {
        try {
            return Bookie.getBookieAddress(conf);
        } catch (UnknownHostException uhe) {
            InetSocketAddress localAddress = nettyServer.getLocalAddress();
            return new BookieSocketAddress(localAddress.getHostName(), localAddress.getPort());
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
        nettyServer.suspendProcessing();
    }

    /**
     * Resume processing requests in the bookie (for testing)
     */
    @VisibleForTesting
    public void resumeProcessing() {
        nettyServer.resumeProcessing();
    }

    public synchronized void shutdown() {
        if (!running) {
            return;
        }
        this.nettyServer.shutdown();

        // Stop stats exporter.
        statsProvider.stop();

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
        return bookie.isRunning() && nettyServer.isRunning() && running;
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
     * Whether auto-recovery service running with Bookie?
     *
     * @return true if auto-recovery service is running, otherwise return false
     */
    public boolean isAutoRecoveryRunning() {
        return this.autoRecoveryMain != null
                && this.autoRecoveryMain.isAutoRecoveryRunning();
    }

    public void join() throws InterruptedException {
        bookie.join();
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * A thread to watch whether bookie & nioserver is still alive
     */
    private class DeathWatcher extends BookieCriticalThread {

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
                if (!isBookieRunning()) {
                    shutdown();
                    break;
                }
                if (isAutoRecoveryDaemonEnabled && !isAutoRecoveryRunning()) {
                    LOG.error("Autorecovery daemon has stopped. Please check the logs");
                    isAutoRecoveryDaemonEnabled = false; // to avoid spamming the logs
                }
            }
            LOG.info("BookieDeathWatcher exited loop!");
        }
    }

    static final Options bkOpts = new Options();
    static {
        bkOpts.addOption("c", "conf", true, "Configuration for Bookie Server");
        bkOpts.addOption("r", "readonly", false, "Running Bookie Server in ReadOnly mode");
        bkOpts.addOption("withAutoRecovery", false,
                "Start Autorecovery service Bookie server");
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

    private static Pair<ServerConfiguration, CommandLine> parseArgs(String[] args)
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
                return Pair.of(conf, cmdLine);
            }

            if (cmdLine.hasOption("withAutoRecovery")) {
                conf.setAutoRecoveryDaemonEnabled(true);
            }

            if (cmdLine.hasOption("withAutoRecovery")) {
                conf.setAutoRecoveryDaemonEnabled(true);
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

            return Pair.of(conf, cmdLine);
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
        Pair<ServerConfiguration, CommandLine> confAndCmdLine = null;
        try {
            confAndCmdLine = parseArgs(args);
        } catch (IllegalArgumentException iae) {
            LOG.error("Error parsing command line arguments : ", iae);
            System.err.println(iae.getMessage());
            printUsage();
            System.exit(ExitCode.INVALID_CONF);
        }

        ServerConfiguration conf = confAndCmdLine.getLeft();
        boolean readOnly = confAndCmdLine.getRight().hasOption("r");

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
            final BookieServer bs;
            if (readOnly) {
                bs = new ReadOnlyBookieServer(conf);
            } else {
                bs = new BookieServer(conf);
            }
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
