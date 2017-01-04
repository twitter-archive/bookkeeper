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

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.bookkeeper.bookie.EntryLogger.EntryLogScanner;
import org.apache.bookkeeper.bookie.Journal.JournalScanner;
import org.apache.bookkeeper.bookie.Journal.LastLogMark;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.LedgerUnderreplicationManager;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.EntryFormatter;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.Tool;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bookie Shell is to provide utilities for users to administer a bookkeeper cluster.
 */
public class BookieShell implements Tool {

    static final Logger LOG = LoggerFactory.getLogger(BookieShell.class);

    static final String ENTRY_FORMATTER_CLASS = "entryFormatterClass";

    static final String CMD_METAFORMAT = "metaformat";
    static final String CMD_BOOKIEFORMAT = "bookieformat";
    static final String CMD_RECOVER = "recover";
    static final String CMD_LEDGER = "ledger";
    static final String CMD_READLOG = "readlog";
    static final String CMD_READJOURNAL = "readjournal";
    static final String CMD_LASTMARK = "lastmark";
    static final String CMD_AUTORECOVERY = "autorecovery";
    static final String CMD_HELP = "help";

    final ServerConfiguration bkConf = new ServerConfiguration();
    File[] indexDirectories;
    File[] ledgerDirectories;
    File journalDirectory;

    EntryLogger entryLogger = null;
    Journal journal = null;
    EntryFormatter formatter;

    int pageSize;
    int entriesPerPage;

    interface Command {
        public int runCmd(String[] args) throws Exception;
        public void printUsage();
    }

    abstract class MyCommand implements Command {
        abstract Options getOptions();
        abstract String getDescription();
        abstract String getUsage();
        abstract int runCmd(CommandLine cmdLine) throws Exception;

        String cmdName;

        MyCommand(String cmdName) {
            this.cmdName = cmdName;
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            try {
                BasicParser parser = new BasicParser();
                CommandLine cmdLine = parser.parse(getOptions(), args);
                return runCmd(cmdLine);
            } catch (ParseException e) {
                LOG.error("Error parsing command line arguments : ", e);
                printUsage();
                return -1;
            }
        }

        @Override
        public void printUsage() {
            HelpFormatter hf = new HelpFormatter();
            System.err.println(cmdName + ": " + getDescription());
            hf.printHelp(getUsage(), getOptions());
        }
    }

    /**
     * Format the bookkeeper metadata present in zookeeper
     */
    class MetaFormatCmd extends MyCommand {
        Options opts = new Options();

        MetaFormatCmd() {
            super(CMD_METAFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format bookkeeper metadata in zookeeper";
        }

        @Override
        String getUsage() {
            return "metaformat [-nonInteractive] [-force]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            boolean result = BookKeeperAdmin.format(adminConf, interactive,
                    force);
            return (result) ? 0 : 1;
        }
    }

    /**
     * Formats the local data present in current bookie server
     */
    class BookieFormatCmd extends MyCommand {
        Options opts = new Options();

        public BookieFormatCmd() {
            super(CMD_BOOKIEFORMAT);
            opts.addOption("n", "nonInteractive", false,
                    "Whether to confirm if old data exists..?");
            opts.addOption("f", "force", false,
                    "If [nonInteractive] is specified, then whether"
                            + " to force delete the old data without prompt..?");
            opts.addOption("d", "removeCookie", false, "Remove its cookie on zookeeper");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Format the current server contents";
        }

        @Override
        String getUsage() {
            return "bookieformat [-nonInteractive] [-force] [-removeCookie]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean interactive = (!cmdLine.hasOption("n"));
            boolean force = cmdLine.hasOption("f");

            ServerConfiguration conf = new ServerConfiguration(bkConf);
            boolean result = Bookie.format(conf, interactive, force);
            // remove cookie
            if (cmdLine.hasOption("d")) {
                BookieSocketAddress address = Bookie.getBookieAddress(conf);
                ZooKeeperClient zkc =
                        ZooKeeperClient.createConnectedZooKeeperClient(conf.getZkServers(),
                                conf.getZkTimeout());
                try {
                    Cookie.removeCookieForBookie(conf, zkc, address);
                } catch (KeeperException.NoNodeException nne) {
                    // ignore no node exception
                    LOG.warn("No cookie to remove for {} : ", address, nne);
                } finally {
                    zkc.close();
                }
            }
            return (result) ? 0 : 1;
        }
    }

    /**
     * Recover command for ledger data recovery for failed bookie
     */
    class RecoverCmd extends MyCommand {
        Options opts = new Options();

        public RecoverCmd() {
            super(CMD_RECOVER);
            opts.addOption("q", "query", false, "Query the ledgers that contain given bookies");
            opts.addOption("dr", "dryrun", false, "Printing the recovery plan w/o doing actual recovery");
            opts.addOption("f", "force", false, "Force recovery without confirmation");
            opts.addOption("l", "ledger", true, "Recover a specific ledger");
            opts.addOption("sk", "skipOpenLedgers", false, "Skip recovering open ledgers");
            opts.addOption("d", "delete", false, "Delete cookie nodes for the bookies.");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Recover the ledger data for failed bookie";
        }

        @Override
        String getUsage() {
            return "recover <bookiesSrc>";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            String[] args = cmdLine.getArgs();
            if (args.length < 1) {
                throw new MissingArgumentException(
                        "'bookiesSrc' argument required");
            }
            boolean query = cmdLine.hasOption("q");
            boolean dryrun = cmdLine.hasOption("dr");
            boolean force = cmdLine.hasOption("f");
            boolean skipOpenLedgers = cmdLine.hasOption("sk");
            boolean removeCookies = cmdLine.hasOption("d");

            Long ledgerId = null;
            if (cmdLine.hasOption("l")) {
                try {
                    ledgerId = Long.parseLong(cmdLine.getOptionValue("l"));
                } catch (NumberFormatException nfe) {
                    throw new IOException("Invalid ledger id provided : " + cmdLine.getOptionValue("l"));
                }
            }

            // Get bookies list
            final String[] bookieStrs = args[0].split(",");
            final Set<BookieSocketAddress> bookieAddrs = new HashSet<BookieSocketAddress>();
            for (String bookieStr : bookieStrs) {
                final String bookieStrParts[] = bookieStr.split(":");
                if (bookieStrParts.length != 2) {
                    System.err.println("BookieSrcs has invalid bookie address format (host:port expected) : "
                            + bookieStr);
                    return -1;
                }
                bookieAddrs.add(new BookieSocketAddress(bookieStrParts[0],
                        Integer.parseInt(bookieStrParts[1])));
            }

            if (!force) {
                System.err.println("Bookies : " + bookieAddrs);
                if (!IOUtils.confirmPrompt("Are you sure to recover them : (Y/N)")) {
                    System.err.println("Give up!");
                    return -1;
                }
            }

            ClientConfiguration adminConf = new ClientConfiguration(bkConf);
            BookKeeperAdmin admin = new BookKeeperAdmin(adminConf);
            try {
                if (query) {
                    return bkQuery(admin, bookieAddrs);
                }
                if (null != ledgerId) {
                    return bkRecovery(admin, ledgerId, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
                }
                return bkRecovery(admin, bookieAddrs, dryrun, skipOpenLedgers, removeCookies);
            } finally {
                admin.close();
            }
        }

        private int bkQuery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs)
                throws InterruptedException, BKException {
            SortedMap<Long, LedgerMetadata> ledgersContainBookies =
                    bkAdmin.getLedgersContainBookies(bookieAddrs);
            System.err.println("NOTE: Bookies in inspection list are marked with '*'.");
            for (Map.Entry<Long, LedgerMetadata> ledger : ledgersContainBookies.entrySet()) {
                System.out.println("ledger " + ledger.getKey() + " : " + ledger.getValue().getState());
                Map<Long, Integer> numBookiesToReplacePerEnsemble =
                        inspectLedger(ledger.getValue(), bookieAddrs);
                System.out.print("summary: [");
                for (Map.Entry<Long, Integer> entry : numBookiesToReplacePerEnsemble.entrySet()) {
                    System.out.print(entry.getKey() + "=" + entry.getValue() + ", ");
                }
                System.out.println("]");
                System.out.println();
            }
            System.err.println("Done");
            return 0;
        }

        private Map<Long, Integer> inspectLedger(LedgerMetadata metadata, Set<BookieSocketAddress> bookiesToInspect) {
            Map<Long, Integer> numBookiesToReplacePerEnsemble = new TreeMap<Long, Integer>();
            for (Map.Entry<Long, ArrayList<BookieSocketAddress>> ensemble : metadata.getEnsembles().entrySet()) {
                ArrayList<BookieSocketAddress> bookieList = ensemble.getValue();
                System.out.print(ensemble.getKey() + ":\t");
                int numBookiesToReplace = 0;
                for (BookieSocketAddress bookie: bookieList) {
                    System.out.print(bookie);
                    if (bookiesToInspect.contains(bookie)) {
                        System.out.print("*");
                        ++numBookiesToReplace;
                    } else {
                        System.out.print(" ");
                    }
                    System.out.print(" ");
                }
                System.out.println();
                numBookiesToReplacePerEnsemble.put(ensemble.getKey(), numBookiesToReplace);
            }
            return numBookiesToReplacePerEnsemble;
        }

        private int bkRecovery(BookKeeperAdmin bkAdmin, long lid, Set<BookieSocketAddress> bookieAddrs,
                               boolean dryrun, boolean skipOpenLedgers, boolean removeCookies)
                throws InterruptedException, BKException, KeeperException {
            bkAdmin.recoverBookieData(lid, bookieAddrs, dryrun, skipOpenLedgers);
            if (removeCookies) {
                for (BookieSocketAddress addr : bookieAddrs) {
                    Cookie.removeCookieForBookie(bkConf, bkAdmin.getZooKeeper(), addr);
                }
            }
            return 0;
        }

        private int bkRecovery(BookKeeperAdmin bkAdmin, Set<BookieSocketAddress> bookieAddrs,
                               boolean dryrun, boolean skipOpenLedgers, boolean removeCookies)
                throws InterruptedException, BKException, KeeperException {
            bkAdmin.recoverBookieData(bookieAddrs, dryrun, skipOpenLedgers);
            if (removeCookies) {
                for (BookieSocketAddress addr : bookieAddrs) {
                    Cookie.removeCookieForBookie(bkConf, bkAdmin.getZooKeeper(), addr);
                }
            }
            return 0;
        }
    }

    /**
     * Ledger Command Handles ledger related operations
     */
    class LedgerCmd extends MyCommand {
        Options lOpts = new Options();

        LedgerCmd() {
            super(CMD_LEDGER);
            lOpts.addOption("m", "meta", false, "Print meta information");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing ledger id");
                printUsage();
                return -1;
            }

            boolean printMeta = false;
            if (cmdLine.hasOption("m")) {
                printMeta = true;
            }
            long ledgerId;
            try {
                ledgerId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                System.err.println("ERROR: invalid ledger id " + leftArgs[0]);
                printUsage();
                return -1;
            }
            if (printMeta) {
                // print meta
                readLedgerMeta(ledgerId);
            }
            // dump ledger info
            readLedgerIndexEntries(ledgerId);
            return 0;
        }

        @Override
        String getDescription() {
            return "Dump ledger index entries into readable format.";
        }

        @Override
        String getUsage() {
            return "ledger [-m] <ledger_id>";
        }

        @Override
        Options getOptions() {
            return lOpts;
        }
    }

    /**
     * Command to read entry log files.
     */
    class ReadLogCmd extends MyCommand {
        Options rlOpts = new Options();

        ReadLogCmd() {
            super(CMD_READLOG);
            rlOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing entry log id or entry log file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long logId;
            try {
                logId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a entry log id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".log")) {
                    // not a log file
                    System.err.println("ERROR: invalid entry log file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                logId = Long.parseLong(idString, 16);
            }
            Long ledgerId = null;
            Long entryId = null;
            Long position = null;
            if (leftArgs.length >= 4) {
                try {
                    ledgerId = Long.parseLong(leftArgs[1]);
                } catch (NumberFormatException nfe) {
                    System.err.println("ERROR: invalid ledger id : " + leftArgs[1]);
                    printUsage();
                    return -1;
                }
                try {
                    entryId = Long.parseLong(leftArgs[2]);
                } catch (NumberFormatException nfe) {
                    System.err.println("ERROR: invalid entry id : " + leftArgs[2]);
                    printUsage();
                    return -1;
                }
                try {
                    position = Long.parseLong(leftArgs[3]);
                } catch (NumberFormatException nfe) {
                    System.err.println("ERROR: invalid position : " + leftArgs[3]);
                    printUsage();
                    return -1;
                }
            }
            if (null == position) {
                // scan entry log
                scanEntryLog(logId, printMsg);
            } else {
                System.out.println("lid=" + ledgerId + ", eid=" + entryId + ", pos=" + position);
                readEntry(ledgerId, entryId, position, printMsg);
            }
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan an entry file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readlog [-m] <entry_log_id | entry_log_file_name> [ledgerId entryId position]";
        }

        @Override
        Options getOptions() {
            return rlOpts;
        }
    }

    /**
     * Command to read journal files
     */
    class ReadJournalCmd extends MyCommand {
        Options rjOpts = new Options();

        ReadJournalCmd() {
            super(CMD_READJOURNAL);
            rjOpts.addOption("m", "msg", false, "Print message body");
        }

        @Override
        public int runCmd(CommandLine cmdLine) throws Exception {
            String[] leftArgs = cmdLine.getArgs();
            if (leftArgs.length <= 0) {
                System.err.println("ERROR: missing journal id or journal file name");
                printUsage();
                return -1;
            }

            boolean printMsg = false;
            if (cmdLine.hasOption("m")) {
                printMsg = true;
            }
            long journalId;
            try {
                journalId = Long.parseLong(leftArgs[0]);
            } catch (NumberFormatException nfe) {
                // not a journal id
                File f = new File(leftArgs[0]);
                String name = f.getName();
                if (!name.endsWith(".txn")) {
                    // not a journal file
                    System.err.println("ERROR: invalid journal file name " + leftArgs[0]);
                    printUsage();
                    return -1;
                }
                String idString = name.split("\\.")[0];
                journalId = Long.parseLong(idString, 16);
            }
            // scan journal
            scanJournal(journalId, printMsg);
            return 0;
        }

        @Override
        String getDescription() {
            return "Scan a journal file and format the entries into readable format.";
        }

        @Override
        String getUsage() {
            return "readjournal [-m] <journal_id | journal_file_name>";
        }

        @Override
        Options getOptions() {
            return rjOpts;
        }
    }

    /**
     * Command to print last log mark
     */
    class LastMarkCmd implements Command {
        @Override
        public int runCmd(String[] args) throws Exception {
            printLastLogMark();
            return 0;
        }

        @Override
        public void printUsage() {
            System.err.println("lastmark: Print last log marker.");
        }
    }

    /**
     * Command to print help message
     */
    class HelpCmd implements Command {
        @Override
        public int runCmd(String[] args) throws Exception {
            if (args.length == 0) {
                printShellUsage();
                return 0;
            }
            String cmdName = args[0];
            Command cmd = commands.get(cmdName);
            if (null == cmd) {
                System.err.println("Unknown command " + cmdName);
                printShellUsage();
                return -1;
            }
            cmd.printUsage();
            return 0;
        }

        @Override
        public void printUsage() {
            System.err.println("help: Describe the usage of this program or its subcommands.");
            System.err.println("usage: help [COMMAND]");
        }
    }

    /**
     * Command for administration of autorecovery
     */
    class AutoRecoveryCmd extends MyCommand {
        Options opts = new Options();

        public AutoRecoveryCmd() {
            super(CMD_AUTORECOVERY);
            opts.addOption("e", "enable", false,
                           "Enable auto recovery of underreplicated ledgers");
            opts.addOption("d", "disable", false,
                           "Disable auto recovery of underreplicated ledgers");
        }

        @Override
        Options getOptions() {
            return opts;
        }

        @Override
        String getDescription() {
            return "Enable or disable autorecovery in the cluster.";
        }

        @Override
        String getUsage() {
            return "autorecovery [-enable|-disable]";
        }

        @Override
        int runCmd(CommandLine cmdLine) throws Exception {
            boolean disable = cmdLine.hasOption("d");
            boolean enable = cmdLine.hasOption("e");

            if ((!disable && !enable)
                || (enable && disable)) {
                LOG.error("One and only one of -enable and -disable must be specified");
                printUsage();
                return 1;
            }
            ZooKeeper zk = null;
            try {
                zk = ZooKeeperClient.createConnectedZooKeeperClient(bkConf.getZkServers(), bkConf.getZkTimeout());
                LedgerManagerFactory mFactory = LedgerManagerFactory.newLedgerManagerFactory(bkConf, zk);
                LedgerUnderreplicationManager underreplicationManager = mFactory.newLedgerUnderreplicationManager();
                if (enable) {
                    if (underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already enabled. Doing nothing");
                    } else {
                        LOG.info("Enabling autorecovery");
                        underreplicationManager.enableLedgerReplication();
                    }
                } else {
                    if (!underreplicationManager.isLedgerReplicationEnabled()) {
                        LOG.warn("Autorecovery already disabled. Doing nothing");
                    } else {
                        LOG.info("Disabling autorecovery");
                        underreplicationManager.disableLedgerReplication();
                    }
                }
            } finally {
                if (zk != null) {
                    zk.close();
                }
            }

            return 0;
        }
    }

    final Map<String, Command> commands;
    {
        commands = new HashMap<String, Command>();
        commands.put(CMD_METAFORMAT, new MetaFormatCmd());
        commands.put(CMD_BOOKIEFORMAT, new BookieFormatCmd());
        commands.put(CMD_RECOVER, new RecoverCmd());
        commands.put(CMD_LEDGER, new LedgerCmd());
        commands.put(CMD_READLOG, new ReadLogCmd());
        commands.put(CMD_READJOURNAL, new ReadJournalCmd());
        commands.put(CMD_LASTMARK, new LastMarkCmd());
        commands.put(CMD_AUTORECOVERY, new AutoRecoveryCmd());
        commands.put(CMD_HELP, new HelpCmd());
    }

    @Override
    public void setConf(Configuration conf) throws Exception {
        bkConf.loadConf(conf);
        journalDirectory = Bookie.getCurrentDirectory(bkConf.getJournalDir());
        ledgerDirectories = Bookie.getCurrentDirectories(bkConf.getLedgerDirs());
        File[] idxDirs = bkConf.getIndexDirs();
        indexDirectories = null != idxDirs ? Bookie.getCurrentDirectories(idxDirs) : ledgerDirectories;
        formatter = EntryFormatter.newEntryFormatter(bkConf, ENTRY_FORMATTER_CLASS);
        LOG.info("Using entry formatter " + formatter.getClass().getName());
        pageSize = bkConf.getPageSize();
        entriesPerPage = pageSize / 8;
    }

    private static void printShellUsage() {
        System.err.println("Usage: BookieShell [-conf configuration] <command>");
        System.err.println();
        System.err.println("       metaformat   [-nonInteractive] [-force]");
        System.err.println("       bookieformat [-nonInteractive] [-force]");
        System.err.println("       recover      <bookieSrc> [bookieDest]");
        System.err.println("       ledger       [-meta] <ledger_id>");
        System.err.println("       readlog      [-msg] <entry_log_id|entry_log_file_name>");
        System.err.println("       readjournal  [-msg] <journal_id|journal_file_name>");
        System.err.println("       autorecovery [-enable|-disable]");
        System.err.println("       lastmark");
        System.err.println("       help");
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length <= 0) {
            printShellUsage();
            return -1;
        }
        String cmdName = args[0];
        Command cmd = commands.get(cmdName);
        if (null == cmd) {
            System.err.println("ERROR: Unknown command " + cmdName);
            printShellUsage();
            return -1;
        }
        // prepare new args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return cmd.runCmd(newArgs);
    }

    public static void main(String argv[]) throws Exception {
        if (argv.length <= 0) {
            printShellUsage();
            System.exit(-1);
        }

        CompositeConfiguration conf = new CompositeConfiguration();
        // load configuration
        if ("-conf".equals(argv[0])) {
            if (argv.length <= 1) {
                printShellUsage();
                System.exit(-1);
            }
            conf.addConfiguration(new PropertiesConfiguration(
                                  new File(argv[1]).toURI().toURL()));

            String[] newArgv = new String[argv.length - 2];
            System.arraycopy(argv, 2, newArgv, 0, newArgv.length);
            argv = newArgv;
        }

        BookieShell shell = new BookieShell();
        shell.setConf(conf);
        int res = shell.run(argv);
        System.exit(res);
    }

    ///
    /// Bookie File Operations
    ///

    /**
     * Get the ledger file of a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     *
     * @return file object.
     */
    private File getLedgerFile(long ledgerId) {
        String ledgerName = LedgerCacheImpl.getLedgerName(ledgerId);
        File lf = null;
        for (File d : indexDirectories) {
            lf = new File(d, ledgerName);
            if (lf.exists()) {
                break;
            }
            lf = null;
        }
        return lf;
    }

    /**
     * Get FileInfo for a specified ledger.
     *
     * @param ledgerId
     *          Ledger Id
     * @return read only file info instance
     */
    ReadOnlyFileInfo getFileInfo(long ledgerId) throws IOException {
        File ledgerFile = getLedgerFile(ledgerId);
        if (null == ledgerFile) {
            throw new FileNotFoundException("No index file found for ledger " + ledgerId + ". It may be not flushed yet.");
        }
        ReadOnlyFileInfo fi = new ReadOnlyFileInfo(ledgerFile, null);
        fi.readHeader();
        return fi;
    }

    private synchronized void initEntryLogger() throws IOException {
        if (null == entryLogger) {
            // provide read only entry logger
            entryLogger = new ReadOnlyEntryLogger(bkConf);
        }
    }

    /**
     * scan over entry log
     *
     * @param logId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     */
    protected void scanEntryLog(long logId, EntryLogScanner scanner) throws IOException {
        initEntryLogger();
        entryLogger.scanEntryLog(logId, scanner);
    }

    protected byte[] readEntry(long ledgerId, long entryId, long position) throws IOException {
        initEntryLogger();
        return entryLogger.readEntry(ledgerId, entryId, position);
    }

    private synchronized Journal getJournal() throws IOException {
        if (null == journal) {
            journal = new Journal(bkConf, new LedgerDirsManager(bkConf, bkConf.getLedgerDirs()),
                    NullStatsLogger.INSTANCE);
        }
        return journal;
    }

    /**
     * Scan journal file
     *
     * @param journalId
     *          Journal File Id
     * @param scanner
     *          Journal File Scanner
     */
    protected void scanJournal(long journalId, JournalScanner scanner) throws IOException {
        getJournal().scanJournal(journalId, 0L, scanner);
    }

    ///
    /// Bookie Shell Commands
    ///

    /**
     * Read ledger meta
     *
     * @param ledgerId
     *          Ledger Id
     */
    protected void readLedgerMeta(long ledgerId) throws Exception {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        byte[] masterKey = fi.getMasterKey();
        if (null == masterKey) {
            System.out.println("master key  : NULL");
        } else {
            System.out.println("master key  : " + bytes2Hex(fi.getMasterKey()));
        }
        long size = fi.size();
        if (size % 8 == 0) {
            System.out.println("size        : " + size);
        } else {
            System.out.println("size : " + size + " (not aligned with 8, may be corrupted or under flushing now)");
        }
        System.out.println("entries     : " + (size / 8));
        System.out.println("fenced      : " + fi.isFenced());
    }

    /**
     * Read ledger index entires
     *
     * @param ledgerId
     *          Ledger Id
     * @throws IOException
     */
    protected void readLedgerIndexEntries(long ledgerId) throws IOException {
        System.out.println("===== LEDGER: " + ledgerId + " =====");
        FileInfo fi = getFileInfo(ledgerId);
        long size = fi.size();
        System.out.println("size        : " + size);
        long curSize = 0;
        long curEntry = 0;
        LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage, null);
        lep.usePage();
        try {
            while (curSize < size) {
                lep.setLedgerAndFirstEntry(ledgerId, curEntry);
                lep.readPage(fi);

                // process a page
                for (int i=0; i<entriesPerPage; i++) {
                    long offset = lep.getOffset(i * 8);
                    if (0 == offset) {
                        System.out.println("entry " + curEntry + "\t:\tN/A");
                    } else {
                        long entryLogId = offset >> 32L;
                        long pos = offset & 0xffffffffL;
                        System.out.println("entry " + curEntry + "\t:\t(log:" + entryLogId + ", pos: " + pos + ", location: " + offset + ")");
                    }
                    ++curEntry;
                }

                curSize += pageSize;
            }
        } catch (IOException ie) {
            LOG.error("Failed to read index page : ", ie);
            if (curSize + pageSize < size) {
                System.out.println("Failed to read index page @ " + curSize + ", the index file may be corrupted : " + ie.getMessage());
            } else {
                System.out.println("Failed to read last index page @ " + curSize
                                 + ", the index file may be corrupted or last index page is not fully flushed yet : " + ie.getMessage());
            }
        }
    }

    protected void readEntry(long ledgerId, long entryId, long position, boolean printMsg) throws Exception {
        System.out.println("Entry(lid=" + ledgerId + ", eid=" + entryId + "), logId=" + (position >> 32));
        byte[] data = readEntry(ledgerId, entryId, position);
        ByteBuffer entryBuf = ByteBuffer.wrap(data);
        formatEntry(position & 0xffffffffL, entryBuf, printMsg);
    }

    /**
     * Scan over an entry log file.
     *
     * @param logId
     *          Entry Log File id.
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanEntryLog(long logId, final boolean printMsg) throws Exception {
        System.out.println("Scan entry log " + logId + " (" + Long.toHexString(logId) + ".log)");
        scanEntryLog(logId, new EntryLogScanner() {
            @Override
            public boolean accept(long ledgerId) {
                return ledgerId != EntryLogger.INVALID_LID;
            }
            @Override
            public void process(long ledgerId, long startPos, ByteBuffer entry) {
                formatEntry(startPos, entry, printMsg);
            }
        });
    }

    /**
     * Scan a journal file
     *
     * @param journalId
     *          Journal File Id
     * @param printMsg
     *          Whether printing the entry data.
     */
    protected void scanJournal(long journalId, final boolean printMsg) throws Exception {
        System.out.println("Scan journal " + journalId + " (" + Long.toHexString(journalId) + ".txn)");
        scanJournal(journalId, new JournalScanner() {
            boolean printJournalVersion = false;
            @Override
            public void process(int journalVersion, long offset, ByteBuffer entry) throws IOException {
                if (!printJournalVersion) {
                    System.out.println("Journal Version : " + journalVersion);
                    printJournalVersion = true;
                }
                formatEntry(offset, entry, printMsg);
            }
        });
    }

    /**
     * Print last log mark
     */
    protected void printLastLogMark() throws IOException {
        LastLogMark lastLogMark = getJournal().getLastLogMark();
        System.out.println(lastLogMark.getCurMark().toString());
    }

    /**
     * Format the message into a readable format.
     *
     * @param pos
     *          File offset of the message stored in entry log file
     * @param recBuff
     *          Entry Data
     * @param printMsg
     *          Whether printing the message body
     */
    private void formatEntry(long pos, ByteBuffer recBuff, boolean printMsg) {
        long ledgerId = recBuff.getLong();
        long entryId = recBuff.getLong();
        int entrySize = recBuff.limit();

        System.out.println("--------- Lid=" + ledgerId + ", Eid=" + entryId
                         + ", ByteOffset=" + pos + ", EntrySize=" + entrySize + " ---------");
        if (entryId == Bookie.METAENTRY_ID_LEDGER_KEY) {
            int masterKeyLen = recBuff.getInt();
            byte[] masterKey = new byte[masterKeyLen];
            recBuff.get(masterKey);
            System.out.println("Type:           META");
            System.out.println("MasterKey:      " + bytes2Hex(masterKey));
            System.out.println();
            return;
        } else if (entryId == Bookie.METAENTRY_ID_FENCE_KEY) {
            System.out.println("Type:           FENCE");
            System.out.println();
            return;
        }
        // process a data entry
        long lastAddConfirmed = recBuff.getLong();
        long length = recBuff.getLong();
        System.out.println("Type:           DATA");
        System.out.println("LastConfirmed:  " + lastAddConfirmed + ", Length: " + length);
        if (!printMsg) {
            System.out.println();
            return;
        }
        // skip digest checking
        recBuff.position(32 + 8);
        System.out.println("Data:");
        System.out.println();
        try {
            byte[] ret = new byte[recBuff.remaining()];
            recBuff.get(ret);
            formatter.formatEntry(ret);
        } catch (Exception e) {
            System.out.println("N/A. Corrupted.");
        }
        System.out.println();
    }

    static String bytes2Hex(byte[] data) {
        StringBuilder sb = new StringBuilder(data.length * 2);
        Formatter formatter = new Formatter(sb);
        for (byte b : data) {
            formatter.format("%02x", b);
        }
        return sb.toString();
    }
}
