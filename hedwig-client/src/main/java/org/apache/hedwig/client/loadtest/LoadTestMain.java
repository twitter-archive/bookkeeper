package org.apache.hedwig.client.loadtest;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.stats.TimeSeriesRepository;
import com.twitter.common.stats.TimeSeriesRepositoryImpl;
import org.apache.commons.cli.*;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The main class that is invoked to start the load test.
 */
public class LoadTestMain {
    private static Logger logger = LoggerFactory.getLogger(LoadTestMain.class);

    public static Option getAsRequiredOption(String opt, boolean hasArg, String description) {
        Option retOpt = new Option(opt, opt, hasArg, description);
        retOpt.setRequired(true);
        return retOpt;
    }

    //TODO: Use long opts?
    public static Options getOptions() {
        Options options = new Options();
        options.addOption(getAsRequiredOption("op", true, "'publish' or 'receive' to indicate " +
                "the mode of operation."));
        options.addOption("tsi", "topic_start_index", true, "Topics that this class" +
                "start at <topic_prefix>-<topic_start_index>. Default 0");
        options.addOption("nt", "num_topics", true, "The number of topics this class " +
                "is responsible for. Default 1.");
        options.addOption("tp", "topic_prefix", true, "The prefix for any topic. Default: 'Topic'");
        options.addOption("ms", "message_size", true, "The size of each published message. Default: 1KB");
        options.addOption("mo", "max_outstanding", true, "Maximum number of outstanding requests." +
                " Default: 1024.");
        options.addOption("co", "concurrency", true, "The number of threads simultaneously " +
                "publishing. Default: 1");
        options.addOption("ds", "duration_sec", true, "The duration of the loadtest in seconds. " +
                "Default: 60 seconds.");
        options.addOption("pr", "publish_rate", true, "The number of publishes per second. 0 signifies " +
                "that there is no rate control. Default: 0");
        options.addOption("cn", "client_config_file", true, "The path for the client configuration" +
                " file.");
        options.addOption("sp", "subscriber_prefix", true, "The prefix for subscriber ids. Default: subscriber.");
        options.addOption("ssi", "subscriber_start_index", true, "The starting index for subscriber id generation." +
                " Default: 0");
        options.addOption("ns", "num_subscribers", true, "Number of subscribers. Default: 1");
        options.addOption("sp", "stat_print_sec", true, "The time duration between printing stats. Default: 30");
        options.addOption("h", "help", false, "Help.");
        return options;
    }

    public void start(String args[]) throws Exception {

        /**
         * Command line options. Parse them and get all useful values so that
         * the code from here on doesn't rely on the command line options.
         */
        Options options = getOptions();
        CommandLineParser parser = new PosixParser();
        final CommandLine cmd = parser.parse(options, args);

        if (cmd.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("LoadTestMain <options>", options);
            System.exit(0);
        }

        ClientConfiguration conf = new ClientConfiguration();
        if (cmd.hasOption("client_config_file")) {
            String file = cmd.getOptionValue("client_config_file");
            conf.loadConf(new File(file).toURI().toURL());
        }
        String op = cmd.getOptionValue("op");
        String topicPrefix = cmd.getOptionValue("topic_prefix", "topic");
        String subscriberPrefix = cmd.getOptionValue("subscriber_prefix", "subscriber");
        LoadTestUtils ltUtil = new LoadTestUtils(topicPrefix, subscriberPrefix);
        int numTopics = Integer.valueOf(cmd.getOptionValue("num_topics", "1"));
        int topicStartIndex = Integer.valueOf(cmd.getOptionValue("topic_start_index",
                "0"));
        int messageSize = Integer.valueOf(cmd.getOptionValue("message_size", "1024"));
        int concurrency = Integer.valueOf(cmd.getOptionValue("concurrency", "1"));
        int durationSec = Integer.valueOf(cmd.getOptionValue("duration_sec", "60"));
        int publishRate = Integer.valueOf(cmd.getOptionValue("publish_rate", "0"));
        int maxOutstanding = Integer.valueOf(cmd.getOptionValue("max_outstanding", "1024"));
        int subscriberStartIndex = Integer.valueOf(cmd.getOptionValue("subscriber_start_index", "0"));
        int numSubscribers = Integer.valueOf(cmd.getOptionValue("num_subscribers", "1"));
        int statPrintSec = Integer.valueOf(cmd.getOptionValue("stat_print_sec", "30"));
        TopicProvider topicProvider = new TopicProvider(numTopics, topicStartIndex,
                ltUtil);
        logger.info("Starting loadtest with the following options. Operation:" + op + ", topicPrefix:" + topicPrefix +
                ", subscriberPrefix:" + subscriberPrefix + ", numTopics:" + numTopics +
                ", topicStartIndex:" + topicStartIndex + ", messageSize:" + messageSize +
                ", concurrency:" + concurrency + ", durationSec:" + durationSec +
                ", publishRate:" + publishRate + ", subscriberStartIndex:" + subscriberStartIndex +
                ", numSubscribers:" + numSubscribers + ", statPrintSec:" + statPrintSec);
        /**
         * Command line related code ends.
         */

        final LoadTestBase testBase;
        if (op.equals("receive")) {
            testBase = new LoadTestReceiver(topicProvider, ltUtil, conf,
                    numSubscribers, subscriberStartIndex);
        } else if (op.equals("publish")) {
            testBase = new LoadTestPublisher(topicProvider, ltUtil, conf,
                    concurrency, publishRate, maxOutstanding, messageSize);
        } else {
            throw new UnrecognizedOptionException("Operation not supported:" + op);
        }

        // The stats need to be sampled every few seconds (1 second here). Start
        // a timeseries sampler to sample our stats every 1 second.
        TimeSeriesRepository sampler = new TimeSeriesRepositoryImpl(Stats.STAT_REGISTRY,
                Amount.of(1L, Time.SECONDS), Amount.of(1L, Time.HOURS));
        ShutdownRegistry.ShutdownRegistryImpl shutdownRegistry = new ShutdownRegistry.ShutdownRegistryImpl();
        sampler.start(shutdownRegistry);

        // A separate thread prints stats every configurable duration.
        ScheduledExecutorService statPrinter = Executors.newSingleThreadScheduledExecutor();
        statPrinter.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("Stats: " + testBase.getStats());
            }
        }, statPrintSec, statPrintSec, TimeUnit.SECONDS);

        /**
         * Start the loadtest. The main thread sleeps for the duration of the loadtest
         * and then wakes up to stop it. None of the other classes are time-aware.
         */
        testBase.start();
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(durationSec));
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for load test to finish.");
            Thread.currentThread().interrupt();
        } finally {
            testBase.stop();
            statPrinter.shutdownNow();
            shutdownRegistry.execute();
        }
        // exit without an error.
        System.exit(0);
    }

    public static void main(String args[]) {
        LoadTestMain loadtest = new LoadTestMain();
        try {
            loadtest.start(args);
        } catch (Exception e) {
            logger.error("Exception while running loadTest.", e);
        }
    }
}
