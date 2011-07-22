package com.twitter.hedwig.client.benchmark;

import java.io.File;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.benchmark.HedwigBenchmark;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Log4JLoggerFactory;

import com.google.protobuf.ByteString;
import org.apache.hedwig.client.conf.ClientConfiguration;

public class TwitterHedwigBenchmark extends HedwigBenchmark {
    public TwitterHedwigBenchmark(ClientConfiguration cfg) {
        super(cfg);
    }

    @Override
    public Void call() throws Exception {
        final String mode = System.getProperty("mode","");
        if (mode.equals("twitterSub")) {
            String topic = System.getProperty("topic", "twitter");
            String subscriberPrefix = System.getProperty("subscriberPrefix", "twitterSubscriber");
            int numSubscribers = Integer.getInteger("numberSubscribers", 1);
            double delayFrequency = Double.valueOf(System.getProperty("delayFrequency", "0"));
            int delayInSecs = Integer.getInteger("delayInSecs", 0);
            int statsReportingIntervalInSecs =
                Integer.getInteger("statsReportingIntervalInSecs", 10);

            TwitterBenchmarkSubscriber benchmarkSub =
                new TwitterBenchmarkSubscriber(subscriber, ByteString.copyFromUtf8(topic),
                    subscriberPrefix, numSubscribers, delayFrequency, delayInSecs,
                    statsReportingIntervalInSecs);

            benchmarkSub.call();
        } else {
            super.call();
        }

        return null;
    }

    public static void main(String[] args) throws Exception {
        ClientConfiguration cfg = new ClientConfiguration();
        if (args.length > 0) {
            String confFile = args[0];
            try {
                cfg.loadConf(new File(confFile).toURI().toURL());
            } catch (ConfigurationException e) {
                throw new RuntimeException(e);
            }
        }

        InternalLoggerFactory.setDefaultFactory(new Log4JLoggerFactory());

        TwitterHedwigBenchmark app = new TwitterHedwigBenchmark(cfg);
        app.call();
        System.exit(0);
    }

}
