package org.apache.hedwig.client.loadtest;

import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.util.MathUtils;
import org.apache.hedwig.client.HedwigClient;
import org.apache.hedwig.client.api.Publisher;
import org.apache.hedwig.client.conf.ClientConfiguration;
import org.apache.hedwig.exceptions.PubSubException;
import org.apache.hedwig.protocol.PubSubProtocol.Message;
import org.apache.hedwig.protocol.LoadTest;
import org.apache.hedwig.protocol.LoadTest.LoadTestMessage;
import org.apache.hedwig.protocol.LoadTest.MessageProviderValue;
import org.apache.hedwig.protocol.PubSubProtocol;
import org.apache.hedwig.util.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTestPublisher extends LoadTestBase {
    private int concurrency;
    private MessageProvider mp;
    private RateLimiter rl;
    private ExecutorService executor;
    private volatile boolean running;
    private static Logger logger = LoggerFactory.getLogger(LoadTestPublisher.class);
    public LoadTestPublisher(TopicProvider topicProvider, LoadTestUtils ltUtil,
                             ClientConfiguration conf, int concurrency,
                             int publishRate, int maxOutstanding, int messageSize) {
        super(topicProvider, ltUtil, conf, "publish");
        this.concurrency = concurrency;
        this.rl = new RateLimiter(publishRate, maxOutstanding);
        this.mp = new MessageProvider(topicProvider,
                messageSize, ltUtil);
        this.executor = Executors.newFixedThreadPool(concurrency);
    }
    private class SinglePublisher implements Runnable {
        final HedwigClient client;
        public SinglePublisher() {
            this.client = new HedwigClient(conf);
        }
        public void run() {
            Publisher publisher = client.getPublisher();
            final AtomicBoolean syncPublish = new AtomicBoolean(true);
            while (running) {
                final long token = rl.take();
                MessageProviderValue value = mp.getMessage();
                LoadTestMessage ltm = value.getMessage();
                ByteString topic = value.getTopic();
                Message messageToPublish = Message.newBuilder()
                        .setBody(ltm.toByteString())
                        .build();
                if (logger.isDebugEnabled()) {
                    logger.debug("Publishing message:" + messageToPublish + " for token:" + token);
                }
                if (syncPublish.getAndSet(false)) {
                    try {
                        publisher.publish(topic, messageToPublish);
                    } catch (Exception e) {
                        logger.error("Exception on first publish" + e);
                        syncPublish.set(true);
                    }
                    continue;
                }
                final long startTimeMillis = MathUtils.now();
                publisher.asyncPublish(topic, messageToPublish, new Callback<Void>() {
                    @Override
                    public void operationFinished(Object ctx, Void resultOfOperation) {
                        rl.offer();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Successfully published message for token:" + token);
                        }
                        long latencyMillis = MathUtils.now() - startTimeMillis;
                        stat.requestComplete(TimeUnit.MILLISECONDS.toMicros(latencyMillis));
                    }

                    @Override
                    public void operationFailed(Object ctx, PubSubException exception) {
                        rl.offer();
                        logger.error("Error while publishing message for token:" + token);
                        long latencyMillis = MathUtils.now() - startTimeMillis;
                        stat.incErrors(TimeUnit.MILLISECONDS.toMicros(latencyMillis));
                        syncPublish.set(true);
                    }
                }, value);
            }
        }
    }

    public void start() {
        running = true;
        for (int i = 0; i < concurrency; i++) {
            executor.submit(new SinglePublisher());
        }
    }

    public void stop() {
        running = false;
        rl.stop();
        executor.shutdownNow();
    }
}
