package org.apache.hedwig.client.loadtest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is used to limit the rate at which publish
 * or any other rate dependent operations in the loadtest
 * are invoked. Tokens are generated at the givne rate and
 * the users are expected to take() a token before they can
 * proceed with an operation.
 *
 * If rate is 0, use a fixed number of tokens to control the rate.
 */
public class RateLimiter implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(RateLimiter.class);
    private final LinkedBlockingQueue<Long> tokenQueue;
    private ExecutorService tokenGenerator = Executors.newSingleThreadExecutor();
    private AtomicLong tokenNumber = new AtomicLong(0);
    // Rate - per second.
    private final int rate;
    private final int rampUpSec;
    // Used to control if rate is 0.
    private final int maxOutstanding;
    // Sleep for the delay period only after each batch size.
    // Else we might end up spending more time in the
    // transition from sleeping->awake->sleeping instead
    // of doing actual work. rate should not be less than this.
    private final int BATCH_SIZE = 100;

    public RateLimiter(int rate, int rampUpSec, int maxOutstanding) {
        this.rate = rate;
        this.rampUpSec = rampUpSec;
        this.maxOutstanding = maxOutstanding;
        // Create a large enough queue. If rate is 0, we should not create
        // a queue with 0 capacity.
        this.tokenQueue = new LinkedBlockingQueue<Long>(BATCH_SIZE*(rate+1));
        tokenGenerator.submit(this);
    }

    public void run() {
        try {
            if (rate <= 0) {
                // Offer a fixed number of tokens and return. The total number of
                // tokens will remain constant.
                for (int i = 0; i < maxOutstanding; i++) {
                    tokenQueue.offer(tokenNumber.incrementAndGet());
                }
                return;
            }
            long startMillis = System.currentTimeMillis();
            long rampUpMsec = TimeUnit.SECONDS.toMillis(this.rampUpSec);
            long currentRate = this.rate;
            while (true) {
                // Generate tokens in batches and sleep for a precalculated delay.
                for (int i = 0; i < BATCH_SIZE; i++) {
                    // Safe to do this as it's only one thread that's updating it.
                    if (tokenQueue.offer(tokenNumber.get() + 1)) {
                        tokenNumber.incrementAndGet();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Generated token: " + tokenNumber.get());
                        }
                    } else {
                        break;
                    }
                }
                if (rampUpMsec != 0) {
                    long diffMillis = System.currentTimeMillis() - startMillis;
                    if (diffMillis < rampUpMsec) {
                        currentRate = Math.max(1, (this.rate * diffMillis) / rampUpMsec);
                    } else {
                        currentRate = this.rate;
                    }
                }
                long delayMillis = BATCH_SIZE * TimeUnit.SECONDS.toMillis(1) / currentRate;
                Thread.sleep(delayMillis);
            }
        } catch (InterruptedException e) {
            logger.info("Interrupted while sleeping.", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("Exception during token generation.", e);
        }
    }

    // Call this function before doing any rate limited operation.
    public Long take() {
        long token = 0;
        try {
            token = tokenQueue.take();
        } catch (InterruptedException e) {
            logger.info("Interrupted while waiting for token.", e);
            Thread.currentThread().interrupt();
        }
        return token;
    }

    // Call this function to let the rate limiter know that one invocation
    // of a rate limited operation has completed.
    public void offer() {
        if (rate <= 0) {
            tokenQueue.offer(tokenNumber.incrementAndGet());
        }
    }

    public void stop() {
        try {
            this.tokenGenerator.shutdownNow();
        } catch (Exception e) {
            logger.error("Exception while shutting down.", e);
        }
    }
}
