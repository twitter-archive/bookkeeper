/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hedwig.server.netty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import java.beans.ConstructorProperties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hedwig.protocol.PubSubProtocol.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Server Stats
 */
public class ServerStats {
    private static final Logger LOG = LoggerFactory.getLogger(ServerStats.class);
    static ServerStats instance = new ServerStats();

    /**
     * A read view of stats, also used in CompositeViewData to expose to JMX
     */
    public static class OpStatData {
        private final long maxLatency, minLatency;
        private final double avgLatency;
        private final long numSuccessOps, numFailedOps;
        private final long[] latencyBuckets;
        private final long[] cumulativeLatencyBuckets;

        @ConstructorProperties({"maxLatency", "minLatency", "avgLatency",
                                "numSuccessOps", "numFailedOps", "latencyBuckets",
                                "p99Latency", "p999Latency", "p9999Latency"})
        public OpStatData(long maxLatency, long minLatency, double avgLatency,
                          long numSuccessOps, long numFailedOps, long[] latencyBuckets) {
            this.maxLatency = maxLatency;
            this.minLatency = minLatency == Long.MAX_VALUE ? 0 : minLatency;
            this.avgLatency = avgLatency;
            this.numSuccessOps = numSuccessOps;
            this.numFailedOps = numFailedOps;
            this.latencyBuckets = Arrays.copyOf(latencyBuckets, latencyBuckets.length);
            // initialize the cumulative latency buckets
            this.cumulativeLatencyBuckets = Arrays.copyOf(latencyBuckets, latencyBuckets.length);
            for (int i = 1; i < latencyBuckets.length; i++) {
                this.cumulativeLatencyBuckets[i] += this.cumulativeLatencyBuckets[i-1];
            }
        }

        /**
         * Expose the latency buckets as a String.
         */
        public long[] getLatencyBuckets() {
            return latencyBuckets;
        }

        /**
         * @param percentile as a percentage 99.99, 99.9 etc.
         * @return
         */
        private long percentileLatency(double percentile) {
            double actualPercentile = percentile/100.0;
            long target = (long)(this.numSuccessOps * actualPercentile);
            for (int i = 0; i < this.cumulativeLatencyBuckets.length; i++) {
                if (this.cumulativeLatencyBuckets[i] >= target) {
                    return i;
                }
            }
            // Should never reach here
            return this.cumulativeLatencyBuckets.length;
        }

        public long getP99Latency() {
            return percentileLatency(99.0);
        }

        public long getP999Latency() {
            return percentileLatency(99.9);
        }

        public long getP9999Latency() {
            return percentileLatency(99.99);
        }

        public long getMaxLatency() {
            return maxLatency;
        }

        public long getMinLatency() {
            return minLatency;
        }

        public double getAvgLatency() {
            return avgLatency;
        }

        public long getNumSuccessOps() {
            return numSuccessOps;
        }

        public long getNumFailedOps() {
            return numFailedOps;
        }
    }

    /**
     * Operation Statistics
     */
    public static class OpStats {
        // For now try to get granularity up to 2000ms.
        static final int NUM_BUCKETS = 2002;

        long maxLatency = 0;
        long minLatency = Long.MAX_VALUE;
        double totalLatency = 0.0f;
        long numSuccessOps = 0;
        long numFailedOps = 0;
        long[] latencyBuckets = new long[NUM_BUCKETS];

        OpStats() {}

        /**
         * Increment number of failed operations
         */
        synchronized public void incrementFailedOps() {
            ++numFailedOps;
        }

        /**
         * Update Latency
         */
        synchronized public void updateLatency(long latency) {
            if (latency < 0) {
                // less than 0ms . Ideally this should not happen.
                // We have seen this latency negative in some cases due to the
                // behaviors of JVM. Ignoring the statistics update for such
                // cases.
                LOG.warn("Latency time coming negative");
                return;
            }
            totalLatency += latency;
            ++numSuccessOps;
            if (latency < minLatency) {
                minLatency = latency;
            }
            if (latency > maxLatency) {
                maxLatency = latency;
            }
            int bucket = (int)latency;
            // latencyBuckets[NUM_BUCKETS-1] has values for latencies greater than NUM_BUCKETS-2
            if (bucket > NUM_BUCKETS - 1) {
                bucket = NUM_BUCKETS - 1;
            }
            ++latencyBuckets[bucket];
        }

        synchronized public OpStatData toOpStatData() {
            double avgLatency = numSuccessOps > 0 ? totalLatency / numSuccessOps : 0.0f;
            return new OpStatData(maxLatency, minLatency, avgLatency,
                                  numSuccessOps, numFailedOps, latencyBuckets);
        }

    }

    public static ServerStats getInstance() {
        return instance;
    }

    protected ServerStats() {
        stats = new HashMap<OperationType, OpStats>();
        for (OperationType type : OperationType.values()) {
            stats.put(type, new OpStats());
        }
    }
    Map<OperationType, OpStats> stats;


    AtomicLong numRequestsReceived = new AtomicLong(0);
    AtomicLong numRequestsRedirect = new AtomicLong(0);
    AtomicLong numMessagesDelivered = new AtomicLong(0);
    AtomicLong numTopics = new AtomicLong(0);
    AtomicLong persistQueueSize = new AtomicLong(0);

    /**
     * Stats of operations
     *
     * @param type
     *          Operation Type
     * @return op stats
     */
    public OpStats getOpStats(OperationType type) {
        return stats.get(type);
    }

    public void incrementRequestsReceived() {
        numRequestsReceived.incrementAndGet();
    }

    public void incrementRequestsRedirect() {
        numRequestsRedirect.incrementAndGet();
    }

    public void incrementMessagesDelivered() {
        numMessagesDelivered.incrementAndGet();
    }

    public void setNumTopics(long n) {
        numTopics.getAndSet(n);
    }

    public void incrementPersistQueueSize() {
        persistQueueSize.incrementAndGet();
    }

    public void decrementPersistQueueSize() {
        persistQueueSize.decrementAndGet();
    }

    public long getNumRequestsReceived() {
        return numRequestsReceived.get();
    }

    public long getNumRequestsRedirect() {
        return numRequestsRedirect.get();
    }

    public long getNumMessagesDelivered() {
        return numMessagesDelivered.get();
    }

    public long getNumTopics() {
        return numTopics.get();
    }

    public long getPersistQueueSize() {
        return persistQueueSize.get();
    }
}
