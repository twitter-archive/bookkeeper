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
package org.apache.bookkeeper.conf;

import static com.google.common.base.Charsets.UTF_8;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.EnsemblePlacementPolicy;
import org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy;
import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;

/**
 * Configuration settings for client side
 */
public class ClientConfiguration extends AbstractConfiguration {

    // Zookeeper Parameters
    protected final static String ZK_TIMEOUT = "zkTimeout";
    protected final static String ZK_SERVERS = "zkServers";

    // A total of "throttle" permits are available for read and write operations to bookies
    // However, reads can only take up to "readThrottle" permits. Write operations always have
    // at least "throttle - readThrottle" permits available.

    // Throttle value
    protected final static String THROTTLE = "throttle";

    // Digest Type
    protected final static String DIGEST_TYPE = "digestType";
    // Passwd
    protected final static String PASSWD = "passwd";

    // NIO Parameters
    protected final static String CLIENT_TCP_NODELAY = "clientTcpNoDelay";
    protected final static String CLIENT_SENDBUFFER_SIZE = "clientSendBufferSize";
    protected final static String CLIENT_RECEIVEBUFFER_SIZE = "clientReceiveBufferSize";
    protected final static String CLIENT_WRITEBUFFER_LOW_WATER_MARK = "clientWriteBufferLowWaterMark";
    protected final static String CLIENT_WRITEBUFFER_HIGH_WATER_MARK = "clientWriteBufferHighWaterMark";
    protected final static String NUM_CHANNELS_PER_BOOKIE = "numChannelsPerBookie";
    // Read Parameters
    protected final static String READ_TIMEOUT = "readTimeout";
    protected final static String SPECULATIVE_READ_TIMEOUT = "speculativeReadTimeout";
    protected final static String FIRST_SPECULATIVE_READ_TIMEOUT = "firstSpeculativeReadTimeout";
    protected final static String MAX_SPECULATIVE_READ_TIMEOUT = "maxSpeculativeReadTimeout";
    protected final static String SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER = "speculativeReadTimeoutBackoffMultiplier";
    protected final static String FIRST_SPECULATIVE_READ_LAC_TIMEOUT = "firstSpeculativeReadLACTimeout";
    protected final static String MAX_SPECULATIVE_READ_LAC_TIMEOUT = "maxSpeculativeReadLACTimeout";
    protected final static String SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER = "speculativeReadLACTimeoutBackoffMultiplier";
    protected final static String ENABLE_PARALLEL_RECOVERY_READ = "enableParallelRecoveryRead";
    protected final static String RECOVERY_READ_BATCH_SIZE = "recoveryReadBatchSize";
    // Add Parameters
    protected final static String DELAY_ENSEMBLE_CHANGE = "delayEnsembleChange";
    // Timeout Setting
    protected final static String ADD_ENTRY_TIMEOUT_SEC = "addEntryTimeoutSec";
    protected final static String ADD_ENTRY_QUORUM_TIMEOUT_SEC = "addEntryQuorumTimeoutSec";
    protected final static String READ_ENTRY_TIMEOUT_SEC = "readEntryTimeoutSec";
    protected final static String TIMEOUT_TASK_INTERVAL_MILLIS = "timeoutTaskIntervalMillis";
    protected final static String TIMEOUT_TIMER_TICK_DURATION_MS = "timeoutTimerTickDurationMs";
    protected final static String TIMEOUT_TIMER_NUM_TICKS = "timeoutTimerNumTicks";
    protected final static String CONNECT_TIMEOUT_MILLIS = "connectTimeoutMillis";

    // Number Woker Threads
    protected final static String NUM_WORKER_THREADS = "numWorkerThreads";

    // Ensemble Placement Policy
    protected final static String ENSEMBLE_PLACEMENT_POLICY = "ensemblePlacementPolicy";

    // Stats
    protected final static String ENABLE_PER_HOST_STATS = "enablePerHostStats";
    protected final static String ENABLE_TASK_EXECUTION_STATS = "enableTaskExecutionStats";
    protected final static String TASK_EXECUTION_WARN_TIME_MICROS = "taskExecutionWarnTimeMicros";

    // Failure History Settings
    protected final static String ENABLE_BOOKIE_FAILURE_TRACKING = "enableBookieFailureTracking";
    protected final static String BOOKIE_FAILURE_HISTORY_EXPIRATION_MS = "bookieFailureHistoryExpirationMSec";

    /**
     * Construct a default client-side configuration
     */
    public ClientConfiguration() {
        super();
    }

    /**
     * Construct a client-side configuration using a base configuration
     *
     * @param conf
     *          Base configuration
     */
    public ClientConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get throttle value
     *
     * @return throttle value
     * @see #setThrottleValue
     */
    public int getThrottleValue() {
        return this.getInt(THROTTLE, 10000);
    }

    /**
     * Set throttle value.
     *
     * Since BookKeeper process requests in asynchrous way, it will holds
     * those pending request in queue. You may easily run it out of memory
     * if producing too many requests than the capability of bookie servers can handle.
     * To prevent that from happeding, you can set a throttle value here.
     *
     * @param throttle
     *          Throttle Value
     * @return client configuration
     */
    public ClientConfiguration setThrottleValue(int throttle) {
        this.setProperty(THROTTLE, Integer.toString(throttle));
        return this;
    }

    /**
     * Get digest type used in bookkeeper admin
     *
     * @return digest type
     * @see #setBookieRecoveryDigestType
     */
    public DigestType getBookieRecoveryDigestType() {
        return DigestType.valueOf(this.getString(DIGEST_TYPE, DigestType.CRC32.toString()));
    }

    /**
     * Set digest type used in bookkeeper admin.
     *
     * Digest Type and Passwd used to open ledgers for admin tool
     * For now, assume that all ledgers were created with the same DigestType
     * and password. In the future, this admin tool will need to know for each
     * ledger, what was the DigestType and password used to create it before it
     * can open it. These values will come from System properties, though fixed
     * defaults are defined here.
     *
     * @param digestType
     *          Digest Type
     * @return client configuration
     */
    public ClientConfiguration setBookieRecoveryDigestType(DigestType digestType) {
        this.setProperty(DIGEST_TYPE, digestType.toString());
        return this;
    }

    /**
     * Get passwd used in bookkeeper admin
     *
     * @return password
     * @see #setBookieRecoveryPasswd
     */
    public byte[] getBookieRecoveryPasswd() {
        return this.getString(PASSWD, "").getBytes(UTF_8);
    }

    /**
     * Set passwd used in bookkeeper admin.
     *
     * Digest Type and Passwd used to open ledgers for admin tool
     * For now, assume that all ledgers were created with the same DigestType
     * and password. In the future, this admin tool will need to know for each
     * ledger, what was the DigestType and password used to create it before it
     * can open it. These values will come from System properties, though fixed
     * defaults are defined here.
     *
     * @param passwd
     *          Password
     * @return client configuration
     */
    public ClientConfiguration setBookieRecoveryPasswd(byte[] passwd) {
        setProperty(PASSWD, new String(passwd, UTF_8));
        return this;
    }

    /**
     * Is tcp connection no delay.
     *
     * @return tcp socket nodelay setting
     * @see #setClientTcpNoDelay
     */
    public boolean getClientTcpNoDelay() {
        return getBoolean(CLIENT_TCP_NODELAY, true);
    }

    /**
     * Set socket nodelay setting.
     *
     * This settings is used to enabled/disabled Nagle's algorithm, which is a means of
     * improving the efficiency of TCP/IP networks by reducing the number of packets
     * that need to be sent over the network. If you are sending many small messages,
     * such that more than one can fit in a single IP packet, setting client.tcpnodelay
     * to false to enable Nagle algorithm can provide better performance.
     * <br>
     * Default value is true.
     *
     * @param noDelay
     *          NoDelay setting
     * @return client configuration
     */
    public ClientConfiguration setClientTcpNoDelay(boolean noDelay) {
        setProperty(CLIENT_TCP_NODELAY, Boolean.toString(noDelay));
        return this;
    }

    /**
     * Get client send buffer size.
     *
     * @return client send buffer size
     */
    public int getClientSendBufferSize() {
        return getInt(CLIENT_SENDBUFFER_SIZE, 1 * 1024 * 1024);
    }

    /**
     * Set client send buffer size.
     *
     * @param bufferSize
     *          send buffer size.
     * @return client configuration.
     */
    public ClientConfiguration setClientSendBufferSize(int bufferSize) {
        setProperty(CLIENT_SENDBUFFER_SIZE, bufferSize);
        return this;
    }

    /**
     * Get client receive buffer size.
     *
     * @return client receive buffer size.
     */
    public int getClientReceiveBufferSize() {
        return getInt(CLIENT_RECEIVEBUFFER_SIZE, 1 * 1024 * 1024);
    }

    /**
     * Set client receive buffer size.
     *
     * @param bufferSize
     *          receive buffer size.
     * @return client configuration.
     */
    public ClientConfiguration setClientReceiveBufferSize(int bufferSize) {
        setProperty(CLIENT_RECEIVEBUFFER_SIZE, bufferSize);
        return this;
    }


    /**
     * Get netty channel write buffer low water mark.
     * @return write buffer low water mark.
     */
    public int getClientWriteBufferLowWaterMark() {
        return getInt(CLIENT_WRITEBUFFER_LOW_WATER_MARK, 32 * 1024);
    }

    /**
     * Set client write buffer low water mark.
     *
     * @param waterMark
     *          write buffer low water mark.
     * @return client configuration.
     */
    public ClientConfiguration setClientWriteBufferLowWaterMark(int waterMark) {
        setProperty(CLIENT_WRITEBUFFER_LOW_WATER_MARK, waterMark);
        return this;
    }

    /**
     * Get netty channel write buffer high water mark.
     *
     * @return write buffer high water mark.
     */
    public int getClientWriteBufferHighWaterMark() {
        return getInt(CLIENT_WRITEBUFFER_HIGH_WATER_MARK, 64 * 1024);
    }

    /**
     * Set client write buffer high water mark.
     *
     * @param waterMark
     *          write buffer high water mark.
     * @return client configuration.
     */
    public ClientConfiguration setClientWriteBufferHighWaterMark(int waterMark) {
        setProperty(CLIENT_WRITEBUFFER_HIGH_WATER_MARK, waterMark);
        return this;
    }

    /**
     * Get num channels per bookie.
     *
     * @return num channels per bookie.
     */
    public int getNumChannelsPerBookie() {
        return getInt(NUM_CHANNELS_PER_BOOKIE, 1);
    }

    /**
     * Set num channels per bookie.
     *
     * @param numChannelsPerBookie
     *          num channels per bookie.
     * @return client configuration.
     */
    public ClientConfiguration setNumChannelsPerBookie(int numChannelsPerBookie) {
        setProperty(NUM_CHANNELS_PER_BOOKIE, numChannelsPerBookie);
        return this;
    }

    /**
     * Get zookeeper servers to connect
     *
     * @return zookeeper servers
     */
    public String getZkServers() {
        List<Object> servers = getList(ZK_SERVERS, null);
        if (null == servers || 0 == servers.size()) {
            return "localhost";
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    public ClientConfiguration setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
        return this;
    }

    /**
     * Get zookeeper timeout
     *
     * @return zookeeper client timeout
     */
    public int getZkTimeout() {
        return getInt(ZK_TIMEOUT, 10000);
    }

    /**
     * Set zookeeper timeout
     *
     * @param zkTimeout
     *          ZooKeeper client timeout
     * @return client configuration
     */
    public ClientConfiguration setZkTimeout(int zkTimeout) {
        setProperty(ZK_TIMEOUT, Integer.toString(zkTimeout));
        return this;
    }

    /**
     * Get the socket read timeout. This is the number of
     * seconds we wait without hearing a response from a bookie
     * before we consider it failed.
     *
     * The default is 5 seconds.
     *
     * @return the current read timeout in seconds
     */
    public int getReadTimeout() {
        return getInt(READ_TIMEOUT, 5);
    }

    /**
     * Set the socket read timeout.
     * @see #getReadTimeout()
     * @param timeout The new read timeout in seconds
     * @return client configuration
     */
    public ClientConfiguration setReadTimeout(int timeout) {
        setProperty(READ_TIMEOUT, Integer.toString(timeout));
        return this;
    }

    /**
     * Get the timeout for add request. This is the number of seconds we wait without hearing
     * a response for add request from a bookie before we consider it failed.
     *
     * The default value is 1 second. We do it more aggressive to not accumulate add requests
     * due to slow responses.
     *
     * @return add entry timeout.
     */
    public int getAddEntryTimeout() {
        return getInt(ADD_ENTRY_TIMEOUT_SEC, 1);
    }

    /**
     * Set timeout for add entry request.
     * @see #getAddEntryTimeout()
     *
     * @param timeout
     *          The new add entry timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setAddEntryTimeout(int timeout) {
        setProperty(ADD_ENTRY_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the timeout for top-level add request. That is, the amount of time we should spend
     * waiting for ack quorum.
     *
     * @return add entry ack quorum timeout.
     */
    public int getAddEntryQuorumTimeout() {
        return getInt(ADD_ENTRY_QUORUM_TIMEOUT_SEC, Integer.MAX_VALUE);
    }

    /**
     * Set timeout for top-level add entry request.
     * @see #getAddEntryQuorumTimeout()
     *
     * @param timeout
     *          The new add entry ack quorum timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setAddEntryQuorumTimeout(int timeout) {
        setProperty(ADD_ENTRY_QUORUM_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the timeout for read entry. This is the number of seconds we wait without hearing
     * a response for read entry request from a bookie before we consider it failed. By default,
     * we use socket timeout specified at {@link #getReadTimeout()}.
     *
     * @return read entry timeout.
     */
    public int getReadEntryTimeout() {
        return getInt(READ_ENTRY_TIMEOUT_SEC, getReadTimeout());
    }

    /**
     * Set the timeout for read entry request.
     * @see #getReadEntryTimeout()
     *
     * @param timeout
     *          The new read entry timeout in seconds.
     * @return client configuration.
     */
    public ClientConfiguration setReadEntryTimeout(int timeout) {
        setProperty(READ_ENTRY_TIMEOUT_SEC, timeout);
        return this;
    }

    /**
     * Get the interval between successive executions of the PerChannelBookieClient's
     * TimeoutTask. This value is in milliseconds. Every X milliseconds, the timeout task
     * will be executed and it will error out entries that have timed out.
     *
     * We do it more aggressive to not accumulate pending requests due to slow responses.
     * @return
     */
    @Deprecated
    public long getTimeoutTaskIntervalMillis() {
        return getLong(TIMEOUT_TASK_INTERVAL_MILLIS,
                TimeUnit.SECONDS.toMillis(Math.min(getAddEntryTimeout(), getReadEntryTimeout())) / 2);
    }

    @Deprecated
    public ClientConfiguration setTimeoutTaskIntervalMillis(long timeoutMillis) {
        setProperty(TIMEOUT_TASK_INTERVAL_MILLIS, Long.toString(timeoutMillis));
        return this;
    }

    /**
     * Get the tick duration in milliseconds that used for timeout timer.
     *
     * @return tick duration in milliseconds
     */
    public long getTimeoutTimerTickDurationMs() {
        return getLong(TIMEOUT_TIMER_TICK_DURATION_MS, 100);
    }

    /**
     * Set the tick duration in milliseconds that used for timeout timer.
     *
     * @param tickDuration
     *          tick duration in milliseconds.
     * @return client configuration.
     */
    public ClientConfiguration setTimeoutTimerTickDurationMs(long tickDuration) {
        setProperty(TIMEOUT_TIMER_TICK_DURATION_MS, tickDuration);
        return this;
    }

    /**
     * Get number of ticks that used for timeout timer.
     *
     * @return number of ticks that used for timeout timer.
     */
    public int getTimeoutTimerNumTicks() {
        return getInt(TIMEOUT_TIMER_NUM_TICKS, 1024);
    }

    /**
     * Set number of ticks that used for timeout timer.
     *
     * @param numTicks
     *          number of ticks that used for timeout timer.
     * @return client configuration.
     */
    public ClientConfiguration setTimeoutTimerNumTicks(int numTicks) {
        setProperty(TIMEOUT_TIMER_NUM_TICKS, numTicks);
        return this;
    }

    /**
     * Get netty connect timeout in millis.
     *
     * @return netty connect timeout in millis.
     */
    public int getConnectTimeoutMillis() {
        // 10 seconds as netty default value.
        return getInt(CONNECT_TIMEOUT_MILLIS, 10000);
    }

    /**
     * Set netty connect timeout in millis.
     *
     * @param connectTimeoutMillis
     *          netty connect timeout in millis.
     * @return client configuration.
     */
    public ClientConfiguration setConnectTimeoutMillis(int connectTimeoutMillis) {
        setProperty(CONNECT_TIMEOUT_MILLIS, connectTimeoutMillis);
        return this;
    }

    /**
     * Get the number of worker threads. This is the number of
     * worker threads used by bookkeeper client to submit operations.
     *
     * @return the number of worker threads
     */
    public int getNumWorkerThreads() {
        return getInt(NUM_WORKER_THREADS, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Set the number of worker threads.
     *
     * <p>
     * NOTE: setting the number of worker threads after BookKeeper object is constructed
     * will not take any effect on the number of threads in the pool.
     * </p>
     *
     * @see #getNumWorkerThreads()
     * @param numThreads number of worker threads used for bookkeeper
     * @return client configuration
     */
    public ClientConfiguration setNumWorkerThreads(int numThreads) {
        setProperty(NUM_WORKER_THREADS, numThreads);
        return this;
    }

    /**
     * Get the period of time after which a speculative entry read should be triggered.
     * A speculative entry read is sent to the next replica bookie before
     * an error or response has been received for the previous entry read request.
     *
     * A speculative entry read is only sent if we have not heard from the current
     * replica bookie during the entire read operation which may comprise of many entries.
     *
     * Speculative reads allow the client to avoid having to wait for the connect timeout
     * in the case that a bookie has failed. It induces higher load on the network and on
     * bookies. This should be taken into account before changing this configuration value.
     *
     * @see org.apache.bookkeeper.client.LedgerHandle#asyncReadEntries
     * @return the speculative read timeout in milliseconds. Default 1000.
     */
    public int getSpeculativeReadTimeout() {
        return getInt(SPECULATIVE_READ_TIMEOUT, 1500);
    }

    /**
     * Set the speculative read timeout. A lower timeout will reduce read latency in the
     * case of a failed bookie, while increasing the load on bookies and the network.
     *
     * The default is 2000 milliseconds. A value of 0 will disable speculative reads
     * completely.
     *
     * @see #getSpeculativeReadTimeout()
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setSpeculativeReadTimeout(int timeout) {
        setProperty(SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Get the first speculative read timeout.
     *
     * @return first speculative read timeout.
     */
    public int getFirstSpeculativeReadTimeout() {
        return getInt(FIRST_SPECULATIVE_READ_TIMEOUT, getSpeculativeReadTimeout());
    }

    /**
     * Set the first speculative read timeout.
     *
     * @param timeout
     *          first speculative read timeout.
     * @return client configuration.
     */
    public ClientConfiguration setFirstSpeculativeReadTimeout(int timeout) {
        setProperty(FIRST_SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }

    /**
     * Multipler to use when determining time between successive speculative read requests
     *
     * @return speculative read timeout backoff multiplier.
     */
    public float getSpeculativeReadTimeoutBackoffMultiplier() {
        return getFloat(SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER, 2.0f);
    }

    /**
     * Set the multipler to use when determining time between successive speculative read requests
     *
     * @param speculativeReadTimeoutBackoffMultiplier
     *          multipler to use when determining time between successive speculative read requests.
     * @return client configuration.
     */
    public ClientConfiguration setSpeculativeReadTimeoutBackoffMultiplier(float speculativeReadTimeoutBackoffMultiplier) {
        setProperty(SPECULATIVE_READ_TIMEOUT_BACKOFF_MULTIPLIER, speculativeReadTimeoutBackoffMultiplier);
        return this;
    }

    /**
     * Multipler to use when determining time between successive speculative read LAC requests
     *
     * @return speculative read LAC timeout backoff multiplier.
     */
    public float getSpeculativeReadLACTimeoutBackoffMultiplier() {
        return getFloat(SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER, 2.0f);
    }

    /**
     * Set the multipler to use when determining time between successive speculative read LAC requests
     *
     * @param speculativeReadLACTimeoutBackoffMultiplier
     *          multipler to use when determining time between successive speculative read LAC requests.
     * @return client configuration.
     */
    public ClientConfiguration setSpeculativeReadLACTimeoutBackoffMultiplier(float speculativeReadLACTimeoutBackoffMultiplier) {
        setProperty(SPECULATIVE_READ_LAC_TIMEOUT_BACKOFF_MULTIPLIER, speculativeReadLACTimeoutBackoffMultiplier);
        return this;
    }

    /**
     * Get the max speculative read timeout.
     *
     * @return max speculative read timeout.
     */
    public int getMaxSpeculativeReadTimeout() {
        return getInt(MAX_SPECULATIVE_READ_TIMEOUT, getSpeculativeReadTimeout());
    }

    /**
     * Set the max speculative read timeout.
     *
     * @param timeout
     *          max speculative read timeout.
     * @return client configuration.
     */
    public ClientConfiguration setMaxSpeculativeReadTimeout(int timeout) {
        setProperty(MAX_SPECULATIVE_READ_TIMEOUT, timeout);
        return this;
    }


    /**
     * Get the period of time after which the first speculative read last add confirmed and entry
     * should be triggered.
     * A speculative entry request is sent to the next replica bookie before
     * an error or response has been received for the previous entry read request.
     *
     * A speculative entry read is only sent if we have not heard from the current
     * replica bookie during the entire read operation which may comprise of many entries.
     *
     * Speculative requests allow the client to avoid having to wait for the connect timeout
     * in the case that a bookie has failed. It induces higher load on the network and on
     * bookies. This should be taken into account before changing this configuration value.
     *
     * @return the speculative request timeout in milliseconds. Default 1500.
     */
    public int getFirstSpeculativeReadLACTimeout() {
        return getInt(FIRST_SPECULATIVE_READ_LAC_TIMEOUT, 1500);
    }


    /**
     * Get the maximum interval between successive speculative read last add confirmed and entry
     * requests.
     *
     * @return the max speculative request timeout in milliseconds. Default 5000.
     */
    public int getMaxSpeculativeReadLACTimeout() {
        return getInt(MAX_SPECULATIVE_READ_LAC_TIMEOUT, 5000);
    }

    /**
     * Set the period of time after which the first speculative read last add confirmed and entry
     * should be triggered.
     * A lower timeout will reduce read latency in the case of a failed bookie,
     * while increasing the load on bookies and the network.
     *
     * The default is 1500 milliseconds. A value of 0 will disable speculative reads
     * completely.
     *
     * @see #getSpeculativeReadTimeout()
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setFirstSpeculativeReadLACTimeout(int timeout) {
        setProperty(FIRST_SPECULATIVE_READ_LAC_TIMEOUT, timeout);
        return this;
    }

    /**
     * Set the maximum interval between successive speculative read last add confirmed and entry
     * requests.
     *
     * @param timeout the timeout value, in milliseconds
     * @return client configuration
     */
    public ClientConfiguration setMaxSpeculativeReadLACTimeout(int timeout) {
        setProperty(MAX_SPECULATIVE_READ_LAC_TIMEOUT, timeout);
        return this;
    }

    /**
     * Get Ensemble Placement Policy Class.
     *
     * @return ensemble placement policy class.
     */
    public Class<? extends EnsemblePlacementPolicy> getEnsemblePlacementPolicy()
        throws ConfigurationException {
        return ReflectionUtils.getClass(this, ENSEMBLE_PLACEMENT_POLICY,
                                        RackawareEnsemblePlacementPolicy.class,
                                        EnsemblePlacementPolicy.class,
                                        defaultLoader);
    }

    /**
     * Set Ensemble Placement Policy Class.
     *
     * @param policyClass
     *          Ensemble Placement Policy Class.
     */
    public ClientConfiguration setEnsemblePlacementPolicy(Class<? extends EnsemblePlacementPolicy> policyClass) {
        setProperty(ENSEMBLE_PLACEMENT_POLICY, policyClass.getName());
        return this;
    }

    /**
     * Whether to enable parallel reading in recovery read.
     *
     * @return true if enable parallel reading in recovery read. otherwise, return false.
     */
    public boolean getEnableParallelRecoveryRead() {
        return getBoolean(ENABLE_PARALLEL_RECOVERY_READ, false);
    }

    /**
     * Enable/Disable parallel reading in recovery read.
     *
     * @param enabled
     *          flag to enable/disable parallel reading in recovery read.
     * @return client configuration.
     */
    public ClientConfiguration setEnableParallelRecoveryRead(boolean enabled) {
        setProperty(ENABLE_PARALLEL_RECOVERY_READ, enabled);
        return this;
    }

    /**
     * Get Recovery Read Batch Size.
     *
     * @return recovery read batch size.
     */
    public int getRecoveryReadBatchSize() {
        return getInt(RECOVERY_READ_BATCH_SIZE, 1);
    }

    /**
     * Set Recovery Read Batch Size.
     *
     * @param batchSize
     *          recovery read batch size.
     * @return client configuration.
     */
    public ClientConfiguration setRecoveryReadBatchSize(int batchSize) {
        setProperty(RECOVERY_READ_BATCH_SIZE, batchSize);
        return this;
    }

    /**
     * Whether to enable per host stats?
     *
     * @return flag to enable/disable per host stats
     */
    public boolean getEnablePerHostStats() {
        return getBoolean(ENABLE_PER_HOST_STATS, false);
    }

    /**
     * Enable/Disable per host stats.
     *
     * @param enabled
     *          flag to enable/disable per host stats.
     * @return
     */
    public ClientConfiguration setEnablePerHostStats(boolean enabled) {
        setProperty(ENABLE_PER_HOST_STATS, enabled);
        return this;
    }

    /**
     * Whether to enable recording task execution stats.
     *
     * @return flag to enable/disable recording task execution stats.
     */
    public boolean getEnableTaskExecutionStats() {
        return getBoolean(ENABLE_TASK_EXECUTION_STATS, false);
    }

    /**
     * Enable/Disable recording task execution stats.
     *
     * @param enabled
     *          flag to enable/disable recording task execution stats.
     * @return client configuration.
     */
    public ClientConfiguration setEnableTaskExecutionStats(boolean enabled) {
        setProperty(ENABLE_TASK_EXECUTION_STATS, enabled);
        return this;
    }

    /**
     * Get task execution duration which triggers a warning.
     *
     * @return time in microseconds which triggers a warning.
     */
    public long getTaskExecutionWarnTimeMicros() {
        return getLong(TASK_EXECUTION_WARN_TIME_MICROS, TimeUnit.SECONDS.toMicros(1));
    }

    /**
     * Set task execution duration which triggers a warning.
     *
     * @param warnTime
     *          time in microseconds which triggers a warning.
     * @return client configuration.
     */
    public ClientConfiguration setTaskExecutionWarnTimeMicros(long warnTime) {
        setProperty(TASK_EXECUTION_WARN_TIME_MICROS, warnTime);
        return this;
    }

    /**
     * Whether to delay ensemble change or not?
     *
     * @return true if to delay ensemble change, otherwise false.
     */
    public boolean getDelayEnsembleChange() {
        return getBoolean(DELAY_ENSEMBLE_CHANGE, false);
    }

    /**
     * Enable/Disable delaying ensemble change.
     * <p>
     * If set to true, ensemble change only happens when it can't meet
     * ack quorum requirement. If set to false, ensemble change happens
     * immediately when it received a failed write.
     * </p>
     *
     * @param enabled
     *          flag to enable/disable delaying ensemble change.
     * @return client configuration.
     */
    public ClientConfiguration setDelayEnsembleChange(boolean enabled) {
        setProperty(DELAY_ENSEMBLE_CHANGE, enabled);
        return this;
    }

    /**
     * Whether to enable bookie failure tracking
     *
     * @return flag to enable/disable bookie failure tracking
     */
    public boolean getEnableBookieFailureTracking() {
        return getBoolean(ENABLE_BOOKIE_FAILURE_TRACKING, true);
    }

    /**
     * Enable/Disable bookie failure tracking.
     *
     * @param enabled
     *          flag to enable/disable bookie failure tracking
     * @return client configuration.
     */
    public ClientConfiguration setEnableBookieFailureTracking(boolean enabled) {
        setProperty(ENABLE_BOOKIE_FAILURE_TRACKING, enabled);
        return this;
    }

    /**
     * Get the bookie failure tracking expiration timeout.
     *
     * @return bookie failure tracking expiration timeout.
     */
    public int getBookieFailureHistoryExpirationMSec() {
        return getInt(BOOKIE_FAILURE_HISTORY_EXPIRATION_MS, 60000);
    }

    /**
     * Set the bookie failure tracking expiration timeout.
     *
     * @param timeout
     *          bookie failure tracking expiration timeout.
     * @return client configuration.
     */
    public ClientConfiguration setBookieFailureHistoryExpirationMSec(int expirationMSec) {
        setProperty(BOOKIE_FAILURE_HISTORY_EXPIRATION_MS, expirationMSec);
        return this;
    }
}
