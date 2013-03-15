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

import java.io.File;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;

/**
 * Configuration manages server-side settings
 */
public class ServerConfiguration extends AbstractConfiguration {
    // Entry Log Parameters
    protected final static String ENTRY_LOG_SIZE_LIMIT = "logSizeLimit";
    protected final static String ENTRY_LOG_FILE_PREALLOCATION_ENABLED = "entryLogFilePreallocationEnabled";
    protected final static String MINOR_COMPACTION_INTERVAL = "minorCompactionInterval";
    protected final static String MINOR_COMPACTION_THRESHOLD = "minorCompactionThreshold";
    protected final static String MAJOR_COMPACTION_INTERVAL = "majorCompactionInterval";
    protected final static String MAJOR_COMPACTION_THRESHOLD = "majorCompactionThreshold";

    // Gc Parameters
    protected final static String GC_WAIT_TIME = "gcWaitTime";
    // Bookie death watch interval
    protected final static String DEATH_WATCH_INTERVAL = "bookieDeathWatchInterval";
    // Ledger Cache Parameters
    protected final static String OPEN_FILE_LIMIT = "openFileLimit";
    protected final static String PAGE_LIMIT = "pageLimit";
    protected final static String PAGE_SIZE = "pageSize";
    // Journal Parameters
    protected final static String MAX_JOURNAL_SIZE = "journalMaxSizeMB";
    protected final static String MAX_BACKUP_JOURNALS = "journalMaxBackups";
    protected final static String JOURNAL_ADAPTIVE_GROUP_WRITES = "journalAdaptiveGroupWrites";
    protected final static String JOURNAL_MAX_GROUP_WAIT_MSEC = "journalMaxGroupWaitMSec";
    protected final static String JOURNAL_BUFFERED_WRITES_THRESHOLD = "journalBufferedWritesThreshold";
    protected final static String JOURNAL_FLUSH_WHEN_QUEUE_EMPTY = "journalFlushWhenQueueEmpty";
    protected final static String JOURNAL_REMOVE_FROM_PAGE_CACHE = "journalRemoveFromPageCache";
    protected final static String JOURNAL_PRE_ALLOC_SIZE = "journalPreAllocSizeMB";
    protected final static String JOURNAL_WRITE_BUFFER_SIZE = "journalWriteBufferSizeKB";
    // Bookie Parameters
    protected final static String BOOKIE_PORT = "bookiePort";
    protected final static String JOURNAL_DIR = "journalDirectory";
    protected final static String LEDGER_DIRS = "ledgerDirectories";
    protected final static String INDEX_DIRS = "indexDirectories";
    // NIO Parameters
    protected final static String SERVER_TCP_NODELAY = "serverTcpNoDelay";
    // Zookeeper Parameters
    protected final static String ZK_TIMEOUT = "zkTimeout";
    protected final static String ZK_SERVERS = "zkServers";
    // Statistics Parameters
    protected final static String ENABLE_STATISTICS = "enableStatistics";
    protected final static String OPEN_LEDGER_REREPLICATION_GRACE_PERIOD = "openLedgerRereplicationGracePeriod";
    //ReadOnly mode support on all disk full
    protected final static String READ_ONLY_MODE_ENABLED = "readOnlyModeEnabled";
    protected final static String DISK_USAGE_THRESHOLD = "diskUsageThreshold";
    protected final static String DISK_CHECK_INTERVAL = "diskCheckInterval";

    // Worker Thread parameters.
    protected final static String NUM_ADD_WORKER_THREADS = "numAddWorkerThreads";
    protected final static String NUM_READ_WORKER_THREADS = "numReadWorkerThreads";

    // Stats exporting
    protected final static String STATS_EXPORT = "statsExport";
    protected final static String STATS_HTTP_PORT = "statsHttpPort";

    protected final static String READ_BUFFER_SIZE = "readBufferSizeBytes";
    protected final static String WRITE_BUFFER_SIZE = "writeBufferSizeBytes";

    protected final static String SORTED_LEDGER_STORAGE_ENABLED = "sortedLedgerStorageEnabled";
    protected final static String SKIP_LIST_SIZE_LIMIT = "skipListSizeLimit";
    protected final static String SKIP_LIST_CHUNK_SIZE_ENTRY = "skipListArenaChunkSize";
    protected final static String SKIP_LIST_MAX_ALLOC_ENTRY = "skipListArenaMaxAllocSize";

    /**
     * Construct a default configuration object
     */
    public ServerConfiguration() {
        super();
    }

    /**
     * Construct a configuration based on other configuration
     *
     * @param conf
     *          Other configuration
     */
    public ServerConfiguration(AbstractConfiguration conf) {
        super();
        loadConf(conf);
    }

    /**
     * Get entry logger size limitation
     *
     * @return entry logger size limitation
     */
    public long getEntryLogSizeLimit() {
        return this.getLong(ENTRY_LOG_SIZE_LIMIT, 2 * 1024 * 1024 * 1024L);
    }

    /**
     * Set entry logger size limitation
     *
     * @param logSizeLimit
     *          new log size limitation
     */
    public ServerConfiguration setEntryLogSizeLimit(long logSizeLimit) {
        this.setProperty(ENTRY_LOG_SIZE_LIMIT, Long.toString(logSizeLimit));
        return this;
    }

    /**
     * Is entry log file preallocation enabled.
     *
     * @return whether entry log file preallocation is enabled or not.
     */
    public boolean isEntryLogFilePreAllocationEnabled() {
        return this.getBoolean(ENTRY_LOG_FILE_PREALLOCATION_ENABLED, true);
    }

    /**
     * Enable/disable entry log file preallocation.
     *
     * @param enabled
     *          enable/disable entry log file preallocation.
     * @return server configuration object.
     */
    public ServerConfiguration setEntryLogFilePreAllocationEnabled(boolean enabled) {
        this.setProperty(ENTRY_LOG_FILE_PREALLOCATION_ENABLED, enabled);
        return this;
    }

    /**
     * Get Garbage collection wait time
     *
     * @return gc wait time
     */
    public long getGcWaitTime() {
        return this.getLong(GC_WAIT_TIME, 1000);
    }

    /**
     * Set garbage collection wait time
     *
     * @param gcWaitTime
     *          gc wait time
     * @return server configuration
     */
    public ServerConfiguration setGcWaitTime(long gcWaitTime) {
        this.setProperty(GC_WAIT_TIME, Long.toString(gcWaitTime));
        return this;
    }

    /**
     * Get bookie death watch interval
     *
     * @return watch interval
     */
    public int getDeathWatchInterval() {
        return this.getInt(DEATH_WATCH_INTERVAL, 1000);
    }

    /**
     * Get open file limit
     *
     * @return max number of files to open
     */
    public int getOpenFileLimit() {
        return this.getInt(OPEN_FILE_LIMIT, 900);
    }

    /**
     * Set limitation of number of open files.
     *
     * @param fileLimit
     *          Limitation of number of open files.
     * @return server configuration
     */
    public ServerConfiguration setOpenFileLimit(int fileLimit) {
        setProperty(OPEN_FILE_LIMIT, fileLimit);
        return this;
    }

    /**
     * Get limitation number of index pages in ledger cache
     *
     * @return max number of index pages in ledger cache
     */
    public int getPageLimit() {
        return this.getInt(PAGE_LIMIT, -1);
    }

    /**
     * Set limitation number of index pages in ledger cache.
     *
     * @param pageLimit
     *          Limitation of number of index pages in ledger cache.
     * @return server configuration
     */
    public ServerConfiguration setPageLimit(int pageLimit) {
        this.setProperty(PAGE_LIMIT, pageLimit);
        return this;
    }

    /**
     * Get page size
     *
     * @return page size in ledger cache
     */
    public int getPageSize() {
        return this.getInt(PAGE_SIZE, 8192);
    }

    /**
     * Set page size
     *
     * @see #getPageSize()
     *
     * @param pageSize
     *          Page Size
     * @return Server Configuration
     */
    public ServerConfiguration setPageSize(int pageSize) {
        this.setProperty(PAGE_SIZE, pageSize);
        return this;
    }

    /**
     * Max journal file size
     *
     * @return max journal file size
     */
    public long getMaxJournalSizeMB() {
        return this.getLong(MAX_JOURNAL_SIZE, 2 * 1024);
    }

    /**
     * Set new max journal file size
     *
     * @param maxJournalSize
     *          new max journal file size
     * @return server configuration
     */
    public ServerConfiguration setMaxJournalSizeMB(long maxJournalSize) {
        this.setProperty(MAX_JOURNAL_SIZE, Long.toString(maxJournalSize));
        return this;
    }

    /**
     * How much space should we pre-allocate at a time in the journal
     *
     * @return journal pre-allocation size in MB
     */
    public int getJournalPreAllocSizeMB() {
        return this.getInt(JOURNAL_PRE_ALLOC_SIZE, 16);
    }

    /**
     * Size of the write buffers used for the journal
     *
     * @return journal write buffer size in KB
     */
    public int getJournalWriteBufferSizeKB() {
        return this.getInt(JOURNAL_WRITE_BUFFER_SIZE, 64);
    }

    /**
     * Max number of older journal files kept
     *
     * @return max number of older journal files to kept
     */
    public int getMaxBackupJournals() {
        return this.getInt(MAX_BACKUP_JOURNALS, 5);
    }

    /**
     * Set max number of older journal files to kept
     *
     * @param maxBackupJournals
     *          Max number of older journal files
     * @return server configuration
     */
    public ServerConfiguration setMaxBackupJournals(int maxBackupJournals) {
        this.setProperty(MAX_BACKUP_JOURNALS, Integer.toString(maxBackupJournals));
        return this;
    }

    /**
     * Get bookie port that bookie server listen on
     *
     * @return bookie port
     */
    public int getBookiePort() {
        return this.getInt(BOOKIE_PORT, 3181);
    }

    /**
     * Set new bookie port that bookie server listen on
     *
     * @param port
     *          Port to listen on
     * @return server configuration
     */
    public ServerConfiguration setBookiePort(int port) {
        this.setProperty(BOOKIE_PORT, Integer.toString(port));
        return this;
    }

    /**
     * Get dir name to store journal files
     *
     * @return journal dir name
     */
    public String getJournalDirName() {
        return this.getString(JOURNAL_DIR, "/tmp/bk-txn");
    }

    /**
     * Set dir name to store journal files
     *
     * @param journalDir
     *          Dir to store journal files
     * @return server configuration
     */
    public ServerConfiguration setJournalDirName(String journalDir) {
        this.setProperty(JOURNAL_DIR, journalDir);
        return this;
    }

    /**
     * Get dir to store journal files
     *
     * @return journal dir, if no journal dir provided return null
     */
    public File getJournalDir() {
        String journalDirName = getJournalDirName();
        if (null == journalDirName) {
            return null;
        }
        return new File(journalDirName);
    }

    /**
     * Get dir names to store ledger data
     *
     * @return ledger dir names, if not provided return null
     */
    public String[] getLedgerDirNames() {
        String[] ledgerDirs = this.getStringArray(LEDGER_DIRS);
        if (null == ledgerDirs) {
            return new String[] { "/tmp/bk-data" };
        }
        return ledgerDirs;
    }

    /**
     * Set dir names to store ledger data
     *
     * @param ledgerDirs
     *          Dir names to store ledger data
     * @return server configuration
     */
    public ServerConfiguration setLedgerDirNames(String[] ledgerDirs) {
        if (null == ledgerDirs) {
            return this;
        }
        this.setProperty(LEDGER_DIRS, ledgerDirs);
        return this;
    }

    /**
     * Get dirs that stores ledger data
     *
     * @return ledger dirs
     */
    public File[] getLedgerDirs() {
        String[] ledgerDirNames = getLedgerDirNames();
        if (null == ledgerDirNames) {
            return null;
        }
        File[] ledgerDirs = new File[ledgerDirNames.length];
        for (int i = 0; i < ledgerDirNames.length; i++) {
            ledgerDirs[i] = new File(ledgerDirNames[i]);
        }
        return ledgerDirs;
    }

    /** 
     * Get dir name to store index files.
     *   
     * @return ledger index dir name, if no index dirs provided return null
     */  
    public String[] getIndexDirNames() {
        if (!this.containsKey(INDEX_DIRS)) {
            return null;
        }
        return this.getStringArray(INDEX_DIRS);
    }   

    /** 
     * Set dir name to store index files.
     *
     * @param indexDirs
     *          Index dir names
     * @return server configuration.
     */
    public ServerConfiguration setIndexDirName(String[] indexDirs) {
        this.setProperty(INDEX_DIRS, indexDirs);
        return this;
    }

    /**
     * Get index dir to store ledger index files.
     *
     * @return index dirs, if no index dirs provided return null
     */
    public File[] getIndexDirs() {
        String[] idxDirNames = getIndexDirNames();
        if (null == idxDirNames) {
            return null;
        }
        File[] idxDirs = new File[idxDirNames.length];
        for (int i=0; i<idxDirNames.length; i++) {
            idxDirs[i] = new File(idxDirNames[i]);
        }
        return idxDirs;
    }

    /**
     * Is tcp connection no delay.
     *
     * @return tcp socket nodelay setting
     */
    public boolean getServerTcpNoDelay() {
        return getBoolean(SERVER_TCP_NODELAY, true);
    }

    /**
     * Set socket nodelay setting
     *
     * @param noDelay
     *          NoDelay setting
     * @return server configuration
     */
    public ServerConfiguration setServerTcpNoDelay(boolean noDelay) {
        setProperty(SERVER_TCP_NODELAY, Boolean.toString(noDelay));
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
            return null;
        }
        return StringUtils.join(servers, ",");
    }

    /**
     * Set zookeeper servers to connect
     *
     * @param zkServers
     *          ZooKeeper servers to connect
     */
    public ServerConfiguration setZkServers(String zkServers) {
        setProperty(ZK_SERVERS, zkServers);
        return this;
    }

    /**
     * Get zookeeper timeout
     *
     * @return zookeeper server timeout
     */
    public int getZkTimeout() {
        return getInt(ZK_TIMEOUT, 10000);
    }

    /**
     * Set zookeeper timeout
     *
     * @param zkTimeout
     *          ZooKeeper server timeout
     * @return server configuration
     */
    public ServerConfiguration setZkTimeout(int zkTimeout) {
        setProperty(ZK_TIMEOUT, Integer.toString(zkTimeout));
        return this;
    }

    /**
     * Is statistics enabled
     *
     * @return is statistics enabled
     */
    public boolean isStatisticsEnabled() {
        return getBoolean(ENABLE_STATISTICS, true);
    }

    /**
     * Turn on/off statistics
     *
     * @param enabled
     *          Whether statistics enabled or not.
     * @return server configuration
     */
    public ServerConfiguration setStatisticsEnabled(boolean enabled) {
        setProperty(ENABLE_STATISTICS, Boolean.toString(enabled));
        return this;
    }

    /**
     * Get threshold of minor compaction.
     *
     * For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a minor compaction.
     *
     * If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public double getMinorCompactionThreshold() {
        return getDouble(MINOR_COMPACTION_THRESHOLD, 0.2f);
    }

    /**
     * Set threshold of minor compaction
     *
     * @see #getMinorCompactionThreshold()
     *
     * @param threshold
     *          Threshold for minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionThreshold(double threshold) {
        setProperty(MINOR_COMPACTION_THRESHOLD, threshold);
        return this;
    }

    /**
     * Get threshold of major compaction.
     *
     * For those entry log files whose remaining size percentage reaches below
     * this threshold  will be compacted in a major compaction.
     *
     * If it is set to less than zero, the major compaction is disabled.
     *
     * @return threshold of major compaction
     */
    public double getMajorCompactionThreshold() {
        return getDouble(MAJOR_COMPACTION_THRESHOLD, 0.8f);
    }

    /**
     * Set threshold of major compaction.
     *
     * @see #getMajorCompactionThreshold()
     *
     * @param threshold
     *          Threshold of major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionThreshold(double threshold) {
        setProperty(MAJOR_COMPACTION_THRESHOLD, threshold);
        return this;
    }

    /**
     * Get interval to run minor compaction, in seconds.
     *
     * If it is set to less than zero, the minor compaction is disabled.
     *
     * @return threshold of minor compaction
     */
    public long getMinorCompactionInterval() {
        return getLong(MINOR_COMPACTION_INTERVAL, 3600);
    }

    /**
     * Set interval to run minor compaction
     *
     * @see #getMinorCompactionInterval()
     *
     * @param interval
     *          Interval to run minor compaction
     * @return server configuration
     */
    public ServerConfiguration setMinorCompactionInterval(long interval) {
        setProperty(MINOR_COMPACTION_INTERVAL, interval);
        return this;
    }

    /**
     * Get interval to run major compaction, in seconds.
     *
     * If it is set to less than zero, the major compaction is disabled.
     *
     * @return high water mark
     */
    public long getMajorCompactionInterval() {
        return getLong(MAJOR_COMPACTION_INTERVAL, 86400);
    }

    /**
     * Set interval to run major compaction.
     *
     * @see #getMajorCompactionInterval()
     *
     * @param interval
     *          Interval to run major compaction
     * @return server configuration
     */
    public ServerConfiguration setMajorCompactionInterval(long interval) {
        setProperty(MAJOR_COMPACTION_INTERVAL, interval);
        return this;
    }
    
    /**
     * Set the grace period which the rereplication worker will wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     * 
     * The grace period allows the writer to detect the bookie failure, and and
     * start writing to another ledger fragment. If the writer writes nothing
     * during the grace period, the rereplication worker assumes that it has
     * crashed and therefore fences the ledger, preventing any further writes to
     * that ledger.
     * 
     * @see org.apache.bookkeeper.client.BookKeeper#openLedger
     */
    public void setOpenLedgerRereplicationGracePeriod(String waitTime) {
        setProperty(OPEN_LEDGER_REREPLICATION_GRACE_PERIOD, waitTime);
    }

    /**
     * Get the grace period which the rereplication worker to wait before
     * fencing and rereplicating a ledger fragment which is still being written
     * to, on bookie failure.
     */
    public long getOpenLedgerRereplicationGracePeriod() {
        return getLong(OPEN_LEDGER_REREPLICATION_GRACE_PERIOD, 30000);
    }

    /**
     * Get the number of threads that should handle write requests.
     * @return
     */
    public int getNumAddWorkerThreads() {
        return getInt(NUM_ADD_WORKER_THREADS, 1);
    }

    /**
     * Get the number of threads that should handle read requests.
     */
    public int getNumReadWorkerThreads() {
        return getInt(NUM_READ_WORKER_THREADS, 80);
    }

    /**
     * Get the number of bytes we should use as capacity for the {@link
     * org.apache.bookkeeper.bookie.BufferedReadChannel}
     * Default is 512 bytes
     * @return
     */
    public int getReadBufferBytes() {
        return getInt(READ_BUFFER_SIZE, 512);
    }

    // Whether we should export stats on the http server or not.
    public boolean getStatsExport() {
        return getBoolean(STATS_EXPORT, false);
    }

    // The port on which the http server exporting stats runs.
    public int getStatsHttpPort() {
        return getInt(STATS_HTTP_PORT, 9002);
    }

    /**
     * Get the number of bytes used as capacity for the reordered write buffer. Default is
     * 64KB.
     * NOTE: Make sure this value is greater than the maximum message size.
     * @return
     */
    public int getWriteBufferBytes() {
        return getInt(WRITE_BUFFER_SIZE, 65536);
    }

    /**
     * Set sorted-ledger storage enabled or not
     *
     * @param enabled
     */
    public ServerConfiguration setSortedLedgerStorageEnabled(boolean enabled) {
        this.setProperty(SORTED_LEDGER_STORAGE_ENABLED, enabled);
        return this;
    }

    /**
     * Check if sorted-ledger storage enabled (default true)
     *
     * @return
     */
    public boolean getSortedLedgerStorageEnabled() {
        return this.getBoolean(SORTED_LEDGER_STORAGE_ENABLED, true);
    }

    /**
     * Get skip list data size limitation (default 64MB)
     *
     * @return skip list data size limitation
     */
    public long getSkipListSizeLimit() {
        return this.getLong(SKIP_LIST_SIZE_LIMIT, 64 * 1024 * 1024L);
    }

    /**
     * Set skip list size limit.
     *
     * @param size skip list size limit.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListSizeLimit(int size) {
        setProperty(SKIP_LIST_SIZE_LIMIT, size);
        return this;
    }

    /**
     * Get the number of bytes we should use as chunk allocation for the {@link
     * org.apache.bookkeeper.bookie.SkipListArena}
     * Default is 4 MB
     * @return
     */
    public int getSkipListArenaChunkSize() {
        return getInt(SKIP_LIST_CHUNK_SIZE_ENTRY, 4096 * 1024);
    }

    /**
     * Set the number of bytes w used as chunk allocation for {@link
     * org.apache.bookkeeper.bookie.SkipListArena}.
     *
     * @param size chunk size.
     * @return server configuration object.
     */
    public ServerConfiguration setSkipListArenaChunkSize(int size) {
        setProperty(SKIP_LIST_CHUNK_SIZE_ENTRY, size);
        return this;
    }

    /**
     * Get the max size we should delegate memory allocation to VM for the {@link
     * org.apache.bookkeeper.bookie.SkipListArena}
     * Default is 128 KB
     * @return
     */
    public int getSkipListArenaMaxAllocSize() {
        return getInt(SKIP_LIST_MAX_ALLOC_ENTRY, 128 * 1024);
    }

    /**
     * Validate the configuration.
     * @throws ConfigurationException
     */
    public void validate() throws ConfigurationException {
        if (getSkipListArenaChunkSize() < getSkipListArenaMaxAllocSize()) {
            throw new ConfigurationException("Arena max allocation size should be smaller than the chunk size.");
        }
    }

    /**
     * Should we group journal force writes
     *
     * @return group journal force writes
     */
    public boolean getJournalAdaptiveGroupWrites() {
        return getBoolean(JOURNAL_ADAPTIVE_GROUP_WRITES, true);
    }

    /**
     * Enable/disable group journal force writes
     *
     * @param enabled flag to enable/disable group journal force writes
     */
    public ServerConfiguration setJournalAdaptiveGroupWrites(boolean enabled) {
        setProperty(JOURNAL_ADAPTIVE_GROUP_WRITES, enabled);
        return this;
    }

    /**
     * Maximum latency to impose on a journal write to achieve grouping
     *
     * @return max wait for grouping
     */
    public long getJournalMaxGroupWaitMSec() {
        return getLong(JOURNAL_MAX_GROUP_WAIT_MSEC, 200);
    }

    /**
     * Maximum latency to impose on a journal write to achieve grouping
     *
     * @return max wait for grouping
     */
    public long getJournalBufferedWritesThreshold() {
        return getLong(JOURNAL_BUFFERED_WRITES_THRESHOLD, 512 * 1024);
    }


    /**
     * Set if we should flush the journal when queue is empty
     */
    public ServerConfiguration setJournalFlushWhenQueueEmpty(boolean enabled) {
        setProperty(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY, enabled);
        return this;
    }

    /**
     * Should we flush the journal when queue is empty
     *
     * @return flush when queue is empty
     */
    public boolean getJournalFlushWhenQueueEmpty() {
        return getBoolean(JOURNAL_FLUSH_WHEN_QUEUE_EMPTY, false);
    }

    /**
     * Should we remove pages from page cache after force write
     *
     * @return remove pages from cache
     */
    public boolean getJournalRemovePagesFromCache() {
        return getBoolean(JOURNAL_REMOVE_FROM_PAGE_CACHE, false);
    }

    /**
     * Set the ReadOnlyModeEnabled status
     */
    public ServerConfiguration setReadOnlyModeEnabled(boolean enabled) {
        setProperty(READ_ONLY_MODE_ENABLED, enabled);
        return this;
    }

    /**
     * Get ReadOnlyModeEnabled status
     */
    public boolean isReadOnlyModeEnabled() {
        return getBoolean(READ_ONLY_MODE_ENABLED, false);
    }

    /**
     * Set the Disk free space threshold in Bytes after which disk will be
     * considered as full during diskcheck.
     */
    public ServerConfiguration setDiskUsageThreshold(float threshold) {
        setProperty(DISK_USAGE_THRESHOLD, threshold);
        return this;
    }

    /**
     * Returns disk free space threshold. By default 100MB
     */
    public float getDiskUsageThreshold() {
        return getFloat(DISK_USAGE_THRESHOLD, 0.95f);
    }

    /**
     * Set the disk checker interval to monitor ledger disk space
     */
    public ServerConfiguration setDiskCheckInterval(int interval) {
        setProperty(DISK_CHECK_INTERVAL, interval);
        return this;
    }

    /**
     * Get the disk checker interval
     */
    public int getDiskCheckInterval() {
        return getInt(DISK_CHECK_INTERVAL, 10 * 1000);
    }
}
