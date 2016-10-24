/*
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

package org.apache.bookkeeper.bookie;

import static com.google.common.base.Charsets.UTF_8;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Optional;
import com.google.common.collect.MapMaker;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.bookie.EntryLogMetadataManager.EntryLogMetadata;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;
import static org.apache.bookkeeper.util.BookKeeperConstants.MAX_LOG_SIZE_LIMIT;

/**
 * This class manages the writing of the bookkeeper entries. All the new
 * entries are written to a common log. The LedgerCache will have pointers
 * into files created by this class with offsets into the files to find
 * the actual ledger entry. The entry log files created by this class are
 * identified by a long.
 *
 * Entry Log File Format:
 * ----------------------
 * log header: 0 - 1023
 * data entries: starting from 1024
 * metadata entries: starting after data entries (introduced in version 1)
 *
 * Log Header Format:
 * ------------------
 * BKLO: 0 - 3
 * version: 4 - 7
 * ledgers map offset: 8 - 15
 * total ledgers: 16 - 19
 * reserved: 20 - 1023
 *
 * Data Entry Format:
 * ------------------
 * length: 0 - 3
 * data: 4 - (size + 4) (first 16 bytes in data is ledger id and entry id)
 *
 * Metadata Entry Format:
 * (metadata entry is a special data entry, whose ledger id is always -1.
 *  so for version 0 bookies would skip metadata entries)
 * ----------------------
 * length: 0 - 3
 * ledger id : 4 - 11 ( always be -1 )
 * metadata entry type : 12 - 19
 * metadata: 20 - (length + 4)
 *
 * LedgerMap Entry Format:
 * total ledgers: 0 - 4
 * ledgers map: each ledger entry is comprised of 16 bytes,
 *              first 8 bytes are ledger id, second 8 bytes are entry size
 */
public class EntryLogger {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);

    static final int VERSION_0 = 0; // raw format
    static final int VERSION_1 = 1; // introduce metadata entry with ledgers map
    static final int CURRENT_VERSION = VERSION_1;

    /**
     * The 1K block at the head of the entry logger file
     * that contains the fingerprint and (future) meta-data
     */
    static final int LOGFILE_HEADER_LENGTH = 1024;

    // Metadata Entry Types
    /** EntryId used to mark an entry (belonging to INVALID_ID) as a component of the serialized ledgers map **/
    private static final long METADATA_LEDGERMAP_ENTRY = -2L;
    // Metadata Entry Header Length: length + (-1) + entry_type
    private static final int METADATA_ENTRY_HEADER_LENGTH = 4 + 8 + 8;
    private static final int MAX_LEDGERMAP_ENTRY_LENGTH = 1024;
    private static final int LEDGER_MAP_ENTRY_LENGTH = 8 + 8;

    static final byte[] MAGIC_BYTES = "BKLO".getBytes(UTF_8);
    // magic bytes + version
    static final int HEADER_VERSION_LENGTH = MAGIC_BYTES.length + 4;
    // ledgers map offset + total ledgers
    static final int HEADER_V1_LENGTH = 8 + 4;

    private static class Header {
        final int version;
        final long ledgersMapOffset;
        final int totalLedgers;

        Header(int version, long ledgersMapOffset, int totalLedgers) {
            this.version = version;
            this.ledgersMapOffset = ledgersMapOffset;
            this.totalLedgers = totalLedgers;
        }

        int getVersion() {
            return version;
        }

        long getLedgersMapOffset() {
            return ledgersMapOffset;
        }

        int getTotalLedgers() {
            return totalLedgers;
        }

        static Header read(long logId, BufferedReadChannel channel) throws IOException {
             // read the header
            ByteBuffer buffer = ByteBuffer.allocate(HEADER_VERSION_LENGTH + HEADER_V1_LENGTH);
            int numBytes = channel.read(buffer, 0L);

            if (numBytes != HEADER_VERSION_LENGTH + HEADER_V1_LENGTH) {
                throw new IOException("Short header found in entry log file " + logId);
            }

            buffer.flip();
            byte[] magicbytes = new byte[MAGIC_BYTES.length];
            buffer.get(magicbytes);
            if (!Arrays.equals(MAGIC_BYTES, magicbytes)) {
                throw new IOException("Invalid entry log file " + logId);
            }

            int version = buffer.getInt();
            long ledgersMapOffset = buffer.getLong();
            int totalLedgers = buffer.getInt();

            return new Header(version, ledgersMapOffset, totalLedgers);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("EntryLogHeader(version=")
              .append(version)
              .append(", ledgers_map_offset=")
              .append(ledgersMapOffset)
              .append(", total_ledgers=")
              .append(totalLedgers)
              .append(")");
            return sb.toString();
        }
    }

    private static class EntryLogReadChannel extends BufferedReadChannel {

        private final long logId;

        EntryLogReadChannel(long logId,
                            FileChannel fileChannel,
                            int readCapacity) throws IOException {
            super(fileChannel, readCapacity);
            this.logId = logId;
        }

        public long getLogId() {
            return logId;
        }

        public Optional<EntryLogMetadata> readEntryLogMetadata() {
            Header header;
            try {
                header = Header.read(logId, this);
            } catch (IOException ioe) {
                LOG.warn("Encountered error on reading entry log header for log {}," +
                        " falling back to scan entry log file to build entry log metadata.", logId, ioe);
                return Optional.absent();
            }

            LOG.info("Read header for entry log {} : {}", logId, header);

            if (header.getVersion() <= VERSION_0) {
                return Optional.absent();
            }

            if (header.getLedgersMapOffset() < LOGFILE_HEADER_LENGTH) {
                return Optional.absent();
            }

            if (header.getTotalLedgers() <= 0) {
                return Optional.absent();
            }

            try {
                return Optional.of(doReadEntryLogMetadata(header));
            } catch (IOException ioe) {
                LOG.warn("Encountered error on reading entry log metadata for log {}," +
                        " falling back to scan entry log file to build entry log metadata.", logId, ioe);
                return Optional.absent();
            } catch (BufferUnderflowException bue) {
                LOG.warn("Encountered error on reading entry log metadata for log {}," +
                        " falling back to scan entry log file to build entry log metadata.", logId, bue);
                return Optional.absent();
            }
        }

        private EntryLogMetadata doReadEntryLogMetadata(Header header) throws IOException {
            EntryLogMetadata metadata = new EntryLogMetadata(logId);

            ByteBuffer sizeBuf = ByteBuffer.allocate(4);

            long startOffset = header.getLedgersMapOffset();
            while (startOffset < size()) {
                // read length field
                sizeBuf.clear();
                read(sizeBuf, startOffset);
                sizeBuf.flip();

                // get ledgers map entry size
                int ledgersMapEntrySize = sizeBuf.getInt();

                // read ledgers map entry
                ByteBuffer dataBuf = ByteBuffer.allocate(ledgersMapEntrySize);
                read(dataBuf, startOffset + 4);
                dataBuf.flip();

                long metaLedgerId = dataBuf.getLong();
                long metaEntryId = dataBuf.getLong();

                if (metaLedgerId != INVALID_LID || metaEntryId != METADATA_LEDGERMAP_ENTRY) {
                    startOffset += (4 + ledgersMapEntrySize);
                    continue;
                }

                int numEntries = dataBuf.getInt();
                int numReads = 0;
                while (dataBuf.hasRemaining()) {
                    long lid = dataBuf.getLong();
                    long size = dataBuf.getLong();
                    metadata.addLedgerSize(lid, size);
                    ++numReads;
                }

                if (numEntries != numReads) {
                    throw new IOException("Invalid metadata ledgers map entry found at offset "
                            + startOffset + " for log " + logId);
                }

                startOffset += (4 + ledgersMapEntrySize);
            }

            if (header.getTotalLedgers() != metadata.getTotalLedgers()) {
                throw new IOException("Inconsistent number of ledgers detected in log " + logId
                        + " : " + header.getTotalLedgers() + " ledgers in header but "
                        + metadata.getTotalLedgers() + " found in ledgers map");
            }

            return metadata;
        }
    }

    private static class EntryLogWriteChannel extends BufferedChannel {

        private final long logId;
        private final EntryLogMetadata metadata;

        EntryLogWriteChannel(long logId,
                             FileChannel fc,
                             int writeCapacity,
                             int readCapacity)
                throws IOException {
            super(fc, writeCapacity, readCapacity);
            this.logId = logId;
            this.metadata = new EntryLogMetadata(logId);
        }

        public long getLogId() {
            return logId;
        }

        public EntryLogMetadata getMetadata() {
            return metadata;
        }

        /**
         * Write entry log header.
         *
         * @param header
         *          entry log header
         * @throws IOException
         */
        void writeHeader(ByteBuffer header) throws IOException {
            write(header);
        }

        private long writeLedgerMap() throws IOException {
            long startOffsetOfLedgerMapEntry = position();

            ByteBuffer buffer = ByteBuffer.allocate(MAX_LEDGERMAP_ENTRY_LENGTH);
            Iterator<Map.Entry<Long, Long>> entryIterator = metadata.ledgersMap.entrySet().iterator();

            int numEntries = 0;

            buffer.putInt(0);
            buffer.putLong(INVALID_LID);
            buffer.putLong(METADATA_LEDGERMAP_ENTRY);
            buffer.putInt(numEntries);

            while (entryIterator.hasNext()) {
                Map.Entry<Long, Long> entry = entryIterator.next();
                if (buffer.remaining() < LEDGER_MAP_ENTRY_LENGTH) {
                    int size = buffer.position();
                    buffer.flip();
                    buffer.putInt(0, size - 4);
                    buffer.putInt(METADATA_ENTRY_HEADER_LENGTH, numEntries);
                    write(buffer);

                    // clear the buffer
                    numEntries = 0;
                    buffer.clear();
                    buffer.putInt(0);
                    buffer.putLong(INVALID_LID);
                    buffer.putLong(METADATA_LEDGERMAP_ENTRY);
                    buffer.putInt(numEntries);
                } else {
                    buffer.putLong(entry.getKey());
                    buffer.putLong(entry.getValue());
                    ++numEntries;
                }
            }
            int size = buffer.position();
            buffer.flip();
            buffer.putInt(0, size - 4);
            buffer.putInt(METADATA_ENTRY_HEADER_LENGTH, numEntries);
            write(buffer);

            return startOffsetOfLedgerMapEntry;
        }

        void appendLedgerMap() throws IOException {
            long startOffsetOfLedgerMap = writeLedgerMap();
            // update header
            ByteBuffer header = ByteBuffer.allocate(HEADER_V1_LENGTH);
            header.putLong(startOffsetOfLedgerMap);
            header.putInt(metadata.getTotalLedgers());
            header.flip();
            long position = HEADER_VERSION_LENGTH;
            while (header.hasRemaining()) {
                position += fileChannel.write(header, position);
            }
        }

        @Override
        public String toString() {
            return String.format("EntryLog(logId = %d)", logId);
        }
    }

    volatile File currentDir;
    private final LedgerDirsManager ledgerDirsManager;
    private final AtomicBoolean shouldCreateNewEntryLog = new AtomicBoolean(false);

    private volatile long leastUnflushedLogId;
    private volatile long curLogId;
    /**
     * The maximum size of a entry logger file.
     */
    final long logSizeLimit;
    final boolean readLedgersMapEnabled;
    final boolean writeLedgersMapEnabled;
    private List<EntryLogWriteChannel> logChannelsToFlush;
    private final AtomicInteger numPendingLogFilesToFlush = new AtomicInteger(0);
    private volatile EntryLogWriteChannel logChannel;
    private final EntryLoggerAllocator entryLoggerAllocator;
    private final boolean entryLogPreAllocationEnabled;

    // Entry Log Metadata Management
    private final EntryLogMetadataManager entryLogMetadataManager;
    private final CopyOnWriteArraySet<EntryLogListener> listeners
            = new CopyOnWriteArraySet<EntryLogListener>();

    public final static long INVALID_LID = -1L;
    final ByteBuffer LOGFILE_HEADER = ByteBuffer.allocate(LOGFILE_HEADER_LENGTH);

    final static int MIN_SANE_ENTRY_SIZE = 8 + 8;
    final static long MB = 1024 * 1024;

    final ServerConfiguration serverCfg;
    /**
     * Scan entries in a entry log file.
     */
    static interface EntryLogScanner {
        /**
         * Tests whether or not the entries belongs to the specified ledger
         * should be processed.
         *
         * @param ledgerId
         *          Ledger ID.
         * @return true if and only the entries of the ledger should be scanned.
         */
        public boolean accept(long ledgerId);

        /**
         * Process an entry.
         *
         * @param ledgerId
         *          Ledger ID.
         * @param offset
         *          File offset of this entry.
         * @param entry
         *          Entry ByteBuffer
         * @throws IOException
         */
        public void process(long ledgerId, long offset, ByteBuffer entry) throws IOException;
    }

    /**
     * Entry Log Listener
     */
    static interface EntryLogListener {
        /**
         * Rotate a new entry log to write.
         */
        public void onRotateEntryLog();
    }

    /**
     * Create an EntryLogger that stores it's log files in the given
     * directories
     */
    public EntryLogger(ServerConfiguration conf,
                       LedgerDirsManager ledgerDirsManager)
            throws IOException {
        this(conf, ledgerDirsManager, null, NullStatsLogger.INSTANCE);
    }

    public EntryLogger(ServerConfiguration conf,
                       LedgerDirsManager ledgerDirsManager,
                       EntryLogListener listener,
                       StatsLogger statsLogger)
            throws IOException {
        this.ledgerDirsManager = ledgerDirsManager;
        addListener(listener);
        // log size limit
        this.logSizeLimit = Math.min(conf.getEntryLogSizeLimit(), MAX_LOG_SIZE_LIMIT);
        this.entryLogPreAllocationEnabled = conf.isEntryLogFilePreAllocationEnabled();
        this.writeLedgersMapEnabled = conf.isEntryLogWriteLedgersMapEnabled();
        this.readLedgersMapEnabled = conf.isEntryLogReadLedgersMapEnabled();
        this.entryLogMetadataManager = new EntryLogMetadataManager(statsLogger);

        // Initialize the entry log header buffer. This cannot be a static object
        // since in our unit tests, we run multiple Bookies and thus EntryLoggers
        // within the same JVM. All of these Bookie instances access this header
        // so there can be race conditions when entry logs are rolled over and
        // this header buffer is cleared before writing it into the new logChannel.
        LOGFILE_HEADER.put(MAGIC_BYTES);
        LOGFILE_HEADER.putInt(CURRENT_VERSION);
        // Find the largest logId
        long logId = -1;
        for (File dir : ledgerDirsManager.getAllLedgerDirs()) {
            if (!dir.exists()) {
                throw new FileNotFoundException(
                        "Entry log directory does not exist");
            }
            long lastLogId = getLastLogId(dir);
            if (lastLogId > logId) {
                logId = lastLogId;
            }
        }
        this.leastUnflushedLogId = logId + 1;
        this.entryLoggerAllocator = new EntryLoggerAllocator(logId);
        this.serverCfg = conf;
        initialize();

        statsLogger.registerGauge(
                NUM_PENDING_ENTRY_LOG_FILES,
                new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return numPendingLogFilesToFlush.get();
                    }
                }
        );
        statsLogger.registerGauge(
                CURRENT_ENTRYLOG_ID,
                new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return curLogId;
                    }
                }
        );
        statsLogger.registerGauge(
                LEAST_UNFLUSHED_ENTRYLOG_ID,
                new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return leastUnflushedLogId;
                    }
                }
        );

    }

    void addListener(EntryLogListener listener) {
        if (null != listener) {
            listeners.add(listener);
        }
    }

    /**
     * If the log id of current writable channel is the same as entryLogId and the position
     * we want to read might end up reading from a position in the write buffer of the
     * buffered channel, route this read to the current logChannel. Else,
     * read from the EntryLogReadChannel that is provided.
     * @param entryLogId
     * @param channel
     * @param buff remaining() on this bytebuffer tells us the last position that we
     *             expect to read.
     * @param pos The starting position from where we want to read.
     * @return
     */
    private int readFromLogChannel(long entryLogId, EntryLogReadChannel channel,
                                   ByteBuffer buff, long pos)
            throws IOException {
        EntryLogWriteChannel bc = logChannel;
        if (null != bc) {
            if (entryLogId == bc.getLogId()) {
                synchronized (bc) {
                    if (pos + buff.remaining() >= bc.getFileChannelPosition()) {
                        return bc.read(buff, pos);
                    }
                }
            }
        }
        return channel.read(buff, pos);
    }

    /**
     * A thread-local variable that wraps a mapping of log ids to bufferedchannels
     * These channels should be used only for reading. logChannel is the one
     * that is used for writes.
     */
    private final ThreadLocal<Map<Long, EntryLogReadChannel>> logid2channel
            = new ThreadLocal<Map<Long, EntryLogReadChannel>>() {
        @Override
        public Map<Long, EntryLogReadChannel> initialValue() {
            // Since this is thread local there only one modifier
            // We dont really need the concurrency, but we need to use
            // the weak values. Therefore using the concurrency level of 1
            return new MapMaker().concurrencyLevel(1)
                .weakValues()
                .makeMap();
        }
    };

    /**
     * Each thread local buffered read channel can share the same file handle because reads are not relative
     * and don't cause a change in the channel's position. We use this map to store the file channels. Each
     * file channel is mapped to a log id which represents an open log file.
     */
    private final ConcurrentMap<Long, FileChannel> logid2filechannel
            = new ConcurrentHashMap<Long, FileChannel>();

    /**
     * Put the logId, bc pair in the map responsible for the current thread.
     * @param logId
     * @param bc
     */
    public EntryLogReadChannel  putInChannels(long logId, EntryLogReadChannel bc) {
        Map<Long, EntryLogReadChannel> threadMap = logid2channel.get();
        return threadMap.put(logId, bc);
    }

    /**
     * Remove all entries for this log file in each thread's cache.
     * @param logId
     */
    public void removeFromChannelsAndClose(long logId) {
        FileChannel fileChannel = logid2filechannel.remove(logId);
        if (null != fileChannel) {
            try {
                fileChannel.close();
            } catch (IOException e) {
                LOG.warn("Exception while closing channel for log file:" + logId);
            }
        }
    }

    public EntryLogReadChannel getFromChannels(long logId) {
        return logid2channel.get().get(logId);
    }

    public EntryLogMetadataManager getEntryLogMetadataManager() {
        return entryLogMetadataManager;
    }

    /**
     * Get the least unflushed log id. Garbage collector thread should not process
     * unflushed entry log file.
     *
     * @return least unflushed log id.
     */
    synchronized long getLeastUnflushedLogId() {
        return leastUnflushedLogId;
    }

    /**
     * Get current log id.
     *
     * @return current log id.
     */
    synchronized long getCurLogId() {
        return curLogId;
    }

    protected void initialize() throws IOException {
        // Register listener for disk full notifications.
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());
        // create a new log to write
        createNewLog();
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new entry
                // log.
                if (currentDir != null && currentDir.equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskAlmostFull(File disk) {
                // If the current entry log disk is almost full, then create new entry
                // log.
                if (currentDir != null && currentDir.equals(disk)) {
                    shouldCreateNewEntryLog.set(true);
                }
            }

            @Override
            public void diskFailed(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void allDisksFull() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void fatalError() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void diskJustWritable(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    /**
     * Rolling a new log file to write.
     */
    synchronized void rollLog() throws IOException {
        createNewLog();
    }

    /**
     * Creates a new log file
     */
    void createNewLog() throws IOException {
        // first tried to create a new log channel. add current log channel to ToFlush list only when
        // there is a new log channel. it would prevent that a log channel is referenced by both
        // *logChannel* and *ToFlush* list.
        if (null != logChannel) {
            if (null == logChannelsToFlush) {
                logChannelsToFlush = new LinkedList<EntryLogWriteChannel>();
                numPendingLogFilesToFlush.set(0);
            }
            // flush the internal buffer back to filesystem but not sync disk
            // so the readers could access the data from filesystem.
            logChannel.flush(false);
            EntryLogWriteChannel newLogChannel = entryLoggerAllocator.createNewLog();
            logChannelsToFlush.add(logChannel);
            numPendingLogFilesToFlush.incrementAndGet();
            LOG.info("Flushing entry logger {} back to filesystem, pending for syncing entry loggers : {}.",
                    logChannel.getLogId(), logChannelsToFlush);
            for (EntryLogListener listener : listeners) {
                listener.onRotateEntryLog();
            }
            logChannel = newLogChannel;
        } else {
            logChannel = entryLoggerAllocator.createNewLog();
        }
        curLogId = logChannel.getLogId();
    }

    /**
     * An allocator pre-allocates entry log files.
     */
    class EntryLoggerAllocator {

        long preallocatedLogId;
        Future<EntryLogWriteChannel> preallocation = null;
        ExecutorService allocatorExecutor;

        EntryLoggerAllocator(long logId) {
            preallocatedLogId = logId;
            allocatorExecutor = Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder().setNameFormat("EntryLoggerAllocator-%d").build());
        }

        synchronized EntryLogWriteChannel createNewLog() throws IOException {
            EntryLogWriteChannel bc;
            if (!entryLogPreAllocationEnabled || null == preallocation) {
                // initialization time to create a new log
                bc = allocateNewLog();
            } else {
                // has a preallocated entry log
                try {
                    bc = preallocation.get();
                } catch (ExecutionException ee) {
                    if (ee.getCause() instanceof IOException) {
                        throw (IOException) (ee.getCause());
                    } else {
                        throw new IOException("Error to execute entry log allocation.", ee);
                    }
                } catch (CancellationException ce) {
                    throw new IOException("Task to allocate a new entry log is cancelled.", ce);
                } catch (InterruptedException ie) {
                    throw new IOException("Intrrupted when waiting a new entry log to be allocated.", ie);
                }
                preallocation = allocatorExecutor.submit(new Callable<EntryLogWriteChannel>() {
                    @Override
                    public EntryLogWriteChannel call() throws IOException {
                        return allocateNewLog();
                    }
                });
            }
            LOG.info("Created new entry logger {}.", bc.getLogId());
            return bc;
        }

        /**
         * Allocate a new log file.
         */
        EntryLogWriteChannel allocateNewLog() throws IOException {
            List<File> list = ledgerDirsManager.getWritableLedgerDirs();

            if (list.isEmpty()) {
                throw new IOException("No writable ledger directories to allocate new entry log.");
            }

            Collections.shuffle(list);
            // It would better not to overwrite existing entry log files
            File newLogFile = null;
            do {
                if (preallocatedLogId >= Integer.MAX_VALUE) {
                    preallocatedLogId = 0;
                } else {
                    ++preallocatedLogId;
                }
                String logFileName = Long.toHexString(preallocatedLogId) + ".log";
                for (File dir : list) {
                    newLogFile = new File(dir, logFileName);
                    currentDir = dir;
                    if (newLogFile.exists()) {
                        LOG.warn("Found existed entry log " + newLogFile
                               + " when trying to create it as a new log.");
                        newLogFile = null;
                        break;
                    }
                }
            } while (newLogFile == null);

            FileChannel channel = new RandomAccessFile(newLogFile, "rw").getChannel();
            EntryLogWriteChannel logChannel = new EntryLogWriteChannel(preallocatedLogId, channel,
                    serverCfg.getWriteBufferBytes(), serverCfg.getReadBufferBytes());
            logChannel.writeHeader((ByteBuffer) LOGFILE_HEADER.clear());

            for (File f : list) {
                try {
                    setLastLogId(f, preallocatedLogId);
                } catch (IOException ioe) {
                    LOG.warn("Failed to write lastId {} to directory {} : ",
                             new Object[] { preallocatedLogId, f, ioe });
                }
            }

            LOG.info("Preallocated entry logger {}.", preallocatedLogId);
            return logChannel;
        }

        /**
         * Stop the allocator.
         */
        void stop() {
            // wait until the preallocation finished.
            allocatorExecutor.shutdown();
            LOG.info("Stopped entry logger preallocator.");
        }
    }

    /**
     * Remove entry log.
     *
     * @param entryLogId
     *          Entry Log File Id
     */
    protected boolean removeEntryLog(long entryLogId) {
        removeFromChannelsAndClose(entryLogId);
        File entryLogFile;
        try {
            entryLogFile = findFile(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.error("Trying to delete an entryLog file that could not be found: "
                    + entryLogId + ".log");
            return false;
        }
        if (!entryLogFile.delete()) {
            LOG.warn("Could not delete entry log file {}", entryLogFile);
        }
        return true;
    }

    /**
     * writes the given id to the "lastId" file in the given directory.
     */
    protected void setLastLogId(File dir, long logId) throws IOException {
        FileOutputStream fos;
        fos = new FileOutputStream(new File(dir, "lastId"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos, UTF_8));
        try {
            bw.write(Long.toHexString(logId) + "\n");
            bw.flush();
        } finally {
            try {
                bw.close();
            } catch (IOException e) {
                LOG.error("Could not close lastId file in {}", dir.getPath());
            }
        }
    }

    private long getLastLogId(File dir) {
        long id = readLastLogId(dir);
        // read success
        if (id > 0) {
            return id;
        }
        // read failed, scan the ledger directories to find biggest log id
        File[] logFiles = dir.listFiles(new FileFilter() {
            @Override
            public boolean accept(File file) {
                return file.getName().endsWith(".log");
            }
        });
        List<Long> logs = new ArrayList<Long>();
        for (File lf : logFiles) {
            String idString = lf.getName().split("\\.")[0];
            try {
                long lid = Long.parseLong(idString, 16);
                logs.add(lid);
            } catch (NumberFormatException nfe) {
            }
        }
        // no log file found in this directory
        if (0 == logs.size()) {
            return -1;
        }
        // order the collections
        Collections.sort(logs);
        return logs.get(logs.size() - 1);
    }

    /**
     * reads id from the "lastId" file in the given directory.
     */
    private long readLastLogId(File f) {
        FileInputStream fis;
        try {
            fis = new FileInputStream(new File(f, "lastId"));
        } catch (FileNotFoundException e) {
            return -1;
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, UTF_8));
        try {
            String lastIdString = br.readLine();
            return Long.parseLong(lastIdString, 16);
        } catch (IOException e) {
            return -1;
        } catch(NumberFormatException e) {
            return -1;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
            }
        }
    }

    /**
     * Flushes all rotated log channels. After log channels are flushed,
     * move leastUnflushedLogId ptr to current logId.
     */
    void checkpoint() throws IOException {
        flushRotatedLogs();
    }

    void flushRotatedLogs() throws IOException {
        List<EntryLogWriteChannel> channels = null;
        long flushedLogId = -1;
        synchronized (this) {
            channels = logChannelsToFlush;
            logChannelsToFlush = null;
            numPendingLogFilesToFlush.set(0);
        }
        if (null == channels) {
            return;
        }
        for (EntryLogWriteChannel channel : channels) {
            if (writeLedgersMapEnabled) {
                channel.appendLedgerMap();
            }
            channel.flush(true);
            entryLogMetadataManager.addEntryLogMetadata(channel.getMetadata());
            // since this channel is only used for writing, after flushing the channel,
            // we had to close the underlying file channel. Otherwise, we might end up
            // leaking fds which cause the disk spaces could not be reclaimed.
            closeFileChannel(channel);
            if (channel.getLogId() > flushedLogId) {
                flushedLogId = channel.getLogId();
            }
            LOG.info("Synced entry logger {} to disk.", channel.getLogId());
        }
        // move the leastUnflushedLogId ptr
        leastUnflushedLogId = flushedLogId + 1;
    }

    void flush() throws IOException {
        flushRotatedLogs();
        flushCurrentLog();
    }

    synchronized void flushCurrentLog() throws IOException {
        if (logChannel != null) {
            logChannel.flush(true);
            LOG.info("Flush and sync current entry logger {}.", logChannel.getLogId());
        }
    }

    long addEntry(long ledgerId, ByteBuffer entry) throws IOException {
        return addEntry(ledgerId, entry, true);
    }

    synchronized long addEntry(long ledgerId, ByteBuffer entry, boolean rollLog) throws IOException {
        int entrySize = entry.remaining() + 4;
        boolean reachEntryLogLimit =
                rollLog ? reachEntryLogLimit(entrySize) : reachEntryLogHardLimit(entrySize);
        // Create new log if logSizeLimit reached or current disk is full
        boolean createNewLog = shouldCreateNewEntryLog.get();
        if (createNewLog || reachEntryLogLimit) {
            createNewLog();

            // Reset the flag
            if (createNewLog) {
                shouldCreateNewEntryLog.set(false);
            }
        }
        ByteBuffer buff = ByteBuffer.allocate(4);
        buff.putInt(entry.remaining());
        buff.flip();
        logChannel.write(buff);

        int entryLength = entry.remaining() + 4;
        long pos = logChannel.position();
        logChannel.write(entry);
        logChannel.getMetadata().addLedgerSize(ledgerId, entryLength);
        return (logChannel.getLogId() << 32L) | pos;
    }

    static long logIdForOffset(long offset) {
        return offset >> 32L;
    }

    synchronized boolean reachEntryLogLimit(long size) {
        return logChannel.position() + size > logSizeLimit;
    }

    synchronized boolean reachEntryLogHardLimit(long size) {
        return logChannel.position() + size > Integer.MAX_VALUE;
    }

    byte[] readEntry(long ledgerId, long entryId, long location) throws IOException, Bookie.NoEntryException {
        long entryLogId = location >> 32L;
        long pos = location & 0xffffffffL;
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        pos -= 4; // we want to get the ledgerId and length to check
        EntryLogReadChannel fc;
        try {
            fc = getChannelForLogId(entryLogId);
        } catch (FileNotFoundException e) {
            LOG.warn("{} on reading (lid={}, eid={}) from location {} @ log {}",
                    new Object[]{ e.getMessage(), ledgerId, entryId, location, entryLogId });
            throw new Bookie.NoEntryException(e.getMessage(), ledgerId, entryId);
        }
        if (readFromLogChannel(entryLogId, fc, sizeBuff, pos) != sizeBuff.capacity()) {
            throw new Bookie.NoEntryException("Short read from entrylog " + entryLogId,
                                              ledgerId, entryId);
        }
        pos += 4;
        sizeBuff.flip();
        int entrySize = sizeBuff.getInt();
        // entrySize does not include the ledgerId
        if (entrySize > MB) {
            LOG.error("Sanity check failed for entry size of " + entrySize + " at location " + pos + " in " + entryLogId);
        }
        if (entrySize < MIN_SANE_ENTRY_SIZE) {
            LOG.warn("Read invalid entry length {} found for lid={}, eid={} in log {} @offset {}",
                    new Object[] { entrySize, ledgerId, entryId, entryLogId, pos });
            throw new IOException("Invalid entry length " + entrySize + " found for lid=" + ledgerId
                    + ", eid=" + entryId + " in log " + entryLogId + " @offset " + pos);
        }
        byte data[] = new byte[entrySize];
        ByteBuffer buff = ByteBuffer.wrap(data);
        int rc = readFromLogChannel(entryLogId, fc, buff, pos);
        if ( rc != data.length) {
            // Note that throwing NoEntryException here instead of IOException is not
            // without risk. If all bookies in a quorum throw this same exception
            // the client will assume that it has reached the end of the ledger.
            // However, this may not be the case, as a very specific error condition
            // could have occurred, where the length of the entry was corrupted on all
            // replicas. However, the chance of this happening is very very low, so
            // returning NoEntryException is mostly safe.
            throw new Bookie.NoEntryException("Short read for " + ledgerId + "@"
                                              + entryId + " in " + entryLogId + "@"
                                              + pos + "("+rc+"!="+data.length+")", ledgerId, entryId);
        }
        buff.flip();
        long thisLedgerId = buff.getLong();
        if (thisLedgerId != ledgerId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry belongs to " + thisLedgerId + " not " + ledgerId);
        }
        long thisEntryId = buff.getLong();
        if (thisEntryId != entryId) {
            throw new IOException("problem found in " + entryLogId + "@" + entryId + " at position + " + pos + " entry is " + thisEntryId + " not " + entryId);
        }

        return data;
    }

    private EntryLogReadChannel getChannelForLogId(long entryLogId) throws IOException {
        EntryLogReadChannel fc = getFromChannels(entryLogId);
        if (fc != null) {
            return fc;
        }
        File file = findFile(entryLogId);
        // get channel is used to open an existing entry log file
        // it would be better to open using read mode
        FileChannel newFc = new RandomAccessFile(file, "r").getChannel();
        FileChannel oldFc = logid2filechannel.putIfAbsent(entryLogId, newFc);
        if (null != oldFc) {
            newFc.close();
            newFc = oldFc;
        }
        // We set the position of the write buffer of this buffered channel to Long.MAX_VALUE
        // so that there are no overlaps with the write buffer while reading
        fc = new EntryLogReadChannel(entryLogId, newFc, serverCfg.getReadBufferBytes());
        putInChannels(entryLogId, fc);
        return fc;
    }

    /**
     * Whether the log file exists or not.
     */
    boolean logExists(long logId) {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId) + ".log");
            if (f.exists()) {
                return true;
            }
        }
        return false;
    }

    private File findFile(long logId) throws FileNotFoundException {
        for (File d : ledgerDirsManager.getAllLedgerDirs()) {
            File f = new File(d, Long.toHexString(logId)+".log");
            if (f.exists()) {
                return f;
            }
        }
        throw new FileNotFoundException("No file for log " + Long.toHexString(logId));
    }

    protected Optional<EntryLogMetadata> extractEntryLogMetadataFromIndex(long entryLogId) throws IOException {
        EntryLogReadChannel bc = getChannelForLogId(entryLogId);
        return bc.readEntryLogMetadata();
    }

    protected EntryLogMetadata extractEntryLogMetadata(long entryLogId) throws IOException {
        EntryLogMetadata metadata = entryLogMetadataManager.getEntryLogMetadata(entryLogId);

        if (metadata != null) {
            LOG.info("Return entry log metadata for {} : ", entryLogId, metadata);
            return metadata;
        }

        try {
            if (readLedgersMapEnabled) {
                Optional<EntryLogMetadata> metadataOptional = extractEntryLogMetadataFromIndex(entryLogId);
                if (metadataOptional.isPresent()) {
                    metadata = metadataOptional.get();
                    return metadata;
                }
            }
            // scanning the log to extract metadata
            metadata = extractEntryLogMetadataByScanning(entryLogId);
            return metadata;
        } finally {
            if (null != metadata) {
                entryLogMetadataManager.addEntryLogMetadata(metadata);
            }
        }
    }

    /**
     * Scan entry log
     *
     * @param entryLogId
     *          Entry Log Id
     * @param scanner
     *          Entry Log Scanner
     * @throws IOException
     */
    protected void scanEntryLog(long entryLogId, EntryLogScanner scanner) throws IOException {
        ByteBuffer sizeBuff = ByteBuffer.allocate(4);
        ByteBuffer lidBuff = ByteBuffer.allocate(8);
        EntryLogReadChannel bc;
        // Get the BufferedChannel for the current entry log file
        try {
            bc = getChannelForLogId(entryLogId);
        } catch (IOException e) {
            LOG.warn("Failed to get channel to scan entry log: " + entryLogId + ".log");
            throw e;
        }
        // Start the read position in the current entry log file to be after
        // the header where all of the ledger entries are.
        long pos = LOGFILE_HEADER_LENGTH;
        // Read through the entry log file and extract the ledger ID's.
        while (true) {
            // Check if we've finished reading the entry log file.
            if (pos >= bc.size()) {
                break;
            }
            if (readFromLogChannel(entryLogId, bc, sizeBuff, pos) != sizeBuff.capacity()) {
                throw new ShortReadException("Short read for entry size from entrylog " + entryLogId);
            }
            long offset = pos;
            pos += 4;
            sizeBuff.flip();
            int entrySize = sizeBuff.getInt();
            if (entrySize > MB) {
                LOG.warn("Found large size entry of " + entrySize + " at location " + pos + " in "
                        + entryLogId);
            }
            if (entrySize < 0) {
                throw new ShortReadException("Invalid entry size found for entry from entryLog " + entryLogId
                                    + "@" + pos + " : " + entrySize);
            }
            sizeBuff.clear();
            // try to read ledger id first
            if (readFromLogChannel(entryLogId, bc, lidBuff, pos) != lidBuff.capacity()) {
                throw new ShortReadException("Short read for ledger id from entrylog " + entryLogId);
            }
            lidBuff.flip();
            long lid = lidBuff.getLong();
            lidBuff.clear();
            if (lid == INVALID_LID || !scanner.accept(lid)) {
                // skip this entry
                pos += entrySize;
                continue;
            }
            // read the entry
            byte data[] = new byte[entrySize];
            ByteBuffer buff = ByteBuffer.wrap(data);
            int rc = readFromLogChannel(entryLogId, bc, buff, pos);
            if (rc != data.length) {
                throw new ShortReadException("Short read for ledger entry from entryLog " + entryLogId
                                    + "@" + pos + "(" + rc + "!=" + data.length + ")");
            }
            buff.flip();
            // process the entry
            scanner.process(lid, offset, buff);
            // Advance position to the next entry
            pos += entrySize;
        }
    }

    /**
     * A scanner used to extract entry log meta from entry log files.
     */
    static class ExtractionScanner implements EntryLogScanner {
        EntryLogMetadata meta;

        public ExtractionScanner(EntryLogMetadata meta) {
            this.meta = meta;
        }

        @Override
        public boolean accept(long ledgerId) {
            return ledgerId != EntryLogger.INVALID_LID;
        }
        @Override
        public void process(long ledgerId, long offset, ByteBuffer entry) {
            // add new entry size of a ledger to entry log meta
            meta.addLedgerSize(ledgerId, entry.limit() + 4);
        }
    }

    private EntryLogMetadata extractEntryLogMetadataByScanning(long entryLogId)
            throws IOException {
        EntryLogMetadata entryLogMeta = new EntryLogMetadata(entryLogId);
        ExtractionScanner scanner = new ExtractionScanner(entryLogMeta);
        // Read through the entry log file and extract the entry log meta
        try {
            scanEntryLog(entryLogId, scanner);
        } catch (ShortReadException sre) {
            // short read exception, it means that the last entry in entry logger is corrupted due to
            // an unsuccessful shutdown (e.g kill -9 or power off)
            LOG.warn("Short read on retrieving entry log metadata for {} : ", entryLogId, sre);
        }
        LOG.info("Retrieved entry log meta data entryLogId: {}, meta: {}", entryLogId, entryLogMeta);
        return entryLogMeta;
    }

    /**
     * Shutdown method to gracefully stop entry logger.
     */
    public void shutdown() {
        // since logChannel is buffered channel, do flush when shutting down
        try {
            flush();
            for (FileChannel fc : logid2filechannel.values()) {
                fc.close();
            }
            // clear the mapping, so we don't need to go through the channels again in finally block in normal case.
            logid2filechannel.clear();
            // close current writing log file
            closeFileChannel(logChannel);
            logChannel = null;
        } catch (IOException ie) {
            // we have no idea how to avoid io exception during shutting down, so just ignore it
            LOG.error("Error flush entry log during shutting down, which may cause entry log corrupted.", ie);
        } finally {
            for (FileChannel fc : logid2filechannel.values()) {
                IOUtils.close(LOG, fc);
            }
            forceCloseFileChannel(logChannel);
        }
        // shutdown the pre-allocation thread
        entryLoggerAllocator.stop();
    }

    private static void closeFileChannel(BufferedChannelBase channel) throws IOException {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            fileChannel.close();
        }
    }

    private static void forceCloseFileChannel(BufferedChannelBase channel) {
        if (null == channel) {
            return;
        }
        FileChannel fileChannel = channel.getFileChannel();
        if (null != fileChannel) {
            IOUtils.close(LOG, fileChannel);
        }
    }

}
