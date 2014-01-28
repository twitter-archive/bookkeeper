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

import java.util.Arrays;

import java.io.Closeable;
import java.io.File;
import java.io.RandomAccessFile;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.apache.bookkeeper.util.NativeIO;
import org.apache.bookkeeper.util.ZeroBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple wrapper around FileChannel to add versioning
 * information to the file.
 */
class JournalChannel implements Closeable {
    static Logger LOG = LoggerFactory.getLogger(JournalChannel.class);

    final RandomAccessFile randomAccessFile;
    final int fd;
    final FileChannel fc;
    final BufferedChannel bc;
    final int formatVersion;
    long nextPrealloc = 0;

    final byte[] MAGIC_WORD = "BKLG".getBytes();

    final static int SECTOR_SIZE = 512;
    private final static int START_OF_FILE = -12345;
    private static long CACHE_DROP_LAG_BYTES = 8 * 1024 * 1024;

    // No header
    static final int V1 = 1;
    // Adding header
    static final int V2 = 2;
    // Adding ledger key
    static final int V3 = 3;
    // Adding fencing key
    static final int V4 = 4;
    // 1) expanding header to 512
    // 2) Padding writes to align sector size
    static final int V5 = 5;

    static final int HEADER_SIZE = SECTOR_SIZE; // align header to sector size
    static final int VERSION_HEADER_SIZE = 8; // 4byte magic word, 4 byte version
    static final int MIN_COMPAT_JOURNAL_FORMAT_VERSION = V1;
    static final int CURRENT_JOURNAL_FORMAT_VERSION = V5;

    private final long preAllocSize;
    private final boolean fRemoveFromPageCache;
    public final ByteBuffer zeros = ByteBuffer.allocate(SECTOR_SIZE);

    // The position of the file channel's last drop position
    private long lastDropPosition = 0L;

    // Mostly used by tests
    JournalChannel(File journalDirectory, long logId) throws IOException {
        this(journalDirectory, logId, 4*1024*1024, 65536, START_OF_FILE);
    }

    JournalChannel(File journalDirectory, long logId, long preAllocSize, int writeBufferSize) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, START_OF_FILE);
    }

    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, long position) throws IOException {
         this(journalDirectory, logId, preAllocSize, writeBufferSize, position, false);
    }

    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, boolean fRemoveFromPageCache) throws IOException {
        this(journalDirectory, logId, preAllocSize, writeBufferSize, START_OF_FILE, fRemoveFromPageCache);
    }

    JournalChannel(File journalDirectory, long logId,
                   long preAllocSize, int writeBufferSize, long position, boolean fRemoveFromPageCache) throws IOException {
        this.preAllocSize = preAllocSize - preAllocSize % SECTOR_SIZE;
        this.fRemoveFromPageCache = fRemoveFromPageCache;
        File fn = new File(journalDirectory, Long.toHexString(logId) + ".txn");

        LOG.info("Opening journal {}", fn);
        if (!fn.exists()) { // new file, write version
            randomAccessFile = new RandomAccessFile(fn, "rw");
            fc = randomAccessFile.getChannel();
            formatVersion = CURRENT_JOURNAL_FORMAT_VERSION;

            ByteBuffer bb = ByteBuffer.allocate(HEADER_SIZE);
            ZeroBuffer.put(bb);
            bb.clear();
            bb.put(MAGIC_WORD);
            bb.putInt(formatVersion);
            bb.clear();
            fc.write(bb);

            bc = new BufferedChannel(fc, writeBufferSize);
            forceWrite(true);
            nextPrealloc = this.preAllocSize;
            fc.write(zeros, nextPrealloc - SECTOR_SIZE);
        } else {  // open an existing file
            randomAccessFile = new RandomAccessFile(fn, "r");
            fc = randomAccessFile.getChannel();
            bc = null; // readonly

            ByteBuffer bb = ByteBuffer.allocate(VERSION_HEADER_SIZE);
            int c = fc.read(bb);
            bb.flip();

            if (c == VERSION_HEADER_SIZE) {
                byte[] first4 = new byte[4];
                bb.get(first4);

                if (Arrays.equals(first4, MAGIC_WORD)) {
                    formatVersion = bb.getInt();
                } else {
                    // pre magic word journal, reset to 0;
                    formatVersion = V1;
                }
            } else {
                // no header, must be old version
                formatVersion = V1;
            }

            if (formatVersion < MIN_COMPAT_JOURNAL_FORMAT_VERSION
                || formatVersion > CURRENT_JOURNAL_FORMAT_VERSION) {
                String err = String.format("Invalid journal version, unable to read."
                        + " Expected between (%d) and (%d), got (%d)",
                        MIN_COMPAT_JOURNAL_FORMAT_VERSION, CURRENT_JOURNAL_FORMAT_VERSION,
                        formatVersion);
                LOG.error(err);
                throw new IOException(err);
            }

            try {
                if (position == START_OF_FILE) {
                    if (formatVersion >= V5) {
                        fc.position(HEADER_SIZE);
                    } else if (formatVersion >= V2) {
                        fc.position(VERSION_HEADER_SIZE);
                    } else {
                        fc.position(0);
                    }
                } else {
                    fc.position(position);
                }
            } catch (IOException e) {
                LOG.error("Bookie journal file can seek to position :", e);
            }
        }
        this.fd = NativeIO.getSysFileDescriptor(randomAccessFile.getFD());
    }

    int getFormatVersion() {
        return formatVersion;
    }

    BufferedChannel getBufferedChannel() throws IOException {
        if (bc == null) {
            throw new IOException("Read only journal channel");
        }
        return bc;
    }

    void preAllocIfNeeded(long size) throws IOException {
        preAllocIfNeeded(size, null);
    }

    void preAllocIfNeeded(long size, Stopwatch stopwatch) throws IOException {
        if (bc.position() + size > nextPrealloc) {
            if (null != stopwatch) {
                stopwatch.reset().start();
            }
            nextPrealloc += preAllocSize;
            zeros.clear();
            fc.write(zeros, nextPrealloc - SECTOR_SIZE);
            if (null != stopwatch) {
                ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(
                        BookkeeperServerStatsLogger.BookkeeperServerOp.JOURNAL_PREALLOCATION)
                        .registerSuccessfulEvent(stopwatch.stop().elapsed(TimeUnit.MICROSECONDS));
            }
        }
    }

    int read(ByteBuffer dst)
            throws IOException {
        return fc.read(dst);
    }

    public void close() throws IOException {
        fc.close();
    }

    public void forceWrite(boolean forceMetadata) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Journal ForceWrite");
        }
        ServerStatsProvider.getStatsLoggerInstance().getCounter(
                BookkeeperServerStatsLogger.BookkeeperServerCounter.JOURNAL_NUM_FORCE_WRITES).inc();
        long newForceWritePosition = bc.forceWrite(forceMetadata);
        //
        // For POSIX_FADV_DONTNEED, we want to drop from the beginning
        // of the file to a position prior to the current position.
        //
        // The CACHE_DROP_LAG_BYTES is to prevent dropping a page that will
        // be appended again, which would introduce random seeking on journal
        // device.
        //
        // <======== drop ==========>
        //                           <-----------LAG------------>
        // +------------------------+---------------------------O
        // lastDropPosition     newDropPos             lastForceWritePosition
        //
        if (fRemoveFromPageCache) {
            long newDropPos = newForceWritePosition - CACHE_DROP_LAG_BYTES;
            if (lastDropPosition < newDropPos) {
                NativeIO.bestEffortRemoveFromPageCache(fd, lastDropPosition, newDropPos - lastDropPosition);
            }
            this.lastDropPosition = newDropPos;
        }
    }
}
