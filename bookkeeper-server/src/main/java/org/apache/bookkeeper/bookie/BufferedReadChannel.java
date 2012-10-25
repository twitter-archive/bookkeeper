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

import java.io.IOError;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.lang.Math;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Buffered channel without a write buffer. Only reads are buffered.
 */
public class BufferedReadChannel {
    private static final Logger LOG = LoggerFactory.getLogger(EntryLogger.class);

    final FileChannel fileChannel;
    final int capacity;
    ByteBuffer readBuffer;
    // The start position of the data currently in the read buffer.
    long readBufferStartPosition = Long.MIN_VALUE;
    long invocationCount = 0;
    long cacheHitCount = 0;

    public BufferedReadChannel(FileChannel fileChannel, int capacity) throws IOException {
        this.fileChannel = fileChannel;
        this.capacity = capacity;
        this.readBuffer = ByteBuffer.allocateDirect(capacity);
        this.readBuffer.limit(0);
    }

    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    private FileChannel validateAndGetFileChannel() throws IOException {
        // Even if we have BufferedReadChannel objects in the cache, higher layers should
        // guarantee that once a log file has been closed and possibly deleted during garbage
        // collection, attempts will not be made to read from it
        if (!fileChannel.isOpen()) {
            throw new IOException("Attempting to access a file channel that has already been closed");
        }

        return fileChannel;
    }

    public long size() throws IOException {
        return validateAndGetFileChannel().size();
    }

    synchronized public int read(ByteBuffer buff, long pos) throws IOException {
        invocationCount++;
        long currentPosition = pos;
        while (buff.remaining() > 0) {
            // Check if the data is in the buffer, if so, copy it.
            if (readBufferStartPosition <= currentPosition && currentPosition < readBufferStartPosition + readBuffer.limit()) {
                long posInBuffer = currentPosition - readBufferStartPosition;
                long bytesToCopy = Math.min(buff.remaining(), readBuffer.limit() - posInBuffer);
                ByteBuffer rbDup = readBuffer.duplicate();
                rbDup.position((int)posInBuffer);
                rbDup.limit((int)(posInBuffer + bytesToCopy));
                buff.put(rbDup);
                currentPosition += bytesToCopy;
                cacheHitCount++;
            } else {
                // We don't have it in the buffer, so put necessary data in the buffer
                readBuffer.clear();
                readBufferStartPosition = currentPosition;
                int readBytes = 0;
                if ((readBytes = validateAndGetFileChannel().read(readBuffer, currentPosition)) <= 0) {
                    throw new IOException("Reading from filechannel returned a non-positive value. Short read.");
                }
                readBuffer.limit(readBytes);
            }
        }
        return (int)(currentPosition - pos);
    }

    synchronized public void clear() {
        readBuffer.clear();
        readBuffer.limit(0);
    }

    private boolean randomSample (int percent)
    {
        return (Math.random()*100 < percent);
    }

    protected void finalize () {
        // To avoid too much logging lets do it only 10% of the times
        if (randomSample(10)) {
            LOG.info("Buffer Cache Hit Rate: #invocations:" + invocationCount + " #readCacheHits:" + cacheHitCount);
        }
    }
}
