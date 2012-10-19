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

/**
 * A Buffered channel without a write buffer. Only reads are buffered.
 */
public class BufferedReadChannel {
    final FileChannel bc;
    final int capacity;
    ByteBuffer readBuffer;
    // The start position of the data currently in the read buffer.
    long readBufferStartPosition = Long.MIN_VALUE;

    public BufferedReadChannel(FileChannel bc, int capacity) throws IOException {
        this.bc = bc;
        this.capacity = capacity;
        this.readBuffer = ByteBuffer.allocateDirect(capacity);
        this.readBuffer.limit(0);
    }

    public FileChannel getFileChannel() {
        return this.bc;
    }

    public long size() throws IOException {
        return bc.size();
    }

    synchronized public int read(ByteBuffer buff, long pos) throws IOException {
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
            } else {
                // We don't have it in the buffer, so put necessary data in the buffer
                readBuffer.clear();
                readBufferStartPosition = currentPosition;
                int readBytes = 0;
                if ((readBytes = bc.read(readBuffer, currentPosition)) <= 0) {
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
}
