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
package org.apache.bookkeeper.bookie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Reorder writes so that writes to a particular ledger are roughly grouped together. Allocates chunks to ledgers
 * on each write that doesn't fit in the previous chunk.
 */
public class BufferedReorderedWriteChannel extends BufferedChannel {

    private static Logger logger = LoggerFactory.getLogger(BufferedReorderedWriteChannel.class);
    public static int FAKE_CEILING_BYTES = 12;
    /**
     * A class representing the next write position for each key (ledger id) and the absolute ceiling
     * that it should not cross.
     */
    private static class OrderedValue {
        // The absolute starting position for the next write operation
        private long nextWritePosition;
        // The absolute position of the first unwritable byte in the buffer.
        private long ceilPosition;
        private OrderedValue(long writeStart, long ceiling) {
            this.nextWritePosition = writeStart;
            this.ceilPosition = ceiling;
        }
    }

    /**
     * Keeps necessary information to calculate the chunk size that is returned on every invocation
     * of getChunkSize().
     *
     * Package visibility for unit tests.
     * TODO: Change behavior so that at least 90%(or any configurable value) of the keys fit in one chunk.
     */
    static class ChunkSizeCalculator {
        final int minChunkSizeBytes;

        ChunkSizeCalculator(int minChunkSizeBytes) {
            this.minChunkSizeBytes = minChunkSizeBytes;
        }

        /**
         * Calculate and return the chunk size for the next round of writes.
         * @return
         */
        int getChunkSize() {
            // For now return the min size.
            return minChunkSizeBytes;
        }

        /**
         * Register that a write of size writeSize was done for key.
         * @param key
         * @param writeSize
         */
        void registerWriteForKey(long key, int writeSize) {
            // no-op for now.
        }
    }

    // The current chunk size.
    private int currentChunkSize;

    private final ChunkSizeCalculator chunkCalc;

    // Maps the write positions for each key.
    private final ConcurrentMap<Long, OrderedValue> writePositionMap;

    /**
     * We reserve 8 bytes in each chunk to pad the last entry. 4 bytes for the size
     * and 8 for the ledger id which we will tag as -1L. The fake
     * ceiling is the actual ceiling-12
     * @param chunk
     * @param writeSize
     * @return
     */
    private boolean exceedsCeiling(OrderedValue chunk, int writeSize) {
        return chunk.nextWritePosition+writeSize > chunk.ceilPosition;
    }

    /**
     * Pad the current chunk till it's ceiling with a fake entry. The ledger id
     * for this entry will be 0xffffffff. We do this to make scanning easier.
     * @param chunk
     */
    synchronized private void padChunk(OrderedValue chunk) {
        // The entry is of the form SIZE|LID|DATA. SIZE is the total number of bytes
        // of the DATA along with the 8 byte LID.
        int sizeToWrite = (int)(chunk.ceilPosition - chunk.nextWritePosition - 4);
        setWriteBufferPos(chunk.nextWritePosition);
        if (logger.isDebugEnabled()) {
            logger.debug("Padding chunk at:" + chunk.nextWritePosition + " size:" + sizeToWrite +
                    " ledger:" + EntryLogger.INVALID_LID);
        }
        writeBuffer.putInt(sizeToWrite);
        writeBuffer.putLong(EntryLogger.INVALID_LID);
        chunk.nextWritePosition = chunk.ceilPosition;
        setWriteBufferPos(this.position);
    }

    /**
     * Allocate a new chunk that fits writeSize bytes. If no chunk can
     * be allocated, write to the file but don't fsync.
     *
     * NOTE: writeSize should not exceed the writeCapacity of this buffered channel.
     * @param writeSize in bytes
     * @return
     */
    synchronized private OrderedValue allocateChunk(int writeSize) throws IOException {
        int numChunks = (writeSize/currentChunkSize) + (writeSize%currentChunkSize>0?1:0);
        int bytesToAlloc = numChunks*this.currentChunkSize;
        // If we are exceeding the bytebuffer's capacity, write to the filechannel.
        // this.position points to the start of the next chunk
        if (this.position + bytesToAlloc - this.writeBufferStartPosition > this.writeCapacity) {
            // This will reset position and writeBufferStartPosition to point to the same value.
            flush(false);
            this.currentChunkSize = this.chunkCalc.getChunkSize();
            // The chunk size could have changed, so do the calculations again.
            return allocateChunk(writeSize);
        }
        // The absolute start of the chunk is at 'position' and the ceiling will depend
        // on the number of chunks we're allocating.
        OrderedValue retVal = new OrderedValue(position, position + bytesToAlloc);
        this.position += bytesToAlloc;
        setWriteBufferPos(this.position);
        return retVal;
    }

    /**
     * Get the chunk to write a blob of writeSize for key. Allocate a new chunk if it is not
     * allocated or if the writeSize would cause it to write beyond it's ceiling, allocate a new
     * chunk. If no more chunks can be allocated, write to the backing file channel but don't fsync.
     *
     * @param key
     * @param writeSize in bytes
     * @return
     */
    synchronized private OrderedValue getChunk(long key, int writeSize) throws IOException {
        // Make sure that there is place left for the fake entry.
        int modifiedWriteSize = writeSize + FAKE_CEILING_BYTES;
        OrderedValue currentValue = writePositionMap.get(key);
        // We need to allocate a new chunk if we don't have one allocated
        // or if we are exceeding the limit of the currently allocated chunk
        if (null == currentValue || exceedsCeiling(currentValue, modifiedWriteSize)) {
            currentValue = allocateChunk(modifiedWriteSize);
            OrderedValue prevValue = this.writePositionMap.put(key, currentValue);
            if (null != prevValue) {
                // Pad any previously mapped chunks.
                padChunk(prevValue);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Returning chunk with start:" + currentValue.nextWritePosition + ", ceiling:" + currentValue
                    .ceilPosition);
        }
        return currentValue;
    }

    synchronized private void setWriteBufferPos(long absolutePosition) {
        this.writeBuffer.position((int)(absolutePosition - this.writeBufferStartPosition));
    }

    public BufferedReorderedWriteChannel(FileChannel fc, int writeCapacity, int readCapacity,
                                         int minChunkSize) throws IOException {
        super(fc, writeCapacity, readCapacity);
        this.chunkCalc = new ChunkSizeCalculator(minChunkSize);
        this.writePositionMap = new ConcurrentHashMap<Long, OrderedValue>();
        this.currentChunkSize = this.chunkCalc.getChunkSize();
    }

    /**
     * Write all the data in src to the backing bytebuffer. Reorder the writes so that
     * writes to each ledger are clubbed together. Acquire chunks as and when required
     * to facilitate this. key is the ordering key, which is the ledger id.
     * @param key
     * @param src
     * @return The absolute start position of the write.
     */
    synchronized public long write(long key, ByteBuffer src) throws IOException {
        int bytesToWrite = src.remaining();
        if (bytesToWrite > this.writeCapacity - FAKE_CEILING_BYTES) {
            throw new IOException("Writing a value of size:" + src.remaining() + " to the byte buffer " +
                    "that is larger than the buffer's capacity:" + this.writeCapacity);
        }
        OrderedValue chunk = getChunk(key, bytesToWrite);
        // Set the write buffer's position to the start position of the next write to this chunk and write
        // the bytes in src to the bytebuffer. getChunk() ensures that all the remaining bytes in src
        // will be written to the buffer.
        long absoluteWritePosition = chunk.nextWritePosition;
        setWriteBufferPos(chunk.nextWritePosition);
        this.writeBuffer.put(src);
        // Reset this to point to the actual position.
        setWriteBufferPos(this.position);
        // Update the chunk to make sure the next write position is updated.
        chunk.nextWritePosition += bytesToWrite;
        // Register this write with the chunk calculator.
        this.chunkCalc.registerWriteForKey(key, bytesToWrite);
        if (logger.isDebugEnabled()) {
            logger.debug("Writing to ledgerId:" + key + ". Wrote at absolute position:"
                    + absoluteWritePosition + " an entry of size:" + bytesToWrite);
        }
        return absoluteWritePosition;
    }

    @Override
    public void flush(boolean shouldForceWrite) throws IOException {
        // We are going to reset the write buffer start position. Invalidate any outstanding OrderedValues
        // in the writePositionMap so that any new call to getChunk would have to acquire a new chunk
        synchronized (this) {
            // Pad all outstanding chunks.
            if (logger.isDebugEnabled()) {
                logger.debug("Flushing outstanding entries. Total number:" + writePositionMap.size());
            }
            for (ConcurrentMap.Entry<Long, OrderedValue> entry : writePositionMap.entrySet()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Padding chunk for key:" + entry.getKey());
                }
                padChunk(entry.getValue());
            }
            writePositionMap.clear();
            // Reset the position of the write buffer to reflect the last allocated chunk.
            if (logger.isDebugEnabled()) {
                logger.debug("Flushing at position:" + this.position);
            }
            setWriteBufferPos(this.position);
            writeBuffer.flip();
            fileChannel.write(writeBuffer);
            writeBuffer.clear();
            writeBufferStartPosition = fileChannel.position();
        }
        if (shouldForceWrite) {
            forceWrite();
        }
    }
}
