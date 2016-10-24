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

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.*;

/**
 * Manage entry log metadata.
 */
class EntryLogMetadataManager {

    /**
     * Records the total size, remaining size and the set of ledgers that comprise a entry log.
     */
    static class EntryLogMetadata {
        final long entryLogId;
        long totalSize;
        long remainingSize;
        final ConcurrentHashMap<Long, Long> ledgersMap;

        public EntryLogMetadata(long logId) {
            this.entryLogId = logId;

            totalSize = remainingSize = 0;
            ledgersMap = new ConcurrentHashMap<Long, Long>();
        }

        public void addLedgerSize(long ledgerId, long size) {
            totalSize += size;
            remainingSize += size;
            Long ledgerSize = ledgersMap.get(ledgerId);
            if (null == ledgerSize) {
                ledgerSize = 0L;
            }
            ledgerSize += size;
            ledgersMap.put(ledgerId, ledgerSize);
        }

        public void removeLedger(long ledgerId) {
            Long size = ledgersMap.remove(ledgerId);
            if (null == size) {
                return;
            }
            remainingSize -= size;
        }

        public boolean containsLedger(long ledgerId) {
            return ledgersMap.containsKey(ledgerId);
        }

        public double getUsage() {
            if (totalSize == 0L) {
                return 0.0f;
            }
            return (double)remainingSize / totalSize;
        }

        public int getTotalLedgers() {
            return ledgersMap.size();
        }

        public long getTotalSize() {
            return totalSize;
        }

        public long getRemainingSize() {
            return remainingSize;
        }

        public boolean isEmpty() {
            return ledgersMap.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{ totalSize = ").append(totalSize).append(", remainingSize = ")
              .append(remainingSize).append(", ledgersMap = ").append(ledgersMap).append(" }");
            return sb.toString();
        }
    }

    private final AtomicLong totalSize = new AtomicLong(0L);
    private final ConcurrentMap<Long, EntryLogMetadata> entryLogMetadataMap =
            new ConcurrentHashMap<Long, EntryLogMetadata>();

    EntryLogMetadataManager(StatsLogger statsLogger) {
        statsLogger.registerGauge(
                GC_TOTAL_SCANNED_BYTES,
                new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return totalSize.get();
                    }
                });
        statsLogger.registerGauge(
                GC_TOTAL_SCANNED_ENTRYLOG_FILES,
                new Gauge<Number>() {
                    @Override
                    public Number getDefaultValue() {
                        return 0;
                    }

                    @Override
                    public Number getSample() {
                        return entryLogMetadataMap.size();
                    }
                });
    }

    public Set<Long> getEntryLogs() {
        return entryLogMetadataMap.keySet();
    }

    public Collection<EntryLogMetadata> getEntryLogMetadatas() {
        return entryLogMetadataMap.values();
    }

    /**
     * Add entry log metadata.
     *
     * @param metadata entry log metadata.
     */
    public void addEntryLogMetadata(EntryLogMetadata metadata) {
        EntryLogMetadata oldMetadata = entryLogMetadataMap.put(metadata.entryLogId, metadata);
        if (null != oldMetadata) {
            totalSize.addAndGet(-oldMetadata.totalSize);
        }
        totalSize.addAndGet(metadata.totalSize);
    }

    /**
     * Whether the manager contains entry log <i>entryLogId</i>.
     *
     * @param entryLogId
     *          entry log id.
     * @return true if the manager already contains the metadata of entry log <i>entryLogId</i>.
     */
    public boolean containsEntryLog(long entryLogId) {
        return entryLogMetadataMap.containsKey(entryLogId);
    }

    /**
     * Remove entry log metadata for entry log <i>entryLogId</i>.
     *
     * @param entryLogId
     *          entry log id.
     */
    public void removeEntryLogMetadata(long entryLogId) {
        EntryLogMetadata metadata = entryLogMetadataMap.remove(entryLogId);
        if (null != metadata) {
            totalSize.addAndGet(-metadata.totalSize);
        }
    }

    /**
     * Return the entry log metadata for entry log <i>entryLogId</i>.
     *
     * @param entryLogId
     *          entry log id.
     * @return entry log metadata.
     */
    public EntryLogMetadata getEntryLogMetadata(long entryLogId) {
        return entryLogMetadataMap.get(entryLogId);
    }

}
