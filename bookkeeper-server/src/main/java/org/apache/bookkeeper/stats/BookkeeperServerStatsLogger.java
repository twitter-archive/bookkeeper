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
package org.apache.bookkeeper.stats;

public class BookkeeperServerStatsLogger extends BookkeeperStatsLogger {
    public static enum BookkeeperServerOp {
        ADD_ENTRY_REQUEST, ADD_ENTRY,
        READ_ENTRY_REQUEST, READ_ENTRY, READ_ENTRY_SCHEDULING_DELAY,
        READ_ENTRY_FENCE_REQUEST, READ_ENTRY_FENCE_WAIT, READ_ENTRY_FENCE_READ,
        READ_ENTRY_LONG_POLL_REQUEST, READ_ENTRY_LONG_POLL_PRE_WAIT, READ_ENTRY_LONG_POLL_WAIT, READ_ENTRY_LONG_POLL_READ,
        JOURNAL_ADD_ENTRY, JOURNAL_MEM_ADD_ENTRY, JOURNAL_PREALLOCATION, JOURNAL_FLUSH_IN_MEM_ADD,
        JOURNAL_FORCE_WRITE_LATENCY, JOURNAL_FORCE_WRITE_GROUPING_COUNT,
        JOURNAL_FORCE_WRITE_BATCH_ENTRIES, JOURNAL_FORCE_WRITE_BATCH_BYTES,
        JOURNAL_FLUSH_LATENCY, JOURNAL_CREATION_LATENCY,
        STORAGE_GET_OFFSET, STORAGE_GET_ENTRY,
        SKIP_LIST_GET_ENTRY, SKIP_LIST_PUT_ENTRY, SKIP_LIST_SNAPSHOT
    }

    public static enum BookkeeperServerCounter {
        JOURNAL_WRITE_BYTES, JOURNAL_QUEUE_SIZE, READ_BYTES, WRITE_BYTES,
        NUM_MINOR_COMP, NUM_MAJOR_COMP,
        BUFFERED_READER_NUM_READ_REQUESTS, BUFFERED_READER_NUM_READ_CACHE_HITS,
        JOURNAL_FORCE_WRITE_QUEUE_SIZE, JOURNAL_NUM_FORCE_WRITES,
        JOURNAL_NUM_FLUSH_EMPTY_QUEUE, JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES, JOURNAL_NUM_FLUSH_MAX_WAIT,
        SKIP_LIST_FLUSH_BYTES, SKIP_LIST_THROTTLING, READ_LAST_ENTRY_NOENTRY_ERROR
    }

    public static enum BookkeeperServerGauge {
        NUM_INDEX_PAGES, NUM_OPEN_LEDGERS,
        JOURNAL_FORCE_WRITE_QUEUE_SIZE, JOURNAL_FORCE_WRITE_GROUPING_COUNT,
        NUM_PENDING_LONG_POLL, NUM_PENDING_READ, NUM_PENDING_ADD,
        SERVER_STATUS
    }

    public BookkeeperServerStatsLogger(StatsLogger underlying) {
        super(underlying, BookkeeperServerOp.values(), BookkeeperServerCounter.values(),
                BookkeeperServerGauge.values());
    }
}
