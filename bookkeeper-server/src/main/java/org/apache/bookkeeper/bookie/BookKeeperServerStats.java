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

public interface BookKeeperServerStats {

    String SERVER_SCOPE = "bookkeeper_server";
    String BOOKIE_SCOPE = "bookie";

    String SERVER_STATUS = "SERVER_STATUS";

    // Gauges
    String OP_LONG_POLL = "LONG_POLL";
    String OP_READ = "READ";
    String OP_ADD = "ADD";

    // Pending
    String PENDINGS = "PENDINGS";

    //
    // Network Stats (scoped under SERVER_SCOPE)
    //

    // Stats
    String CHANNEL_WRITE = "CHANNEL_WRITE";

    //
    // Server Stats (scoped under SERVER_SCOPE)
    //

    // Stats
    String ADD_ENTRY_REQUEST = "ADD_ENTRY_REQUEST";
    String ADD_ENTRY = "ADD_ENTRY";
    String READ_ENTRY_REQUEST = "READ_ENTRY_REQUEST";
    String READ_ENTRY = "READ_ENTRY";
    String READ_ENTRY_SCHEDULING_DELAY = "READ_ENTRY_SCHEDULING_DELAY";
    String READ_ENTRY_FENCE_REQUEST = "READ_ENTRY_FENCE_REQUEST";
    String READ_ENTRY_FENCE_WAIT = "READ_ENTRY_FENCE_WAIT";
    String READ_ENTRY_FENCE_READ = "READ_ENTRY_FENCE_READ";
    String READ_ENTRY_LONG_POLL_REQUEST = "READ_ENTRY_LONG_POLL_REQUEST";
    String READ_ENTRY_LONG_POLL_PRE_WAIT = "READ_ENTRY_LONG_POLL_PRE_WAIT";
    String READ_ENTRY_LONG_POLL_WAIT = "READ_ENTRY_LONG_POLL_WAIT";
    String READ_ENTRY_LONG_POLL_READ = "READ_ENTRY_LONG_POLL_READ";

    //
    // Bookie Stats (scoped under SERVER_SCOPE)
    //

    // Stats
    String BOOKIE_ADD_ENTRY = "BOOKIE_ADD_ENTRY";
    String BOOKIE_RECOVERY_ADD_ENTRY = "BOOKIE_RECOVERY_ADD_ENTRY";
    String BOOKIE_READ_ENTRY = "BOOKIE_READ_ENTRY";
    String BOOKIE_READ_LAST_CONFIRMED = "BOOKIE_READ_LAST_CONFIRMED";
    String BOOKIE_ADD_ENTRY_BYTES = "BOOKIE_ADD_ENTRY_BYTES";
    String BOOKIE_READ_ENTRY_BYTES = "BOOKIE_READ_ENTRY_BYTES";

    // Counters
    String READ_BYTES = "READ_BYTES";
    String WRITE_BYTES = "WRITE_BYTES";
    String READ_LAST_ENTRY_NOENTRY_ERROR = "READ_LAST_ENTRY_NOENTRY_ERROR";

    //
    // Journal Stats (scoped under SERVER_SCOPE)
    //

    // Stats
    String JOURNAL_ADD_ENTRY = "JOURNAL_ADD_ENTRY";
    String JOURNAL_MEM_ADD_ENTRY = "JOURNAL_MEM_ADD_ENTRY";
    String JOURNAL_PREALLOCATION = "JOURNAL_PREALLOCATION";
    String JOURNAL_FORCE_WRITE_LATENCY = "JOURNAL_FORCE_WRITE_LATENCY";
    String JOURNAL_FORCE_WRITE_GROUPING_COUNT = "JOURNAL_FORCE_WRITE_GROUPING_COUNT";
    String JOURNAL_FORCE_WRITE_BATCH_ENTRIES = "JOURNAL_FORCE_WRITE_BATCH_ENTRIES";
    String JOURNAL_FORCE_WRITE_BATCH_BYTES = "JOURNAL_FORCE_WRITE_BATCH_BYTES";
    String JOURNAL_FLUSH_LATENCY = "JOURNAL_FLUSH_LATENCY";
    String JOURNAL_CREATION_LATENCY = "JOURNAL_CREATION_LATENCY";
    String JOURNAL_FLUSH_IN_MEM_ADD = "JOURNAL_FLUSH_IN_MEM_ADD";

    // Counters
    String JOURNAL_WRITE_BYTES = "JOURNAL_WRITE_BYTES";
    String JOURNAL_QUEUE_SIZE = "JOURNAL_QUEUE_SIZE";
    String JOURNAL_FORCE_WRITE_QUEUE_SIZE = "JOURNAL_FORCE_WRITE_QUEUE_SIZE";
    String JOURNAL_NUM_FORCE_WRITES = "JOURNAL_NUM_FORCE_WRITES";
    String JOURNAL_NUM_FLUSH_EMPTY_QUEUE = "JOURNAL_NUM_FLUSH_EMPTY_QUEUE";
    String JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES = "JOURNAL_NUM_FLUSH_MAX_OUTSTANDING_BYTES";
    String JOURNAL_NUM_FLUSH_MAX_WAIT = "JOURNAL_NUM_FLUSH_MAX_WAIT";

    //
    // Ledger Storage Stats (scoped under SERVER_SCOPE)
    //

    // Stats
    String STORAGE_GET_OFFSET = "STORAGE_GET_OFFSET";
    String STORAGE_GET_ENTRY = "STORAGE_GET_ENTRY";
    /** Ledger Cache Stats **/
    String LEDGER_CACHE_READ_PAGE = "LEDGER_CACHE_READ_PAGE";
    /** SkipList Stats **/
    String SKIP_LIST_GET_ENTRY = "SKIP_LIST_GET_ENTRY";
    String SKIP_LIST_PUT_ENTRY = "SKIP_LIST_PUT_ENTRY";
    String SKIP_LIST_SNAPSHOT = "SKIP_LIST_SNAPSHOT";
    /** GC Stats **/
    String GC_NUM_LEDGERS_DELETED_PER_GC = "GC_NUM_LEDGERS_DELETED_PER_GC";
    String GC_NUM_ENTRYLOGS_DELETED_PER_COMPACTION = "GC_NUM_ENTRYLOGS_DELETED_PER_COMPACTION";

    // Counters
    /** Ledger Storage Counters **/
    String NUM_ENTRIES_READ_FROM_MEMTABLE = "NUM_ENTRIES_READ_FROM_MEMTABLE";
    String NUM_ENTRIES_READ_FROM_ENTRYLOG = "NUM_ENTRIES_READ_FROM_ENTRYLOG";
    String NUM_BYTES_READ_FROM_MEMTABLE = "NUM_BYTES_READ_FROM_MEMTABLE";
    String NUM_BYTES_READ_FROM_ENTRYLOG = "NUM_BYTES_READ_FROM_ENTRYLOG";
    /** Ledger Cache Counters **/
    String LEDGER_CACHE_HIT = "LEDGER_CACHE_HIT";
    String LEDGER_CACHE_MISS = "LEDGER_CACHE_MISS";
    /** Compaction/Garbage Collection Related Counters **/
    String NUM_MINOR_COMP = "NUM_MINOR_COMP";
    String NUM_MAJOR_COMP = "NUM_MAJOR_COMP";
    String NUM_GC = "NUM_GC";
    /** Index Related Counters **/
    String INDEX_INMEM_ILLEGAL_STATE_RESET = "INDEX_INMEM_ILLEGAL_STATE_RESET";
    String INDEX_INMEM_ILLEGAL_STATE_DELETE = "INDEX_INMEM_ILLEGAL_STATE_DELETE";
    String BUFFERED_READER_NUM_READ_REQUESTS = "BUFFERED_READER_NUM_READ_REQUESTS";
    String BUFFERED_READER_NUM_READ_CACHE_HITS = "BUFFERED_READER_NUM_READ_CACHE_HITS";
    /** SkipList Related Counters **/
    String SKIP_LIST_FLUSH_BYTES = "SKIP_LIST_FLUSH_BYTES";
    String SKIP_LIST_THROTTLING = "SKIP_LIST_THROTTLING";
    /** LedgerCache Related Counters **/
    String LEDGER_CACHE_NUM_EVICTED_LEDGERS = "LEDGER_CACHE_NUM_EVICTED_LEDGERS";
    /** GC Related Counters **/
    String GC_NUM_LEDGERS_DELETED = "GC_NUM_LEDGERS_DELETED";
    String GC_NUM_ENTRYLOGS_DELETED = "GC_NUM_ENTRYLOGS_DELETED";
    String GC_NUM_ENTRYLOGS_COMPACTED = "GC_NUM_ENTRYLOGS_COMPACTED";
    /** Corruption counter **/
    String ADD_ENTRY_DIGEST_FAILURE = "ADD_ENTRY_DIGEST_FAILURE";

    // Gauge
    /** Ledger Cache Gauges **/
    String NUM_INDEX_PAGES = "NUM_INDEX_PAGES";
    String NUM_OPEN_LEDGERS = "NUM_OPEN_LEDGERS";
    /** Entry Logger Gauges **/
    String NUM_PENDING_ENTRY_LOG_FILES = "NUM_PENDING_ENTRY_LOG_FILES";
    String LEAST_UNFLUSHED_ENTRYLOG_ID = "LEAST_UNFLUSHED_ENTRYLOG_ID";
    String CURRENT_ENTRYLOG_ID = "CURRENT_ENTRYLOG_ID";
    /** GC Gauges **/
    String GC_TOTAL_SCANNED_BYTES = "GC_TOTAL_SCANNED_BYTES";
    String GC_TOTAL_SCANNED_ENTRYLOG_FILES = "GC_TOTAL_SCANNED_ENTRYLOG_FILES";
    String GC_LAST_SCANNED_ENTRYLOG_ID = "GC_LAST_SCANNED_ENTRYLOG_ID";
    String GC_NUM_ENTRYLOG_FILES_SPACE_USAGE = "GC_NUM_ENTRYLOG_FILES_SPACE_USAGE";

    //
    // LedgerDirs Stats
    //

    String LD_LEDGER_SCOPE = "ledger";
    String LD_INDEX_SCOPE = "index";
    String LD_WRITABLE_DIRS = "writable_dirs";

}
