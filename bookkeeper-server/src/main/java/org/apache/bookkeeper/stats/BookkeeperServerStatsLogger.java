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

public interface BookkeeperServerStatsLogger extends StatsLogger {
    public static enum BookkeeperServerOp {
        ADD_ENTRY, READ_ENTRY, RANGE_READ_ENTRY, JOURNAL_ADD_ENTRY, JOURNAL_FORCE_WRITE_LATENCY,
        STORAGE_GET_OFFSET, STORAGE_GET_ENTRY, SKIP_LIST_GET_ENTRY, SKIP_LIST_PUT_ENTRY
    }

    public static enum BookkeeperServerSimpleStatType {
        JOURNAL_WRITE_BYTES, JOURNAL_QUEUE_SIZE, READ_BYTES, WRITE_BYTES, NUM_INDEX_PAGES,
        NUM_OPEN_LEDGERS, NUM_MINOR_COMP, NUM_MAJOR_COMP, BUFFERED_READER_NUM_READ_REQUESTS,
        BUFFERED_READER_NUM_READ_CACHE_HITS, JOURNAL_FORCE_WRITE_GROUPING_COUNT
    }
}
