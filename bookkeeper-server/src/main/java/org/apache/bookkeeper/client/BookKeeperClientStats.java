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

package org.apache.bookkeeper.client;

public interface BookKeeperClientStats {
    public final static String CLIENT_SCOPE = "bookkeeper_client";

    // Metadata Operations

    public final static String LEDGER_CREATE = "LEDGER_CREATE";
    public final static String LEDGER_OPEN = "LEDGER_OPEN";
    public final static String LEDGER_OPEN_RECOVERY = "LEDGER_OPEN_RECOVERY";
    public final static String LEDGER_DELETE = "LEDGER_DELETE";
    public final static String LEDGER_CLOSE = "LEDGER_CLOSE";
    public final static String LEDGER_RECOVER = "LEDGER_RECOVER";
    public final static String LEDGER_RECOVER_READ_ENTRIES = "LEDGER_RECOVER_READ_ENTRIES";
    public final static String LEDGER_RECOVER_ADD_ENTRIES = "LEDGER_RECOVER_ADD_ENTRIES";

    // Data Operations

    // Add Entry Stats
    public final static String ADD_ENTRY = "ADD_ENTRY";
    public final static String ADD_COMPLETE = "ADD_COMPLETE";
    public final static String TIMEOUT_ADD_ENTRY = "TIMEOUT_ADD_ENTRY";
    public final static String NUM_ADDS_SUBMITTED_PER_CALLBACK = "NUM_ADDS_SUBMITTED_PER_CALLBACK";
    public final static String ADD_ENTRY_BYTES = "ADD_ENTRY_BYTES";

    // Read Entry Stats
    public final static String READ_ENTRY = "READ_ENTRY";
    public final static String READ_LAST_CONFIRMED = "READ_LAST_CONFIRMED";
    public final static String TRY_READ_LAST_CONFIRMED = "TRY_READ_LAST_CONFIRMED";
    public final static String READ_LAST_CONFIRMED_LONG_POLL = "READ_LAST_CONFIRMED_LONG_POLL";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY = "READ_LAST_CONFIRMED_AND_ENTRY";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE = "READ_LAST_CONFIRMED_AND_ENTRY_RESPONSE";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY_HIT = "READ_LAST_CONFIRMED_AND_ENTRY_HIT";
    public final static String READ_LAST_CONFIRMED_AND_ENTRY_MISS = "READ_LAST_CONFIRMED_AND_ENTRY_MISS";
    public final static String SPECULATIVES_PER_READ = "SPECULATIVES_PER_READ";
    public final static String SPECULATIVES_PER_READ_LAC = "SPECULATIVES_PER_READ_LAC";

    // Counters

    public final static String NUM_ENSEMBLE_CHANGE = "NUM_ENSEMBLE_CHANGE";
    public final static String NUM_OPEN_LEDGERS = "NUM_OPEN_LEDGERS";
    public final static String NUM_PENDING_ADDS = "NUM_PENDING_ADD";
    public final static String LAC_UPDATE_HITS = "LAC_UPDATE_HITS";
    public final static String LAC_UPDATE_MISSES = "LAC_UPDATE_MISSES";

    // per channel stats
    public final static String CHANNEL_SCOPE = "per_channel_bookie_client";

    public final static String CHANNEL_ADD_ENTRY = "ADD_ENTRY";
    public final static String CHANNEL_ADD_ENTRY_BYTES = "ADD_ENTRY_BYTES";
    public final static String CHANNEL_NETTY_TIMEOUT_ADD_ENTRY = "NETTY_TIMEOUT_ADD_ENTRY";
    public final static String CHANNEL_READ_ENTRY = "READ_ENTRY";
    public final static String CHANNEL_READ_ENTRY_AND_FENCE = "READ_ENTRY_AND_FENCE";
    public final static String CHANNEL_READ_ENTRY_LONG_POLL = "READ_ENTRY_LONG_POLL";
    public final static String CHANNEL_READ_LONG_POLL_RESPONSE = "READ_LONG_POLL_RESPONSE";
    public final static String CHANNEL_NETTY_TIMEOUT_READ_ENTRY = "NETTY_TIMEOUT_READ_ENTRY";
    public final static String CHANNEL_CONNECT = "CHANNEL_CONNECT";
    public final static String CHANNEL_WRITE = "CHANNEL_WRITE";
    public final static String CHANNEL_WRITE_DISPATCH = "CHANNEL_WRITE_DISPATCH";
    public final static String CHANNEL_RESPONSE = "CHANNEL_RESPONSE";

    // Operations Response Code
    public final static String RESPONSE_CODE = "RESPONSE_CODE";

}
