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
package org.apache.bookkeeper.replication;

public interface ReplicationStats {

    public final static String REPLICATION_SCOPE = "replication";

    // auditor stats
    public final static String AUDITOR_SCOPE = "auditor";
    public final static String ELECTION_ATTEMPTS = "election_attempts";
    public final static String AUDITOR_STATUS = "auditor_status";
    public final static String UNDERREPLICATED_LEDGERS = "underreplicated_ledgers";
    public final static String PUBLISHED_UNDERREPLICATED_LEDGERS = "published_underreplicated_ledgers";
    // cluster manager
    public final static String CLUSTER_SCOPE = "cluster";
    public final static String FAILED_BOOKIES = "failed_bookies";
    public final static String REGISTERED_BOOKIES = "registered_bookies";
    public final static String AVAILABLE_BOOKIES = "available_bookies";
    public final static String READ_ONLY_BOOKIES = "readonly_bookies";
    public final static String STALE_BOOKIES = "stale_bookies";
    public final static String ACTIVE_BOOKIES = "active_bookies";
    public final static String LOST_BOOKIES = "lost_bookies";

    // replication worker stats
    public final static String REPLICATION_WORKER_SCOPE = "worker";
    public final static String REREPLICATE_OP = "rereplicate";
    public final static String ACTUAL_REREPLICATE = "actual_rereplicate";
    public final static String REPLICATE_EXCEPTION = "exceptions";

    // BK client stats
    public final static String BK_CLIENT_SCOPE = "bk_client";

}
