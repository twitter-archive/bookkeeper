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

package org.apache.hedwig.server.netty;

import org.apache.hedwig.server.stats.OpStatsData;

/**
 * PubSub Server MBean
 */
public interface PubSubServerMXBean {

    /**
     * @return publish stats
     */
    public OpStatsData getPubStats();

    /**
     * @return subscription stats
     */
    public OpStatsData getSubStats();

    /**
     * @return unsub stats
     */
    public OpStatsData getUnsubStats();

    /**
     * @return consume stats
     */
    public OpStatsData getConsumeStats();

    /**
     * @return number of requests received
     */
    public long getNumRequestsReceived();

    /**
     * @return number of requests redirect
     */
    public long getNumRequestsRedirect();

    /**
     * @return number of messages delivered
     */
    public long getNumMessagesDelivered();

    /**
     * @return number of topics owned by the hub
     */
    public long getNumTopics();

    /**
     * @return number of requests in the persistence manager
     */
    public long getPersistQueueSize();

    /**
     * @return return 1 if the server is up and running. Sanity check.
     */
    public int getIsUp();

}
