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

import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.hedwig.server.jmx.HedwigMBeanInfo;
import org.apache.hedwig.server.stats.ServerStatsProvider;

import org.apache.hedwig.protocol.PubSubProtocol.OperationType;

/**
 * PubSub Server Bean
 */
public class PubSubServerBean implements PubSubServerMXBean, HedwigMBeanInfo {

    private final String name;

    public PubSubServerBean(String jmxName) {
        this.name = jmxName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public OpStatsData getPubStats() {
        return ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(OperationType.PUBLISH).toOpStatsData();
    }

    @Override
    public OpStatsData getSubStats() {
        return ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(OperationType.SUBSCRIBE).toOpStatsData();
    }

    @Override
    public OpStatsData getUnsubStats() {
        return ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(OperationType.UNSUBSCRIBE).toOpStatsData();
    }

    @Override
    public OpStatsData getConsumeStats() {
        return ServerStatsProvider.getStatsLoggerInstance().getOpStatsLogger(OperationType.CONSUME).toOpStatsData();
    }

    @Override
    public long getNumRequestsReceived() {
        return ServerStatsProvider.getStatsGetterInstance().getNumRequestsReceived();
    }

    @Override
    public long getNumRequestsRedirect() {
        return ServerStatsProvider.getStatsGetterInstance().getNumRequestsRedirect();
    }

    @Override
    public long getNumMessagesDelivered() {
        return ServerStatsProvider.getStatsGetterInstance().getNumMessagesDelivered();
    }

    @Override
    public long getNumTopics() {
        return ServerStatsProvider.getStatsGetterInstance().getNumTopics();
    }

    @Override
    public long getPersistQueueSize() {
        return ServerStatsProvider.getStatsGetterInstance().getPersistQueueSize();
    }

    @Override
    public int getIsUp() {
        return 1;
    }

}
