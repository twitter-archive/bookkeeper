package org.apache.bookkeeper.meta;

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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.util.BookKeeperConstants.*;

/**
 * Manage all ledgers in a single zk node.
 *
 * <p>
 * All ledgers' metadata are put in a single zk node, created using zk sequential node.
 * Each ledger node is prefixed with 'L'.
 * </p>
 * <p>
 * All actived ledgers found in bookie server side is managed in a hash map.
 * </p>
 * <p>
 * Garbage collection in FlatLedgerManager is procssed as below:
 * <ul>
 * <li> fetch all existed ledgers from zookeeper, said <b>zkActiveLedgers</b>
 * <li> fetch all active ledgers from bookie server, said <b>bkActiveLedgers</b>
 * <li> loop over <b>bkActiveLedgers</b> to find those ledgers aren't existed in
 * <b>zkActiveLedgers</b>, do garbage collection on them.
 * </ul>
 * </p>
 */
public class FlatLedgerManager extends AbstractZkLedgerManager {

    static final Logger LOG = LoggerFactory.getLogger(FlatLedgerManager.class);
    // path prefix to store ledger znodes
    private final String ledgerPrefix;

    /**
     * Constructor
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     * @param ledgerRootPath
     *          ZooKeeper Path to store ledger metadata
     * @throws IOException when version is not compatible
     */
    public FlatLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        super(conf, zk);

        ledgerPrefix = ledgerRootPath + "/" + LEDGER_NODE_PREFIX;
    }

    @Override
    public void createLedger(final LedgerMetadata metadata, final GenericCallback<Long> cb) {
        StringCallback scb = new StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx,
                    String name) {
                if (Code.OK.intValue() != rc) {
                    LOG.error("Could not create node for ledger",
                              KeeperException.create(KeeperException.Code.get(rc), path));
                    cb.operationComplete(rc, null);
                } else {
                    // update znode status
                    metadata.setVersion(new ZkVersion(0));
                    try {
                        long ledgerId = getLedgerId(name);
                        cb.operationComplete(rc, ledgerId);
                    } catch (IOException ie) {
                        LOG.error("Could not extract ledger-id from path:" + name, ie);
                        cb.operationComplete(BKException.Code.ZKException, null);
                    }
                }
            }
        };
        ZkUtils.asyncCreateFullPathOptimistic(zk, ledgerPrefix, metadata.serialize(),
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL, scb, null);
    }

    @Override
    public String getLedgerPath(long ledgerId) {
        StringBuilder sb = new StringBuilder();
        sb.append(ledgerPrefix)
          .append(StringUtils.getZKStringId(ledgerId));
        return sb.toString();
    }

    @Override
    public long getLedgerId(String nodeName) throws IOException {
        long ledgerId;
        try {
            String parts[] = nodeName.split(ledgerPrefix);
            ledgerId = Long.parseLong(parts[parts.length - 1]);
        } catch (NumberFormatException e) {
            throw new IOException(e);
        }
        return ledgerId;
    }

    @Override
    public void asyncProcessLedgers(final Processor<Long> processor,
                                    final AsyncCallback.VoidCallback finalCb, final Object ctx,
                                    final int successRc, final int failureRc) {
        asyncProcessLedgersInSingleNode(ledgerRootPath, processor, finalCb, ctx, successRc, failureRc);
    }

    @Override
    public void garbageCollectLedgers(GarbageCollector gc) {
        if (null == zk) {
            LOG.warn("Skip garbage collecting ledgers because there is no ZooKeeper handle.");
            return;
        }
        try {
            // create a snapshot first
            Map<Long, Boolean> bkActiveLedgers = activeLedgers.snapshot();
            Set<Long> zkActiveLedgers = getLedgersInSingleNode(ledgerRootPath);
            if (LOG.isDebugEnabled()) {
                LOG.debug("All active ledgers from ZK: {}. Current active ledgers from Bookie: {}.",
                    zkActiveLedgers, bkActiveLedgers.keySet());
            }
            doGc(gc, bkActiveLedgers, zkActiveLedgers);
        } catch (IOException ie) {
            LOG.warn("Error during garbage collecting ledgers from " + ledgerRootPath, ie);
        } catch (InterruptedException inte) {
            LOG.warn("Interrupted during garbage collecting ledgers from " + ledgerRootPath, inte);
        }
    }
}
