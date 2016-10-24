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
package org.apache.bookkeeper.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.conf.AbstractConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.versioning.Version;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.bookkeeper.util.BookKeeperConstants.*;

/**
 * Abstract ledger manager based on zookeeper, which provides common methods such as query zk nodes.
 */
public abstract class AbstractZkLedgerManager implements LedgerManager, ActiveLedgerManager, Watcher {

    static Logger LOG = LoggerFactory.getLogger(AbstractZkLedgerManager.class);

    static final int ZK_CONNECT_BACKOFF_MS_MIN = 200;
    static final int ZK_CONNECT_BACKOFF_MS_MAX = 2000;

    protected final AbstractConfiguration conf;
    protected final ZooKeeper zk;
    protected final String ledgerRootPath;
    protected final int asyncProcessLedgersConcurrency;

    // A sorted map to stored all active ledger ids
    protected final SnapshotMap<Long, Boolean> activeLedgers;
    // ledger metadata listeners
    protected final ConcurrentMap<Long, Set<LedgerMetadataListener>> listeners =
            new ConcurrentHashMap<Long, Set<LedgerMetadataListener>>();
    // we use this to prevent long stack chains from building up in callbacks
    protected final ScheduledExecutorService scheduler;
    protected final ReentrantReadWriteLock closeLock;
    protected boolean closed = false;

    protected class ReadLedgerMetadataTask implements Runnable, GenericCallback<LedgerMetadata> {

        final long ledgerId;
        int currentZKEBackOff = ZK_CONNECT_BACKOFF_MS_MIN;

        ReadLedgerMetadataTask(long ledgerId) {
            this.ledgerId = ledgerId;
        }

        @Override
        public void run() {
            if (null != listeners.get(ledgerId)) {
                LOG.debug("Re-read ledger metadata for {}.", ledgerId);
                readLedgerMetadata(ledgerId, this, AbstractZkLedgerManager.this);
            } else {
                LOG.debug("Ledger metadata listener for ledger {} is already removed.", ledgerId);
            }
        }

        @Override
        public void operationComplete(int rc, final LedgerMetadata result) {
            if (BKException.Code.OK == rc) {
                // reset the back off after a successful operation
                currentZKEBackOff = ZK_CONNECT_BACKOFF_MS_MIN;
                final Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
                if (null != listenerSet) {
                    LOG.debug("Ledger metadata is changed for {} : {}.", ledgerId, result);
                    submitTask(new Runnable() {
                        @Override
                        public void run() {
                            for (LedgerMetadataListener listener : listenerSet) {
                                listener.onChanged(ledgerId, result);
                            }
                        }
                    });
                }
            } else if (BKException.Code.NoSuchLedgerExistsException == rc) {
                // the ledger is removed, do nothing
                Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                if (null != listenerSet) {
                    LOG.debug("Removed ledger metadata listener set on ledger {} as its ledger is deleted : {}",
                            ledgerId, listenerSet.size());
                }
            } else {
                // Use constant backoff for all bookkeeper specific exceptions;
                // for ZK exceptions, use exponential back off
                int backOff = ZK_CONNECT_BACKOFF_MS_MIN;
                if (BKException.Code.ZKException == rc) {
                    backOff = currentZKEBackOff;
                    // Double the backoff for the next retry up to the maximum allowed back off
                    currentZKEBackOff = Math.min(2 * currentZKEBackOff, ZK_CONNECT_BACKOFF_MS_MAX);
                    LOG.info("Encountered zookeeper issue on reading ledger metadata of ledger {} : rc = {}." +
                            " Retrying in {} ms.", new Object[] { ledgerId, rc, backOff });
                } else {
                    LOG.info("Failed on reading ledger metadata of ledger {} : rc = {}. Retrying in {} ms. ",
                            new Object[] { ledgerId, rc, backOff });
                }
                scheduleTask(this, backOff);
            }
        }
    }

    /**
     * ZooKeeper-based Ledger Manager Constructor
     *
     * @param conf
     *          Configuration object
     * @param zk
     *          ZooKeeper Client Handle
     */
    protected AbstractZkLedgerManager(AbstractConfiguration conf, ZooKeeper zk) {
        this.conf = conf;
        this.zk = zk;
        this.ledgerRootPath = conf.getZkLedgersRootPath();
        this.asyncProcessLedgersConcurrency = conf.getAsyncProcessLedgersConcurrency();
        this.activeLedgers = new SnapshotMap<Long, Boolean>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("bkc-zkledgermanager-%d").build()
        );
        this.closeLock = new ReentrantReadWriteLock();
    }

    /**
     * Get the znode path that is used to store ledger metadata
     *
     * @param ledgerId
     *          Ledger ID
     * @return ledger node path
     */
    protected abstract String getLedgerPath(long ledgerId);

    /**
     * Get ledger id from its znode ledger path
     *
     * @param ledgerPath
     *          Ledger path to store metadata
     * @return ledger id
     * @throws IOException when the ledger path is invalid
     */
    protected abstract long getLedgerId(String ledgerPath) throws IOException;

    protected void submitTask(Runnable runnable) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.submit(runnable);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    protected void scheduleTask(Runnable runnable, long delayMs) {
        closeLock.readLock().lock();
        try {
            if (closed) {
                return;
            }
            scheduler.schedule(runnable, delayMs, TimeUnit.MILLISECONDS);
        } finally {
            closeLock.readLock().unlock();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.debug("Received watched event {} from zookeeper based ledger manager.", event);
        if (Event.EventType.None == event.getType()) {
            if (Event.KeeperState.Expired == event.getState()) {
                Set<Long> keySet = new HashSet<Long>(listeners.keySet());
                for (Long lid : keySet) {
                    submitTask(new ReadLedgerMetadataTask(lid));
                }
                LOG.info("Scheduled re-reading ledger metadata for {} ledgers after zookeeper session expired.", keySet.size());
            }
            return;
        }
        String path = event.getPath();
        if (null == path) {
            return;
        }
        final long ledgerId;
        try {
            ledgerId = getLedgerId(event.getPath());
        } catch (IOException ioe) {
            LOG.info("Received invalid ledger path {} : ", event.getPath(), ioe);
            return;
        }
        switch (event.getType()) {
        case NodeDeleted:
            Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
            if (null != listenerSet) {
                LOG.debug("Removed ledger metadata listeners on ledger {} : {}",
                        ledgerId, listenerSet);
            } else {
                LOG.debug("No ledger metadata listeners to remove from ledger {} after it's deleted.",
                        ledgerId);
            }
            break;
        case NodeDataChanged:
            new ReadLedgerMetadataTask(ledgerId).run();
            break;
        default:
            LOG.debug("Received event {} on {}.", event.getType(), event.getPath());
            break;
        }
    }

    @Override
    public void deleteLedger(final long ledgerId, final GenericCallback<Void> cb) {
        zk.delete(getLedgerPath(ledgerId), -1, new VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                int bkRc;
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    LOG.warn("Ledger node does not exist in ZooKeeper: ledgerId={}", ledgerId);
                    bkRc = BKException.Code.NoSuchLedgerExistsException;
                } else if (rc == KeeperException.Code.OK.intValue()) {
                    // removed listener on ledgerId
                    Set<LedgerMetadataListener> listenerSet = listeners.remove(ledgerId);
                    if (null != listenerSet) {
                        LOG.debug("Remove registered ledger metadata listeners on ledger {} after ledger is deleted.",
                                ledgerId, listenerSet);
                    } else {
                        LOG.debug("No ledger metadata listeners to remove from ledger {} when it's being deleted.",
                                ledgerId);
                    }
                    bkRc = BKException.Code.OK;
                } else {
                    bkRc = BKException.Code.ZKException;
                }
                cb.operationComplete(bkRc, (Void)null);
            }
        }, null);
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        if (null != listener) {
            LOG.info("Registered ledger metadata listener {} on ledger {}.", listener, ledgerId);
            Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
            if (listenerSet == null) {
                Set<LedgerMetadataListener> newListenerSet = new HashSet<LedgerMetadataListener>();
                Set<LedgerMetadataListener> oldListenerSet = listeners.putIfAbsent(ledgerId, newListenerSet);
                if (null != oldListenerSet) {
                    listenerSet = oldListenerSet;
                } else {
                    listenerSet = newListenerSet;
                }
            }
            synchronized (listenerSet) {
                listenerSet.add(listener);
            }
            new ReadLedgerMetadataTask(ledgerId).run();
        }
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        Set<LedgerMetadataListener> listenerSet = listeners.get(ledgerId);
        if (listenerSet != null) {
            synchronized (listenerSet) {
                if (listenerSet.remove(listener)) {
                    LOG.info("Unregistered ledger metadata listener {} on ledger {}.", listener, ledgerId);
                }
                if (listenerSet.isEmpty()) {
                    listeners.remove(ledgerId, listenerSet);
                }
            }
        }
    }

    @Override
    public void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb) {
        readLedgerMetadata(ledgerId, readCb, null);
    }

    protected void readLedgerMetadata(final long ledgerId, final GenericCallback<LedgerMetadata> readCb,
                                      Watcher watcher) {
        zk.getData(getLedgerPath(ledgerId), watcher, new DataCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                if (rc == KeeperException.Code.NONODE.intValue()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("No such ledger: " + ledgerId,
                                  KeeperException.create(KeeperException.Code.get(rc), path));
                    }
                    readCb.operationComplete(BKException.Code.NoSuchLedgerExistsException, null);
                    return;
                }

                if (rc == KeeperException.Code.SESSIONEXPIRED.intValue()) {
                    LOG.info("ZK session expired while reading metadata for ledger {}", ledgerId);
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }

                if (rc != KeeperException.Code.OK.intValue()) {
                    LOG.error("Could not read metadata for ledger {} : path = {}, rc = {}",
                            new Object[] { ledgerId, path, KeeperException.Code.get(rc) });
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }

                LedgerMetadata metadata;
                try {
                    metadata = LedgerMetadata.parseConfig(data, new ZkVersion(stat.getVersion()));
                } catch (IOException e) {
                    LOG.error("Could not parse ledger metadata for ledger {} : {}", ledgerId, e.getMessage());
                    readCb.operationComplete(BKException.Code.ZKException, null);
                    return;
                }
                readCb.operationComplete(BKException.Code.OK, metadata);
            }
        }, null);
    }

    @Override
    public void writeLedgerMetadata(final long ledgerId, final LedgerMetadata metadata,
                                    final GenericCallback<Void> cb) {
        Version v = metadata.getVersion();
        if (Version.NEW == v || !(v instanceof ZkVersion)) {
            cb.operationComplete(BKException.Code.MetadataVersionException, null);
            return;
        }
        final ZkVersion zv = (ZkVersion) v;
        zk.setData(getLedgerPath(ledgerId),
                   metadata.serialize(), zv.getZnodeVersion(),
                   new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                if (KeeperException.Code.BadVersion == rc) {
                    cb.operationComplete(BKException.Code.MetadataVersionException, null);
                } else if (KeeperException.Code.OK.intValue() == rc) {
                    // update metadata version
                    metadata.setVersion(zv.setZnodeVersion(stat.getVersion()));
                    cb.operationComplete(BKException.Code.OK, null);
                } else {
                    LOG.warn("Conditional update ledger {}'s metadata failed: rc = {}",
                            ledgerId, KeeperException.Code.get(rc));
                    cb.operationComplete(BKException.Code.ZKException, null);
                }
            }
        }, null);
    }

    /**
     * Get all the ledgers in a single zk node
     *
     * @param nodePath
     *          Zookeeper node path
     * @param getLedgersCallback
     *          callback function to process ledgers in a single node
     */
    protected void asyncGetLedgersInSingleNode(final String nodePath, final GenericCallback<HashSet<Long>> getLedgersCallback) {
        // First sync ZK to make sure we're reading the latest active/available ledger nodes.
        zk.sync(nodePath, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                LOG.debug("Sync node path {} return : {}", path, rc);
                if (rc != Code.OK.intValue()) {
                    LOG.error("ZK error syncing the ledgers node when getting children: path = {}, rc = {}",
                            path, KeeperException.Code.get(rc));
                    getLedgersCallback.operationComplete(rc, null);
                    return;
                }
                // Sync has completed successfully so now we can poll ZK
                // and read in the latest set of active ledger nodes.
                doAsyncGetLedgersInSingleNode(nodePath, getLedgersCallback);
            }
        }, null);
    }

    private void doAsyncGetLedgersInSingleNode(final String nodePath,
                                               final GenericCallback<HashSet<Long>> getLedgersCallback) {
        zk.getChildren(nodePath, false, new AsyncCallback.ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> ledgerNodes) {
                if (rc != Code.OK.intValue()) {
                    LOG.error("Error polling ZK for the available ledger nodes: path = {}, rc = {}",
                            path, KeeperException.Code.get(rc));
                    getLedgersCallback.operationComplete(rc, null);
                    return;
                }
                LOG.debug("Retrieved current set of ledger nodes: {}", ledgerNodes);
                // Convert the ZK retrieved ledger nodes to a HashSet for easier comparisons.
                HashSet<Long> allActiveLedgers = new HashSet<Long>(ledgerNodes.size(), 1.0f);
                for (String ledgerNode : ledgerNodes) {
                    if (isSpecialZnode(ledgerNode)) {
                        continue;
                    }
                    try {
                        // convert the node path to ledger id according to different ledger manager implementation
                        allActiveLedgers.add(getLedgerId(path + "/" + ledgerNode));
                    } catch (IOException ie) {
                        LOG.warn("Error extracting ledgerId from ZK ledger node {} : {}", ledgerNode, ie.getMessage());
                        // This is a pretty bad error as it indicates a ledger node in ZK
                        // has an incorrect format. For now just continue and consider
                        // this as a non-existent ledger.
                        continue;
                    }
                }

                getLedgersCallback.operationComplete(rc, allActiveLedgers);

            }
        }, null);
    }

    private static class GetLedgersCtx {
        int rc;
        boolean done = false;
        HashSet<Long> ledgers = null;
    }

    /**
     * Get all the ledgers in a single zk node
     *
     * @param nodePath
     *          Zookeeper node path
     * @throws IOException
     * @throws InterruptedException
     */
    protected HashSet<Long> getLedgersInSingleNode(final String nodePath)
        throws IOException, InterruptedException {
        final GetLedgersCtx ctx = new GetLedgersCtx();
        LOG.debug("Try to get ledgers of node : {}", nodePath);
        asyncGetLedgersInSingleNode(nodePath, new GenericCallback<HashSet<Long>>() {
                @Override
                public void operationComplete(int rc, HashSet<Long> zkActiveLedgers) {
                    synchronized (ctx) {
                        if (Code.OK.intValue() == rc) {
                            ctx.ledgers = zkActiveLedgers;
                        }
                        ctx.rc = rc;
                        ctx.done = true;
                        ctx.notifyAll();
                    }
                }
            });

        synchronized (ctx) {
            while (ctx.done == false) {
                ctx.wait();
            }
        }
        if (Code.OK.intValue() != ctx.rc) {
            throw new IOException("Error on getting ledgers from node " + nodePath);
        }
        return ctx.ledgers;
    }

    /**
     * Process ledgers in a single zk node.
     *
     * <p>
     * for each ledger found in this zk node, processor#process(ledgerId) will be triggerred
     * to process a specific ledger. after all ledgers has been processed, the finalCb will
     * be called with provided context object. The RC passed to finalCb is decided by :
     * <ul>
     * <li> All ledgers are processed successfully, successRc will be passed.
     * <li> Either ledger is processed failed, failureRc will be passed.
     * </ul>
     * </p>
     *
     * @param path
     *          Zk node path to store ledgers
     * @param processor
     *          Processor provided to process ledger
     * @param finalCb
     *          Callback object when all ledgers are processed
     * @param ctx
     *          Context object passed to finalCb
     * @param successRc
     *          RC passed to finalCb when all ledgers are processed successfully
     * @param failureRc
     *          RC passed to finalCb when either ledger is processed failed
     */
    protected void asyncProcessLedgersInSingleNode(
            final String path, final Processor<Long> processor,
            final AsyncCallback.VoidCallback finalCb, final Object ctx,
            final int successRc, final int failureRc) {
        asyncGetLedgersInSingleNode(path, new GenericCallback<HashSet<Long>>() {
            @Override
            public void operationComplete(int rc, HashSet<Long> zkActiveLedgers) {
                if (Code.OK.intValue() != rc) {
                    finalCb.processResult(failureRc, null, ctx);
                    return;
                }

                LOG.debug("Processing ledgers: {}", zkActiveLedgers);

                processLedgers(zkActiveLedgers.iterator(), processor,
                               asyncProcessLedgersConcurrency <= 0 ? zkActiveLedgers.size() : asyncProcessLedgersConcurrency,
                               finalCb, ctx, successRc, failureRc);
            }
        });
    }

    private void processLedgers(final Iterator<Long> ledgersIter, final Processor<Long> processor,
                                final int concurrency, final VoidCallback finalCb, final Object ctx,
                                final int successRc, final int failureRc) {
        List<Long> ledgersToProcess = new ArrayList<Long>(concurrency);
        while (ledgersIter.hasNext() && ledgersToProcess.size() < concurrency) {
            ledgersToProcess.add(ledgersIter.next());
        }
        if (ledgersToProcess.size() == 0) {
            finalCb.processResult(successRc, null, ctx);
            return;
        }
        MultiCallback mcb;
        if (ledgersIter.hasNext()) {
            // still have ledgers left
            mcb = new MultiCallback(ledgersToProcess.size(), new VoidCallback() {
                @Override
                public void processResult(int newRc, String path, Object newCtx) {
                    if (newRc != successRc) {
                        finalCb.processResult(failureRc, null, ctx);
                        return;
                    }
                    // succeed and continue
                    processLedgers(ledgersIter, processor, concurrency, finalCb, ctx, successRc, successRc);
                }
            }, ctx, successRc, failureRc, scheduler);
        } else {
            // reach the end
            mcb = new MultiCallback(ledgersToProcess.size(), finalCb, ctx, successRc, failureRc, scheduler);
        }
        for (Long ledger : ledgersToProcess) {
            processor.process(ledger, mcb);
        }
    }

    /**
     * Whether the znode a special znode
     *
     * @param znode
     *          Znode Name
     * @return true  if the znode is a special znode otherwise false
     */
    protected boolean isSpecialZnode(String znode) {
        if (AVAILABLE_NODE.equals(znode)
                || COOKIE_NODE.equals(znode)
                || LAYOUT_ZNODE.equals(znode)
                || INSTANCEID.equals(znode)
                || UNDER_REPLICATION_NODE
                        .equals(znode)) {
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        closeLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            closeLock.writeLock().unlock();
        }
        try {
            scheduler.shutdown();
        } catch (Exception e) {
            LOG.warn("Error when closing zookeeper based ledger manager: ", e);
        }
    }

    @Override
    public void addActiveLedger(long ledgerId, boolean active) {
        activeLedgers.put(ledgerId, active);
    }

    @Override
    public void removeActiveLedger(long ledgerId) {
        activeLedgers.remove(ledgerId);
    }

    @Override
    public boolean containsActiveLedger(long ledgerId) {
        return activeLedgers.containsKey(ledgerId);
    }

    /**
     * Do garbage collecting comparing hosted ledgers and zk ledgers
     *
     * @param gc
     *          Garbage collector to do garbage collection when found inactive/deleted ledgers
     * @param bkActiveLedgers
     *          Active ledgers hosted in bookie server
     * @param zkAllLedgers
     *          All ledgers stored in zookeeper
     */
    void doGc(GarbageCollector gc, Map<Long, Boolean> bkActiveLedgers, Set<Long> zkAllLedgers) {
        // remove any active ledgers that doesn't exist in zk
        for (Long bkLid : bkActiveLedgers.keySet()) {
            if (!zkAllLedgers.contains(bkLid)) {
                // remove it from current active ledger
                bkActiveLedgers.remove(bkLid);
                gc.gc(bkLid);
            }
        }
    }
}
