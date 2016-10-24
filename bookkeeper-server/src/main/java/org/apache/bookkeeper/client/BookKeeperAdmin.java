/**
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.SyncOpenCallback;
import org.apache.bookkeeper.client.LedgerFragmentReplicator.SingleFragmentCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.*;

/**
 * Admin client for BookKeeper clusters
 */
public class BookKeeperAdmin {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperAdmin.class);

    // ZK client instance
    private ZooKeeper zk;
    private final boolean ownsZK;

    // BookKeeper client instance
    private BookKeeper bkc;
    private final boolean ownsBK;

    // LedgerFragmentReplicator instance
    private LedgerFragmentReplicator lfr;

    /**
     * Constructor that takes in a ZooKeeper servers connect string so we know
     * how to connect to ZooKeeper to retrieve information about the BookKeeper
     * cluster. We need this before we can do any type of admin operations on
     * the BookKeeper cluster.
     *
     * @param zkServers
     *            Comma separated list of hostname:port pairs for the ZooKeeper
     *            servers cluster.
     * @throws IOException
     *             throws this exception if there is an error instantiating the
     *             ZooKeeper client.
     * @throws InterruptedException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     * @throws KeeperException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(String zkServers) throws IOException, InterruptedException, KeeperException {
        this(new ClientConfiguration().setZkServers(zkServers));
    }

    /**
     * Constructor that takes in a configuration object so we know
     * how to connect to ZooKeeper to retrieve information about the BookKeeper
     * cluster. We need this before we can do any type of admin operations on
     * the BookKeeper cluster.
     *
     * @param conf
     *           Client Configuration Object
     * @throws IOException
     *             throws this exception if there is an error instantiating the
     *             ZooKeeper client.
     * @throws InterruptedException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     * @throws KeeperException
     *             Throws this exception if there is an error instantiating the
     *             BookKeeper client.
     */
    public BookKeeperAdmin(ClientConfiguration conf) throws IOException, InterruptedException, KeeperException {
        // Create the ZooKeeper client instance
        zk = ZooKeeperClient.newBuilder()
                .connectString(conf.getZkServers())
                .sessionTimeoutMs(conf.getZkTimeout())
                .requestRateLimit(conf.getZkRequestRateLimit())
                .build();
        ownsZK = true;

        // Create the BookKeeper client instance
        bkc = new BookKeeper(conf, zk);
        ownsBK = true;

        this.lfr = new LedgerFragmentReplicator(bkc);
    }

    /**
     * Constructor that takes in a BookKeeper instance . This will be useful,
     * when users already has bk instance ready.
     *
     * @param bkc
     *            - bookkeeper instance
     */
    public BookKeeperAdmin(final BookKeeper bkc) {
        this.bkc = bkc;
        ownsBK = false;
        this.zk = bkc.zk;
        ownsZK = false;
        this.lfr = new LedgerFragmentReplicator(bkc);
    }

    public ZooKeeper getZooKeeper() {
        return this.zk;
    }

    /**
     * Gracefully release resources that this client uses.
     *
     * @throws InterruptedException
     *             if there is an error shutting down the clients that this
     *             class uses.
     */
    public void close() throws InterruptedException, BKException {
        if (ownsBK) {
            bkc.close();
        }
        if (ownsZK) {
            zk.close();
        }
    }

    /**
     * Get all the bookies registered under cookies path.
     *
     * @return the registered bookie list.
     */
    public Collection<BookieSocketAddress> getRegisteredBookies()
            throws BKException {
        String cookiePath = bkc.getConf().getZkLedgersRootPath() + "/"
                + BookKeeperConstants.COOKIE_NODE;
        try {
            List<String> children = zk.getChildren(cookiePath, false);
            List<BookieSocketAddress> bookies = new ArrayList<BookieSocketAddress>(children.size());
            for (String child : children) {
                try {
                    bookies.add(new BookieSocketAddress(child));
                } catch (IOException ioe) {
                    LOG.error("Error parsing bookie address {} : ", child, ioe);
                    throw new BKException.ZKException();
                }
            }
            return bookies;
        } catch (KeeperException ke) {
            LOG.error("Failed to get registered bookie list : ", ke);
            throw new BKException.ZKException();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted reading registered bookie list", ie);
            throw new BKException.BKInterruptedException();
        }
    }

    /**
     * Get a list of the available bookies.
     *
     * @return a collection of bookie addresses
     */
    public Collection<BookieSocketAddress> getAvailableBookies()
            throws BKException {
        return bkc.bookieWatcher.getBookies();
    }

    /**
     * Get a list of readonly bookies
     *
     * @return a collection of bookie addresses
     */
    public Collection<BookieSocketAddress> getReadOnlyBookies()
            throws BKException {
        return bkc.bookieWatcher.getReadOnlyBookies();
    }

    /**
     * Register a bookies listener to receive notifications about bookies changes.
     *
     * @param listener the listener to notify
     */
    public void registerBookiesListener(final BookiesListener listener) {
        bkc.bookieWatcher.registerBookiesListener(listener);
    }

    /**
     * Open a ledger as an administrator. This means that no digest password
     * checks are done. Otherwise, the call is identical to BookKeeper#asyncOpenLedger
     *
     * @param lId
     *          ledger identifier
     * @param cb
     *          Callback which will receive a LedgerHandle object
     * @param ctx
     *          optional context object, to be passwd to the callback (can be null)
     *
     * @see BookKeeper#asyncOpenLedger
     */
    public void asyncOpenLedger(final long lId, final OpenCallback cb, final Object ctx) {
        asyncOpenLedger(lId, false, cb, ctx);
    }

    public void asyncOpenLedger(final long lId, final boolean forceRecovery,
                                final OpenCallback cb, final Object ctx) {
        new LedgerOpenOp(bkc, lId, cb, ctx).forceRecovery(forceRecovery).initiate();
    }

    /**
     * Open a ledger as an administrator. This means that no digest password
     * checks are done. Otherwise, the call is identical to
     * BookKeeper#openLedger
     *
     * @param lId
     *            - ledger identifier
     * @see BookKeeper#openLedger
     */
    public LedgerHandle openLedger(final long lId) throws InterruptedException,
            BKException {
        return openLedger(lId, false);
    }

    public LedgerHandle openLedger(final long lId, final boolean forceRecovery) throws InterruptedException,
            BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();
        new LedgerOpenOp(bkc, lId, new SyncOpenCallback(), counter).forceRecovery(forceRecovery).initiate();
        /*
         * Wait
         */
        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getLh();
    }

    /**
     * Open a ledger as an administrator without recovering the ledger. This means
     * that no digest password  checks are done. Otherwise, the call is identical
     * to BookKeeper#asyncOpenLedgerNoRecovery
     *
     * @param lId
     *          ledger identifier
     * @param cb
     *          Callback which will receive a LedgerHandle object
     * @param ctx
     *          optional context object, to be passwd to the callback (can be null)
     *
     * @see BookKeeper#asyncOpenLedgerNoRecovery
     */
    public void asyncOpenLedgerNoRecovery(final long lId, final OpenCallback cb, final Object ctx) {
        new LedgerOpenOp(bkc, lId, cb, ctx).initiateWithoutRecovery();
    }

    /**
     * Open a ledger as an administrator without recovering the ledger. This
     * means that no digest password checks are done. Otherwise, the call is
     * identical to BookKeeper#openLedgerNoRecovery
     *
     * @param lId
     *            ledger identifier
     * @see BookKeeper#openLedgerNoRecovery
     */
    public LedgerHandle openLedgerNoRecovery(final long lId)
            throws InterruptedException, BKException {
        SyncCounter counter = new SyncCounter();
        counter.inc();
        new LedgerOpenOp(bkc, lId, new SyncOpenCallback(), counter)
                .initiateWithoutRecovery();
        /*
         * Wait
         */
        counter.block(0);
        if (counter.getrc() != BKException.Code.OK) {
            throw BKException.create(counter.getrc());
        }

        return counter.getLh();
    }

    // Object used for calling async methods and waiting for them to complete.
    static class SyncObject {
        boolean value;
        int rc;

        public SyncObject() {
            value = false;
            rc = BKException.Code.OK;
        }
    }

    public SortedMap<Long, LedgerMetadata> getLedgersContainBookies(Set<BookieSocketAddress> bookies)
            throws InterruptedException, BKException {
        final SyncObject sync = new SyncObject();
        final AtomicReference<SortedMap<Long, LedgerMetadata>> resultHolder =
                new AtomicReference<SortedMap<Long, LedgerMetadata>>(null);
        asyncGetLedgersContainBookies(bookies, new GenericCallback<SortedMap<Long, LedgerMetadata>>() {
            @Override
            public void operationComplete(int rc, SortedMap<Long, LedgerMetadata> result) {
                LOG.info("GetLedgersContainBookies completed with rc : {}", rc);
                synchronized (sync) {
                    sync.rc = rc;
                    sync.value = true;
                    resultHolder.set(result);
                    sync.notify();
                }
            }
        });
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
        return resultHolder.get();
    }

    public void asyncGetLedgersContainBookies(final Set<BookieSocketAddress> bookies,
                                              final GenericCallback<SortedMap<Long, LedgerMetadata>> callback) {
        final SortedMap<Long, LedgerMetadata> ledgers = new ConcurrentSkipListMap<Long, LedgerMetadata>();
        bkc.getLedgerManager().asyncProcessLedgers(new Processor<Long>() {
            @Override
            public void process(final Long lid, final AsyncCallback.VoidCallback cb) {
                bkc.getLedgerManager().readLedgerMetadata(lid, new GenericCallback<LedgerMetadata>() {
                    @Override
                    public void operationComplete(int rc, LedgerMetadata metadata) {
                        if (BKException.Code.NoSuchLedgerExistsException == rc) {
                            // the ledger was deleted during this iteration.
                            cb.processResult(BKException.Code.OK, null, null);
                            return;
                        } else if (BKException.Code.OK != rc) {
                            cb.processResult(rc, null, null);
                            return;
                        }
                        Set<BookieSocketAddress> bookiesInLedger = metadata.getBookiesInThisLedger();
                        Sets.SetView<BookieSocketAddress> intersection =
                                Sets.intersection(bookiesInLedger, bookies);
                        if (!intersection.isEmpty()) {
                            ledgers.put(lid, metadata);
                        }
                        cb.processResult(BKException.Code.OK, null, null);
                    }
                });
            }
        }, new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                callback.operationComplete(rc, ledgers);
            }
        }, null, BKException.Code.OK, BKException.Code.MetaStoreException);
    }

    /**
     * Synchronous method to rebuild and recover the ledger fragments data that
     * was stored on the source bookie. That bookie could have failed completely
     * and now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     */
    public void recoverBookieData(final BookieSocketAddress bookieSrc, final BookieSocketAddress bookieDest)
            throws InterruptedException, BKException {
        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        bookiesSrc.add(bookieSrc);
        recoverBookieData(bookiesSrc);
    }

    public void recoverBookieData(final Set<BookieSocketAddress> bookiesSrc)
        throws InterruptedException, BKException {
        recoverBookieData(bookiesSrc, false, false);
    }

    public void recoverBookieData(final Set<BookieSocketAddress> bookiesSrc, boolean dryrun, boolean skipOpenLedgers)
        throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(bookiesSrc, dryrun, skipOpenLedgers, new RecoverCallback() {
            @Override
            public void recoverComplete(int rc, Object ctx) {
                LOG.info("Recover bookie operation completed with rc: " + rc);
                SyncObject syncObj = (SyncObject) ctx;
                synchronized (syncObj) {
                    syncObj.rc = rc;
                    syncObj.value = true;
                    syncObj.notify();
                }
            }
        }, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (sync.value == false) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
    }

    public void recoverBookieData(final long lid, final Set<BookieSocketAddress> bookiesSrc, boolean dryrun, boolean skipOpenLedgers)
        throws InterruptedException, BKException {
        SyncObject sync = new SyncObject();
        // Call the async method to recover bookie data.
        asyncRecoverBookieData(lid, bookiesSrc, dryrun, skipOpenLedgers, new RecoverCallback() {
            @Override
            public void recoverComplete(int rc, Object ctx) {
                LOG.info("Recover bookie for {} completed with rc : {}", lid, rc);
                SyncObject syncObject = (SyncObject) ctx;
                synchronized (syncObject) {
                    syncObject.rc = rc;
                    syncObject.value = true;
                    syncObject.notify();
                }
            }
        }, sync);

        // Wait for the async method to complete.
        synchronized (sync) {
            while (!sync.value) {
                sync.wait();
            }
        }
        if (sync.rc != BKException.Code.OK) {
            throw BKException.create(sync.rc);
        }
    }

    /**
     * Async method to rebuild and recover the ledger fragments data that was
     * stored on the source bookie. That bookie could have failed completely and
     * now the ledger data that was stored on it is under replicated. An
     * optional destination bookie server could be given if we want to copy all
     * of the ledger fragments data on the failed source bookie to it.
     * Otherwise, we will just randomly distribute the ledger fragments to the
     * active set of bookies, perhaps based on load. All ZooKeeper ledger
     * metadata will be updated to point to the new bookie(s) that contain the
     * replicated ledger fragments.
     *
     * @param bookieSrc
     *            Source bookie that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param bookieDest
     *            Optional destination bookie that if passed, we will copy all
     *            of the ledger fragments from the source bookie over to it.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(final BookieSocketAddress bookieSrc, final BookieSocketAddress bookieDest,
                                       final RecoverCallback cb, final Object context) {
        Set<BookieSocketAddress> bookiesSrc = new HashSet<BookieSocketAddress>();
        bookiesSrc.add(bookieSrc);
        asyncRecoverBookieData(bookiesSrc, cb, context);
    }

    public void asyncRecoverBookieData(final Set<BookieSocketAddress> bookieSrc,
                                       final RecoverCallback cb, final Object context) {
        asyncRecoverBookieData(bookieSrc, false, false, cb, context);
    }

    public void asyncRecoverBookieData(final Set<BookieSocketAddress> bookieSrc, boolean dryrun,
                                       final boolean skipOpenLedgers, final RecoverCallback cb, final Object context) {
        getActiveLedgers(bookieSrc, dryrun, skipOpenLedgers, cb, context);
    }

    /**
     * Recover a specific ledger.
     *
     * @param lid
     *          ledger to recover
     * @param bookieSrc
     *          Source bookies that had a failure. We want to replicate the ledger fragments that were stored there.
     * @param dryrun
     *          dryrun the recover procedure.
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param callback
     *          RecoverCallback to invoke once all of the data on the dead
     *          bookie has been recovered and replicated.
     * @param context
     *          Context for the RecoverCallback to call.
     */
    public void asyncRecoverBookieData(long lid, final Set<BookieSocketAddress> bookieSrc, boolean dryrun,
                                       boolean skipOpenLedgers, final RecoverCallback callback, final Object context) {
        AsyncCallback.VoidCallback callbackWrapper = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                callback.recoverComplete(bkc.getReturnRc(rc), context);
            }
        };
        recoverLedger(bookieSrc, lid, dryrun, skipOpenLedgers, callbackWrapper);
    }

    /**
     * This method asynchronously polls ZK to get the current set of active
     * ledgers. From this, we can open each ledger and look at the metadata to
     * determine if any of the ledger fragments for it were stored at the dead
     * input bookie.
     *
     * @param bookiesSrc
     *            Source bookies that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param dryrun
     *            dryrun the recover procedure.
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param cb
     *            RecoverCallback to invoke once all of the data on the dead
     *            bookie has been recovered and replicated.
     * @param context
     *            Context for the RecoverCallback to call.
     */
    private void getActiveLedgers(final Set<BookieSocketAddress> bookiesSrc, final boolean dryrun,
                                  final boolean skipOpenLedgers, final RecoverCallback cb, final Object context) {
        // Wrapper class around the RecoverCallback so it can be used
        // as the final VoidCallback to process ledgers
        class RecoverCallbackWrapper implements AsyncCallback.VoidCallback {
            final RecoverCallback cb;

            RecoverCallbackWrapper(RecoverCallback cb) {
                this.cb = cb;
            }

            @Override
            public void processResult(int rc, String path, Object ctx) {
                cb.recoverComplete(bkc.getReturnRc(rc), ctx);
            }
        }

        Processor<Long> ledgerProcessor = new Processor<Long>() {
            @Override
            public void process(Long ledgerId, AsyncCallback.VoidCallback iterCallback) {
                recoverLedger(bookiesSrc, ledgerId, dryrun, skipOpenLedgers, iterCallback);
            }
        };
        bkc.getLedgerManager().asyncProcessLedgers(
                ledgerProcessor, new RecoverCallbackWrapper(cb),
                context, BKException.Code.OK, BKException.Code.LedgerRecoveryException);
    }

    /**
     * This method asynchronously recovers a given ledger if any of the ledger
     * entries were stored on the failed bookie.
     *
     * @param bookiesSrc
     *            Source bookies that had a failure. We want to replicate the
     *            ledger fragments that were stored there.
     * @param lId
     *            Ledger id we want to recover.
     * @param dryrun
     *            printing the recovery plan without actually recovering bookies
     * @param skipOpenLedgers
     *            Skip recovering open ledgers.
     * @param finalLedgerIterCb
     *            IterationCallback to invoke once we've recovered the current
     *            ledger.
     */
    private void recoverLedger(final Set<BookieSocketAddress> bookiesSrc, final long lId, final boolean dryrun,
                               final boolean skipOpenLedgers, final AsyncCallback.VoidCallback finalLedgerIterCb) {
        LOG.debug("Recovering ledger : {}", lId);

        asyncOpenLedgerNoRecovery(lId, new OpenCallback() {
            @Override
            public void openComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != BKException.Code.OK) {
                    LOG.error("BK error opening ledger: " + lId, BKException.create(rc));
                    finalLedgerIterCb.processResult(rc, null, null);
                    return;
                }

                LedgerMetadata lm = lh.getLedgerMetadata();
                if (skipOpenLedgers && !lm.isClosed() && !lm.isInRecovery()) {
                    LOG.info("Skip recovering open ledger {}.", lId);
                    try {
                        lh.close();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (BKException bke) {
                        LOG.warn("Error on closing ledger handle for {}.", lId);
                    }
                    finalLedgerIterCb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                boolean fenceRequired = false;
                if (!lm.isClosed() &&
                    lm.getEnsembles().size() > 0) {
                    Long lastKey = lm.getEnsembles().lastKey();
                    ArrayList<BookieSocketAddress> lastEnsemble = lm.getEnsembles().get(lastKey);
                    // the original write has not removed faulty bookie from
                    // current ledger ensemble. to avoid data loss issue in
                    // the case of concurrent updates to the ensemble composition,
                    // the recovery tool should first close the ledger
                    fenceRequired = containBookies(lastEnsemble, bookiesSrc);
                    if (!dryrun && fenceRequired) {
                        // close opened non recovery ledger handle
                        try {
                            lh.close();
                        } catch (Exception ie) {
                            LOG.warn("Error closing non recovery ledger handle for ledger " + lId, ie);
                        }
                        asyncOpenLedger(lId, new OpenCallback() {
                            @Override
                            public void openComplete(int newrc, final LedgerHandle newlh, Object newctx) {
                                if (newrc != BKException.Code.OK) {
                                    LOG.error("BK error close ledger: " + lId, BKException.create(newrc));
                                    finalLedgerIterCb.processResult(newrc, null, null);
                                    return;
                                }
                                bkc.mainWorkerPool.submit(new SafeRunnable() {
                                    @Override
                                    public void safeRun() {
                                        // do recovery
                                        recoverLedger(bookiesSrc, lId, dryrun, skipOpenLedgers, finalLedgerIterCb);
                                    }
                                });
                            }
                        }, null);
                        return;
                    }
                }

                final AsyncCallback.VoidCallback ledgerIterCb = new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        if (BKException.Code.OK != rc) {
                            LOG.error("Failed to recover ledger {} : {}", lId, rc);
                        } else {
                            LOG.info("Recovered ledger {}.", lId);
                        }
                        try {
                            lh.close();
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                        } catch (BKException bke) {
                            LOG.warn("Error on cloing ledger handle for {}.", lId);
                        }
                        finalLedgerIterCb.processResult(rc, path, ctx);
                    }
                };

                /*
                 * This List stores the ledger fragments to recover indexed by
                 * the start entry ID for the range. The ensembles TreeMap is
                 * keyed off this.
                 */
                final List<Long> ledgerFragmentsToRecover = new LinkedList<Long>();
                /*
                 * This Map will store the start and end entry ID values for
                 * each of the ledger fragment ranges. The only exception is the
                 * current active fragment since it has no end yet. In the event
                 * of a bookie failure, a new ensemble is created so the current
                 * ensemble should not contain the dead bookie we are trying to
                 * recover.
                 */
                Map<Long, Long> ledgerFragmentsRange = new HashMap<Long, Long>();
                Long curEntryId = null;
                for (Map.Entry<Long, ArrayList<BookieSocketAddress>> entry : lh.getLedgerMetadata().getEnsembles()
                         .entrySet()) {
                    if (curEntryId != null)
                        ledgerFragmentsRange.put(curEntryId, entry.getKey() - 1);
                    curEntryId = entry.getKey();
                    if (containBookies(entry.getValue(), bookiesSrc)) {
                        /*
                         * Current ledger fragment has entries stored on the
                         * dead bookie so we'll need to recover them.
                         */
                        ledgerFragmentsToRecover.add(entry.getKey());
                    }
                }
                // add last ensemble otherwise if the failed bookie existed in
                // the last ensemble of a closed ledger. the entries belonged to
                // last ensemble would not be replicated.
                if (curEntryId != null) {
                    ledgerFragmentsRange.put(curEntryId, lh.getLastAddConfirmed());
                }
                /*
                 * See if this current ledger contains any ledger fragment that
                 * needs to be re-replicated. If not, then just invoke the
                 * multiCallback and return.
                 */
                if (ledgerFragmentsToRecover.size() == 0) {
                    ledgerIterCb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                if (dryrun) {
                    System.out.println("Recovered ledger " + lId + " : " + (fenceRequired ? "[fence required]" : ""));
                }

                /*
                 * Multicallback for ledger. Once all fragments for the ledger have been recovered
                 * trigger the ledgerIterCb
                 */
                MultiCallback ledgerFragmentsMcb
                    = new MultiCallback(ledgerFragmentsToRecover.size(), ledgerIterCb, null,
                                        BKException.Code.OK, BKException.Code.LedgerRecoveryException);
                /*
                 * Now recover all of the necessary ledger fragments
                 * asynchronously using a MultiCallback for every fragment.
                 */
                for (final Long startEntryId : ledgerFragmentsToRecover) {
                    Long endEntryId = ledgerFragmentsRange.get(startEntryId);
                    ArrayList<BookieSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().get(startEntryId);
                    // Get bookies to replace
                    Map<Integer, BookieSocketAddress> targetBookieAddresses;
                    try {
                        targetBookieAddresses = getReplacedBookies(lh, ensemble, bookiesSrc);
                    } catch (BKException.BKNotEnoughBookiesException e) {
                        if (!dryrun) {
                            ledgerFragmentsMcb.processResult(BKException.Code.NotEnoughBookiesException, null, null);
                        } else {
                            System.out.println("  Fragment [" + startEntryId + " - " + endEntryId + " ] : "
                                               + BKException.getMessage(BKException.Code.NotEnoughBookiesException));
                        }
                        continue;
                    }

                    if (dryrun) {
                        ArrayList<BookieSocketAddress> newEnsemble =
                                replaceBookiesInEnsemble(ensemble, targetBookieAddresses);
                        System.out.println("  Fragment [" + startEntryId + " - " + endEntryId + " ] : ");
                        System.out.println("    old ensemble : " + formatEnsemble(ensemble, bookiesSrc, '*'));
                        System.out.println("    new ensemble : " + formatEnsemble(newEnsemble, bookiesSrc, '*'));
                        continue;
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Replicating fragment from [" + startEntryId
                                  + "," + endEntryId + "] of ledger " + lh.getId()
                                  + " to " + targetBookieAddresses);
                    }
                    try {
                        LedgerFragmentReplicator.SingleFragmentCallback cb = new LedgerFragmentReplicator.SingleFragmentCallback(
                                ledgerFragmentsMcb, lh, startEntryId, getReplacedBookiesMap(ensemble, targetBookieAddresses));
                        LedgerFragment ledgerFragment = new LedgerFragment(lh,
                                startEntryId, endEntryId, targetBookieAddresses.keySet());
                        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, Sets.newHashSet(targetBookieAddresses.values()));
                    } catch(InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (dryrun) {
                    ledgerIterCb.processResult(BKException.Code.OK, null, null);
                }
            }
            }, null);
    }

    static String formatEnsemble(ArrayList<BookieSocketAddress> ensemble, Set<BookieSocketAddress> bookiesSrc, char marker) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < ensemble.size(); i++) {
            sb.append(ensemble.get(i));
            if (bookiesSrc.contains(ensemble.get(i))) {
                sb.append(marker);
            } else {
                sb.append(' ');
            }
            if (i != ensemble.size() - 1) {
                sb.append(", ");
            }
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * This method asynchronously recovers a ledger fragment which is a
     * contiguous portion of a ledger that was stored in an ensemble that
     * included the failed bookie.
     *
     * @param lh
     *            - LedgerHandle for the ledger
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     * @param ledgerFragmentMcb
     *            - MultiCallback to invoke once we've recovered the current
     *            ledger fragment.
     * @param newBookies
     *            - New bookies we want to use to recover and replicate the
     *            ledger entries that were stored on the failed bookie.
     */
    private void asyncRecoverLedgerFragment(final LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final AsyncCallback.VoidCallback ledgerFragmentMcb,
            final Set<BookieSocketAddress> newBookies) throws InterruptedException {
        lfr.replicate(lh, ledgerFragment, ledgerFragmentMcb, newBookies);
    }

    private Map<Integer, BookieSocketAddress> getReplacedBookies(
                LedgerHandle lh,
                List<BookieSocketAddress> ensemble,
                Set<BookieSocketAddress> bookiesToRereplicate)
            throws BKException.BKNotEnoughBookiesException {
        Set<Integer> bookieIndexesToRereplicate = Sets.newHashSet();
        for (int bookieIndex = 0; bookieIndex < ensemble.size(); bookieIndex++) {
            BookieSocketAddress bookieInEnsemble = ensemble.get(bookieIndex);
            if (bookiesToRereplicate.contains(bookieInEnsemble)) {
                bookieIndexesToRereplicate.add(bookieIndex);
            }
        }
        return getReplacedBookiesByIndexes(
                lh, ensemble, bookieIndexesToRereplicate, Optional.of(bookiesToRereplicate));
    }

    private Map<Integer, BookieSocketAddress> getReplacedBookiesByIndexes(
                LedgerHandle lh,
                List<BookieSocketAddress> ensemble,
                Set<Integer> bookieIndexesToRereplicate,
                Optional<Set<BookieSocketAddress>> excludedBookies)
            throws BKException.BKNotEnoughBookiesException {
        // target bookies to replicate
        Map<Integer, BookieSocketAddress> targetBookieAddresses =
                Maps.newHashMapWithExpectedSize(bookieIndexesToRereplicate.size());
        // bookies to exclude for ensemble allocation
        Set<BookieSocketAddress> bookiesToExclude = Sets.newHashSet();
        if (excludedBookies.isPresent()) {
            bookiesToExclude.addAll(excludedBookies.get());
        }

        // excluding bookies that need to be replicated
        for (Integer bookieIndex : bookieIndexesToRereplicate) {
            BookieSocketAddress bookie = ensemble.get(bookieIndex);
            bookiesToExclude.add(bookie);
        }

        // allocate bookies
        for (Integer bookieIndex : bookieIndexesToRereplicate) {
            BookieSocketAddress oldBookie = ensemble.get(bookieIndex);
            BookieSocketAddress newBookie =
                    bkc.getPlacementPolicy().replaceBookie(
                            lh.getLedgerMetadata().getEnsembleSize(),
                            lh.getLedgerMetadata().getWriteQuorumSize(),
                            lh.getLedgerMetadata().getAckQuorumSize(),
                            ensemble,
                            oldBookie,
                            bookiesToExclude);
            targetBookieAddresses.put(bookieIndex, newBookie);
            bookiesToExclude.add(newBookie);
        }

        return targetBookieAddresses;
    }

    private ArrayList<BookieSocketAddress> replaceBookiesInEnsemble(
            List<BookieSocketAddress> ensemble,
            Map<Integer, BookieSocketAddress> replacedBookies) {
        ArrayList<BookieSocketAddress> newEnsemble = Lists.newArrayList(ensemble);
        for (Map.Entry<Integer, BookieSocketAddress> entry : replacedBookies.entrySet()) {
            newEnsemble.set(entry.getKey(), entry.getValue());
        }
        return newEnsemble;
    }

    /**
     * Replicate the Ledger fragment to target Bookie passed.
     *
     * @param lh
     *            - ledgerHandle
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     */
    public void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment)
            throws InterruptedException, BKException {
        Optional<Set<BookieSocketAddress>> excludedBookies = Optional.absent();
        Map<Integer, BookieSocketAddress> targetBookieAddresses =
                getReplacedBookiesByIndexes(lh, ledgerFragment.getEnsemble(),
                        ledgerFragment.getBookiesIndexes(), excludedBookies);
        replicateLedgerFragment(lh, ledgerFragment, targetBookieAddresses);
    }

    private void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final Map<Integer, BookieSocketAddress> targetBookieAddresses)
            throws InterruptedException, BKException {
        SyncCounter syncCounter = new SyncCounter();
        ResultCallBack resultCallBack = new ResultCallBack(syncCounter);
        SingleFragmentCallback cb = new SingleFragmentCallback(resultCallBack,
                lh, ledgerFragment.getFirstEntryId(), getReplacedBookiesMap(ledgerFragment, targetBookieAddresses));
        syncCounter.inc();
        Set<BookieSocketAddress> targetBookieSet = new HashSet<BookieSocketAddress>();
        targetBookieSet.addAll(targetBookieAddresses.values());
        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, targetBookieSet);
        syncCounter.block(0);
        if (syncCounter.getrc() != BKException.Code.OK) {
            throw BKException.create(bkc.getReturnRc(syncCounter.getrc()));
        }
    }

    private static Map<BookieSocketAddress, BookieSocketAddress> getReplacedBookiesMap(
            ArrayList<BookieSocketAddress> ensemble,
            Map<Integer, BookieSocketAddress> targetBookieAddresses) {
        Map<BookieSocketAddress, BookieSocketAddress> bookiesMap =
                new HashMap<BookieSocketAddress, BookieSocketAddress>();
        for (Map.Entry<Integer, BookieSocketAddress> entry : targetBookieAddresses.entrySet()) {
            BookieSocketAddress oldBookie = ensemble.get(entry.getKey());
            BookieSocketAddress newBookie = entry.getValue();
            bookiesMap.put(oldBookie, newBookie);
        }
        return bookiesMap;
    }

    private static Map<BookieSocketAddress, BookieSocketAddress> getReplacedBookiesMap(
            LedgerFragment ledgerFragment,
            Map<Integer, BookieSocketAddress> targetBookieAddresses) {
        Map<BookieSocketAddress, BookieSocketAddress> bookiesMap =
                new HashMap<BookieSocketAddress, BookieSocketAddress>();
        for (Integer bookieIndex : ledgerFragment.getBookiesIndexes()) {
            BookieSocketAddress oldBookie = ledgerFragment.getAddress(bookieIndex);
            BookieSocketAddress newBookie = targetBookieAddresses.get(bookieIndex);
            bookiesMap.put(oldBookie, newBookie);
        }
        return bookiesMap;
    }

    private static boolean containBookies(ArrayList<BookieSocketAddress> ensemble, Set<BookieSocketAddress> bookies) {
        for (BookieSocketAddress bookie : ensemble) {
            if (bookies.contains(bookie)) {
                return true;
            }
        }
        return false;
    }

    /** This is the class for getting the replication result */
    static class ResultCallBack implements AsyncCallback.VoidCallback {
        private SyncCounter sync;

        public ResultCallBack(SyncCounter sync) {
            this.sync = sync;
        }

        @Override
        public void processResult(int rc, String s, Object obj) {
            sync.setrc(rc);
            sync.dec();
        }
    }

    /**
     * Format the BookKeeper metadata in zookeeper
     *
     * @param isInteractive
     *            Whether format should ask prompt for confirmation if old data
     *            exists or not.
     * @param force
     *            If non interactive and force is true, then old data will be
     *            removed without prompt.
     * @return Returns true if format succeeds else false.
     */
    public static boolean format(ClientConfiguration conf,
            boolean isInteractive, boolean force) throws Exception {
        ZooKeeper zkc = ZooKeeperClient.createConnectedZooKeeperClient(
                conf.getZkServers(), conf.getZkTimeout(),
                new BoundExponentialBackoffRetryPolicy(conf.getZkTimeout(),
                        3 * conf.getZkTimeout(), Integer.MAX_VALUE));
        BookKeeper bkc = null;
        try {
            boolean ledgerRootExists = null != zkc.exists(
                    conf.getZkLedgersRootPath(), false);
            boolean availableNodeExists = null != zkc.exists(
                    conf.getZkAvailableBookiesPath(), false);

            // Create ledgers root node if not exists
            if (!ledgerRootExists) {
                zkc.create(conf.getZkLedgersRootPath(), "".getBytes(UTF_8),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            // create available bookies node if not exists
            if (!availableNodeExists) {
                zkc.create(conf.getZkAvailableBookiesPath(), "".getBytes(UTF_8),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // If old data was there then confirm with admin.
            if (ledgerRootExists) {
                boolean confirm = false;
                if (!isInteractive) {
                    // If non interactive and force is set, then delete old
                    // data.
                    if (force) {
                        confirm = true;
                    } else {
                        confirm = false;
                    }
                } else {
                    // Confirm with the admin.
                    confirm = IOUtils
                            .confirmPrompt("Are you sure to format bookkeeper metadata ?");
                }
                if (!confirm) {
                    LOG.error("BookKeeper metadata Format aborted!!");
                    return false;
                }
            }
            bkc = new BookKeeper(conf, zkc);
            // Format all ledger metadata layout
            bkc.ledgerManagerFactory.format(conf, zkc);

            // Clear the cookies
            try {
                ZKUtil.deleteRecursive(zkc, conf.getZkLedgersRootPath()
                        + "/cookies");
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("cookies node not exists in zookeeper to delete");
            }

            // Clear the INSTANCEID
            try {
                zkc.delete(conf.getZkLedgersRootPath() + "/" + INSTANCEID, -1);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("INSTANCEID not exists in zookeeper to delete");
            }

            // create INSTANCEID
            String instanceId = UUID.randomUUID().toString();
            zkc.create(conf.getZkLedgersRootPath() + "/" + INSTANCEID,
                    instanceId.getBytes(UTF_8), Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);

            LOG.info("Successfully formatted BookKeeper metadata");
        } finally {
            if (null != bkc) {
                bkc.close();
            }
            if (null != zkc) {
                zkc.close();
            }
        }
        return true;
    }

    /**
     * @return the metadata for the passed ledger handle
     */
    public LedgerMetadata getLedgerMetadata(LedgerHandle lh) {
        return lh.getLedgerMetadata();
    }
}
