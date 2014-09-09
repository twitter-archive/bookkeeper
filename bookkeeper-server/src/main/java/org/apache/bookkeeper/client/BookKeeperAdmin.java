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
import java.net.InetSocketAddress;
import java.util.ArrayList;
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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.AsyncCallback.OpenCallback;
import org.apache.bookkeeper.client.AsyncCallback.RecoverCallback;
import org.apache.bookkeeper.client.BookKeeper.SyncOpenCallback;
import org.apache.bookkeeper.client.LedgerFragmentReplicator.SingleFragmentCallback;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.MultiCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.bookkeeper.zookeeper.BoundExponentialBackoffRetryPolicy;
import org.apache.bookkeeper.zookeeper.ZooKeeperClient;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;

/**
 * Admin client for BookKeeper clusters
 */
public class BookKeeperAdmin {
    private static Logger LOG = LoggerFactory.getLogger(BookKeeperAdmin.class);

    static final String COLON = ":";

    // ZK client instance
    private ZooKeeper zk;

    // BookKeeper client instance
    private BookKeeper bkc;

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
        // Create the BookKeeper client instance
        bkc = new BookKeeper(conf, zk);
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
        this.zk = bkc.zk;
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
        bkc.close();
        zk.close();
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

    public SortedMap<Long, LedgerMetadata> getLedgersContainBookies(Set<InetSocketAddress> bookies)
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

    public void asyncGetLedgersContainBookies(final Set<InetSocketAddress> bookies,
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
                        Set<InetSocketAddress> bookiesInLedger = metadata.getBookiesInThisLedger();
                        Sets.SetView<InetSocketAddress> intersection =
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
    public void recoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest)
            throws InterruptedException, BKException {
        Set<InetSocketAddress> bookiesSrc = new HashSet<InetSocketAddress>();
        bookiesSrc.add(bookieSrc);
        recoverBookieData(bookiesSrc);
    }

    public void recoverBookieData(final Set<InetSocketAddress> bookiesSrc)
        throws InterruptedException, BKException {
        recoverBookieData(bookiesSrc, false, false);
    }

    public void recoverBookieData(final Set<InetSocketAddress> bookiesSrc, boolean dryrun, boolean skipOpenLedgers)
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

    public void recoverBookieData(final long lid, final Set<InetSocketAddress> bookiesSrc, boolean dryrun, boolean skipOpenLedgers)
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
    public void asyncRecoverBookieData(final InetSocketAddress bookieSrc, final InetSocketAddress bookieDest,
                                       final RecoverCallback cb, final Object context) {
        Set<InetSocketAddress> bookiesSrc = new HashSet<InetSocketAddress>();
        bookiesSrc.add(bookieSrc);
        asyncRecoverBookieData(bookiesSrc, cb, context);
    }

    public void asyncRecoverBookieData(final Set<InetSocketAddress> bookieSrc,
                                       final RecoverCallback cb, final Object context) {
        asyncRecoverBookieData(bookieSrc, false, false, cb, context);
    }

    public void asyncRecoverBookieData(final Set<InetSocketAddress> bookieSrc, boolean dryrun,
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
    public void asyncRecoverBookieData(long lid, final Set<InetSocketAddress> bookieSrc, boolean dryrun,
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
    private void getActiveLedgers(final Set<InetSocketAddress> bookiesSrc, final boolean dryrun,
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
    private void recoverLedger(final Set<InetSocketAddress> bookiesSrc, final long lId, final boolean dryrun,
                               final boolean skipOpenLedgers, final AsyncCallback.VoidCallback finalLedgerIterCb) {
        LOG.debug("Recovering ledger : {}", lId);

        asyncOpenLedgerNoRecovery(lId, new OpenCallback() {
            @Override
            public void openComplete(int rc, final LedgerHandle lh, Object ctx) {
                if (rc != Code.OK.intValue()) {
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
                        LOG.warn("Error on cloing ledger handle for {}.", lId);
                    }
                    finalLedgerIterCb.processResult(BKException.Code.OK, null, null);
                    return;
                }

                boolean fenceRequired = false;
                if (!lm.isClosed() &&
                    lm.getEnsembles().size() > 0) {
                    Long lastKey = lm.getEnsembles().lastKey();
                    ArrayList<InetSocketAddress> lastEnsemble = lm.getEnsembles().get(lastKey);
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
                                if (newrc != Code.OK.intValue()) {
                                    LOG.error("BK error close ledger: " + lId, BKException.create(newrc));
                                    finalLedgerIterCb.processResult(newrc, null, null);
                                    return;
                                }
                                // do recovery
                                recoverLedger(bookiesSrc, lId, dryrun, skipOpenLedgers, finalLedgerIterCb);
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
                for (Map.Entry<Long, ArrayList<InetSocketAddress>> entry : lh.getLedgerMetadata().getEnsembles()
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
                    ArrayList<InetSocketAddress> ensemble = lh.getLedgerMetadata().getEnsembles().get(startEntryId);
                    ArrayList<InetSocketAddress> newEnsemble = new ArrayList<InetSocketAddress>();
                    // Construct a new ensemble
                    newEnsemble.addAll(ensemble);
                    // Get bookies to replace
                    Set<Integer> bookieIndexesReplaced= new HashSet<Integer>();
                    Set<InetSocketAddress> targetBookieAddresses = new HashSet<InetSocketAddress>();
                    Map<InetSocketAddress, InetSocketAddress> oldBookie2NewBookieMap =
                            new HashMap<InetSocketAddress, InetSocketAddress>();
                    try {
                        Set<InetSocketAddress> bookiesToExclude = new HashSet<InetSocketAddress>();
                        bookiesToExclude.addAll(bookiesSrc);
                        for (int bookieIndex = 0; bookieIndex < ensemble.size(); bookieIndex++) {
                            InetSocketAddress bookieInEnsemble = ensemble.get(bookieIndex);
                            if (bookiesSrc.contains(bookieInEnsemble)) {
                                InetSocketAddress newBookie =
                                        bkc.getPlacementPolicy().replaceBookie(lh.getLedgerMetadata().getEnsembleSize(),
                                                            lh.getLedgerMetadata().getWriteQuorumSize(),
                                                            lh.getLedgerMetadata().getAckQuorumSize(),
                                                            ensemble,
                                                            bookieInEnsemble,
                                                            bookiesToExclude);
                                newEnsemble.set(bookieIndex, newBookie);
                                bookieIndexesReplaced.add(bookieIndex);
                                targetBookieAddresses.add(newBookie);
                                oldBookie2NewBookieMap.put(bookieInEnsemble, newBookie);
                                // exclude new bookie for following allocation
                                bookiesToExclude.add(newBookie);
                            }
                        }
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
                                ledgerFragmentsMcb, lh, startEntryId, oldBookie2NewBookieMap);
                        LedgerFragment ledgerFragment = new LedgerFragment(lh,
                                startEntryId, endEntryId, bookieIndexesReplaced);
                        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, targetBookieAddresses);
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

    static String formatEnsemble(ArrayList<InetSocketAddress> ensemble, Set<InetSocketAddress> bookiesSrc, char marker) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < ensemble.size(); i++) {
            sb.append(StringUtils.addrToString(ensemble.get(i)));
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
            final Set<InetSocketAddress> newBookies) throws InterruptedException {
        lfr.replicate(lh, ledgerFragment, ledgerFragmentMcb, newBookies);
    }

    /**
     * Replicate the Ledger fragment to target Bookie passed.
     *
     * @param lh
     *            - ledgerHandle
     * @param ledgerFragment
     *            - LedgerFragment to replicate
     * @param targetBookieAddress
     *            - target Bookie, to where entries should be replicated.
     */
    public void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final InetSocketAddress targetBookieAddress)
            throws InterruptedException, BKException {
        Preconditions.checkArgument(ledgerFragment.getBookiesIndexes().size() == 1);
        Map<Integer, InetSocketAddress> targetBookieAddresses =
                new HashMap<Integer, InetSocketAddress>();
        targetBookieAddresses.put(ledgerFragment.getBookiesIndexes().iterator().next(), targetBookieAddress);
        replicateLedgerFragment(lh, ledgerFragment, targetBookieAddresses);
    }

    public void replicateLedgerFragment(LedgerHandle lh,
            final LedgerFragment ledgerFragment,
            final Map<Integer, InetSocketAddress> targetBookieAddresses)
            throws InterruptedException, BKException {
        SyncCounter syncCounter = new SyncCounter();
        ResultCallBack resultCallBack = new ResultCallBack(syncCounter);
        SingleFragmentCallback cb = new SingleFragmentCallback(resultCallBack,
                lh, ledgerFragment.getFirstEntryId(), getReplacedBookiesMap(ledgerFragment, targetBookieAddresses));
        syncCounter.inc();
        Set<InetSocketAddress> targetBookieSet = new HashSet<InetSocketAddress>();
        targetBookieSet.addAll(targetBookieAddresses.values());
        asyncRecoverLedgerFragment(lh, ledgerFragment, cb, targetBookieSet);
        syncCounter.block(0);
        if (syncCounter.getrc() != BKException.Code.OK) {
            throw BKException.create(bkc.getReturnRc(syncCounter.getrc()));
        }
    }

    private static Map<InetSocketAddress, InetSocketAddress> getReplacedBookiesMap(LedgerFragment ledgerFragment,
            Map<Integer, InetSocketAddress> targetBookieAddresses) {
        Map<InetSocketAddress, InetSocketAddress> bookiesMap = new HashMap<InetSocketAddress, InetSocketAddress>();
        for (Integer bookieIndex : ledgerFragment.getBookiesIndexes()) {
            InetSocketAddress oldBookie = ledgerFragment.getAddress(bookieIndex);
            InetSocketAddress newBookie = targetBookieAddresses.get(bookieIndex);
            bookiesMap.put(oldBookie, newBookie);
        }
        return bookiesMap;
    }

    private static boolean containBookies(ArrayList<InetSocketAddress> ensemble, Set<InetSocketAddress> bookies) {
        for (InetSocketAddress bookie : ensemble) {
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
                zkc.delete(conf.getZkLedgersRootPath() + "/" + Bookie.INSTANCEID, -1);
            } catch (KeeperException.NoNodeException e) {
                LOG.debug("INSTANCEID not exists in zookeeper to delete");
            }

            // create INSTANCEID
            String instanceId = UUID.randomUUID().toString();
            zkc.create(conf.getZkLedgersRootPath() + "/" + Bookie.INSTANCEID,
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
}
