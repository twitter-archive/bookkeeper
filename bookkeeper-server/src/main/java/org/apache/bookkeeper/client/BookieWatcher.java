package org.apache.bookkeeper.client;

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
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.util.SafeRunnable;
import org.apache.bookkeeper.util.StringUtils;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for maintaining a consistent view of what bookies
 * are available by reading Zookeeper (and setting watches on the bookie nodes).
 * When a bookie fails, the other parts of the code turn to this class to find a
 * replacement
 *
 */
class BookieWatcher implements Watcher, ChildrenCallback {
    static final Logger logger = LoggerFactory.getLogger(BookieWatcher.class);

    public static int ZK_CONNECT_BACKOFF_SEC = 1;
    private static final Set<InetSocketAddress> EMPTY_SET = new HashSet<InetSocketAddress>();

    // Bookie registration path in ZK
    private final String bookieRegistrationPath;

    final BookKeeper bk;
    final ScheduledExecutorService scheduler;
    final EnsemblePlacementPolicy placementPolicy;

    SafeRunnable reReadTask = new SafeRunnable() {
        @Override
        public void safeRun() {
            readBookies();
        }
    };
    private final ReadOnlyBookieWatcher readOnlyBookieWatcher;

    public BookieWatcher(ClientConfiguration conf,
                         ScheduledExecutorService scheduler,
                         EnsemblePlacementPolicy placementPolicy,
                         BookKeeper bk) throws KeeperException, InterruptedException  {
        this.bk = bk;
        // ZK bookie registration path
        this.bookieRegistrationPath = conf.getZkAvailableBookiesPath();
        this.scheduler = scheduler;
        this.placementPolicy = placementPolicy;
        readOnlyBookieWatcher = new ReadOnlyBookieWatcher(conf, bk);
    }

    public Collection<InetSocketAddress> getBookies() {
        try {
            List<String> children = bk.getZkHandle().getChildren(this.bookieRegistrationPath, false);
            return convertToBookieAddresses(children);
        } catch (Exception e) {
            logger.error("Failed to get bookies : ", e);
            return new HashSet<InetSocketAddress>();
        }
    }

    public void readBookies() {
        readBookies(this);
    }

    public void readBookies(ChildrenCallback callback) {
        bk.getZkHandle().getChildren(this.bookieRegistrationPath, this, callback, null);
    }

    @Override
    public void process(WatchedEvent event) {
        readBookies();
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {

        if (rc != KeeperException.Code.OK.intValue()) {
            //logger.error("Error while reading bookies", KeeperException.create(Code.get(rc), path));
            // try the read after a second again
            try {
                scheduler.schedule(reReadTask, ZK_CONNECT_BACKOFF_SEC, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ree) {
                logger.warn("Failed to schedule reading bookies task : ", ree);
            }
            return;
        }

        // Just exclude the 'readonly' znode to exclude r-o bookies from
        // available nodes list.
        children.remove(Bookie.READONLY);

        HashSet<InetSocketAddress> newBookieAddrs = convertToBookieAddresses(children);

        final Set<InetSocketAddress> deadBookies;
        synchronized (this) {
            Set<InetSocketAddress> readonlyBookies = readOnlyBookieWatcher.getReadOnlyBookies();
            deadBookies = placementPolicy.onClusterChanged(newBookieAddrs, readonlyBookies);
        }

        // we don't need to close clients here, because:
        // a. the dead bookies will be removed from topology, which will not be used in new ensemble.
        // b. the read sequence will be reordered based on znode availability, so most of the reads
        //    will not be sent to them.
        // c. the close here is just to disconnect the channel, which doesn't remove the channel from
        //    from pcbc map. we don't really need to disconnect the channel here, since if a bookie is
        //    really down, PCBC will disconnect itself based on netty callback. if we try to disconnect
        //    here, it actually introduces side-effects on case d.
        // d. closing the client here will affect latency if the bookie is alive but just being flaky
        //    on its znode registration due zookeeper session expire.
        // e. if we want to permanently remove a bookkeeper client, we should watch on the cookies' list.
        // if (bk.getBookieClient() != null) {
        //     bk.getBookieClient().closeClients(deadBookies);
        // }
    }

    private static HashSet<InetSocketAddress> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<InetSocketAddress> newBookieAddrs = new HashSet<InetSocketAddress>();
        for (String bookieAddrString : children) {
            InetSocketAddress bookieAddr;
            try {
                bookieAddr = StringUtils.parseAddr(bookieAddrString);
            } catch (IOException e) {
                logger.error("Could not parse bookie address: " + bookieAddrString
                        + ", ignoring this bookie : ", e);
                continue;
            }
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

    /**
     * Blocks until bookies are read from zookeeper, used in the {@link BookKeeper} constructor.
     * @throws InterruptedException
     * @throws KeeperException
     */
    public void readBookiesBlocking() throws InterruptedException, KeeperException {
        // Read readonly bookies first
        readOnlyBookieWatcher.readROBookiesBlocking();

        final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
        readBookies(new ChildrenCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children) {
                try {
                    BookieWatcher.this.processResult(rc, path, ctx, children);
                    queue.put(rc);
                } catch (InterruptedException e) {
                    logger.error("Interruped when trying to read bookies in a blocking fashion : ", e);
                    throw new RuntimeException(e);
                }
            }
        });
        int rc = queue.take();

        if (rc != KeeperException.Code.OK.intValue()) {
            throw KeeperException.create(Code.get(rc));
        }
    }

    /**
     * Wrapper over the {@link #getAdditionalBookies(Set, int)} method when there is no exclusion list (or exisiting bookies)
     * @param ensembleSize
     *          Ensemble Size
     * @param writeQuorumSize
     *          Write Quorum Size
     * @return list of bookies for new ensemble.
     * @throws BKNotEnoughBookiesException
     */
    public ArrayList<InetSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize)
            throws BKNotEnoughBookiesException {
        return placementPolicy.newEnsemble(ensembleSize, writeQuorumSize, EMPTY_SET);
    }

    /**
     * Wrapper over the {@link #getAdditionalBookies(Set, int)} method when you just need 1 extra bookie
     * @param existingBookies
     * @return
     * @throws BKNotEnoughBookiesException
     */
    public InetSocketAddress replaceBookie(List<InetSocketAddress> existingBookies, int bookieIdx)
            throws BKNotEnoughBookiesException {
        InetSocketAddress addr = existingBookies.get(bookieIdx);
        return placementPolicy.replaceBookie(addr, new HashSet<InetSocketAddress>(existingBookies));
    }

    /**
     * Watcher implementation to watch the readonly bookies under
     * &lt;available&gt;/readonly
     */
    private static class ReadOnlyBookieWatcher implements Watcher, ChildrenCallback {

        private final static Logger LOG = LoggerFactory.getLogger(ReadOnlyBookieWatcher.class);
        private HashSet<InetSocketAddress> readOnlyBookies = new HashSet<InetSocketAddress>();
        private final BookKeeper bk;
        private final String readOnlyBookieRegPath;

        public ReadOnlyBookieWatcher(ClientConfiguration conf, BookKeeper bk) throws KeeperException,
                InterruptedException {
            this.bk = bk;
            readOnlyBookieRegPath = conf.getZkAvailableBookiesPath() + "/" + Bookie.READONLY;
            if (null == bk.getZkHandle().exists(readOnlyBookieRegPath, false)) {
                try {
                    bk.getZkHandle().create(readOnlyBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
                } catch (NodeExistsException e) {
                    // this node is just now created by someone.
                }
            }
        }

        @Override
        public void process(WatchedEvent event) {
            readROBookies();
        }

        // read the readonly bookies in blocking fashion. Used only for first
        // time.
        void readROBookiesBlocking() throws InterruptedException, KeeperException {

            final LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<Integer>();
            readROBookies(new ChildrenCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, List<String> children) {
                    try {
                        ReadOnlyBookieWatcher.this.processResult(rc, path, ctx, children);
                        queue.put(rc);
                    } catch (InterruptedException e) {
                        logger.error("Interruped when trying to read readonly bookies in a blocking fashion");
                        throw new RuntimeException(e);
                    }
                }
            });
            int rc = queue.take();

            if (rc != KeeperException.Code.OK.intValue()) {
                throw KeeperException.create(Code.get(rc));
            }
        }

        // Read children and register watcher for readonly bookies path
        void readROBookies(ChildrenCallback callback) {
            bk.getZkHandle().getChildren(this.readOnlyBookieRegPath, this, callback, null);
        }

        void readROBookies() {
            readROBookies(this);
        }

        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            if (rc != Code.OK.intValue()) {
                LOG.error("Not able to read readonly bookies : ", KeeperException.create(Code.get(rc)));
                return;
            }

            HashSet<InetSocketAddress> newReadOnlyBookies = convertToBookieAddresses(children);
            readOnlyBookies = newReadOnlyBookies;
        }

        // returns the readonly bookies
        public HashSet<InetSocketAddress> getReadOnlyBookies() {
            return readOnlyBookies;
        }
    }
}
