/*
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
package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.net.NodeBase;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegionAwareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(RegionAwareEnsemblePlacementPolicy.class);

    static final int REGIONID_DISTANCE_FROM_LEAVES = 2;
    static final String UNKNOWN_REGION = "UnknownRegion";

    protected final Map<String, RackawareEnsemblePlacementPolicy> perRegionPlacement;
    protected final ConcurrentMap<InetSocketAddress, String> address2Region;
    protected String myRegion = null;
    private Configuration conf;

    public RegionAwareEnsemblePlacementPolicy() {
        super();
        perRegionPlacement = new HashMap<String, RackawareEnsemblePlacementPolicy>();
        address2Region = new ConcurrentHashMap<InetSocketAddress, String>();
    }

    protected String getRegion(InetSocketAddress addr) {
        String region = address2Region.get(addr);
        if (null == region) {
            String networkLocation = resolveNetworkLocation(addr);
            if (NetworkTopology.DEFAULT_RACK.equals(networkLocation)) {
                region = UNKNOWN_REGION;
            } else {
                String[] parts = networkLocation.split(NodeBase.PATH_SEPARATOR_STR);
                if (parts.length <= 1) {
                    region = UNKNOWN_REGION;
                } else {
                    region = parts[1];
                }
            }
            address2Region.putIfAbsent(addr, region);
        }
        return region;
    }

    protected String getLocalRegion(BookieNode node) {
        if (null == node || null == node.getAddr()) {
            return UNKNOWN_REGION;
        }
        return getRegion(node.getAddr());
    }

    @Override
    public Set<InetSocketAddress> onClusterChanged(Set<InetSocketAddress> writableBookies,
                                                   Set<InetSocketAddress> readOnlyBookies) {
        rwLock.writeLock().lock();
        try {
            ImmutableSet<InetSocketAddress> joinedBookies, leftBookies, deadBookies;
            Set<InetSocketAddress> oldBookieSet = knownBookies.keySet();
            // left bookies : bookies in known bookies, but not in new writable bookie cluster.
            leftBookies = Sets.difference(oldBookieSet, writableBookies).immutableCopy();
            // joined bookies : bookies in new writable bookie cluster, but not in known bookies
            joinedBookies = Sets.difference(writableBookies, oldBookieSet).immutableCopy();
            // dead bookies.
            deadBookies = Sets.difference(leftBookies, readOnlyBookies).immutableCopy();
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                    "Cluster changed : left bookies are {}, joined bookies are {}, while dead bookies are {}.",
                    new Object[] { leftBookies, joinedBookies, deadBookies });
            }

            // node left
            for (InetSocketAddress addr : leftBookies) {
                BookieNode node = knownBookies.remove(addr);
                topology.remove(node);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} left from cluster.", addr);
                }
            }

            Map<String, Set<InetSocketAddress>> perRegionClusterChange = new HashMap<String, Set<InetSocketAddress>>();

            // node joined
            for (InetSocketAddress addr : joinedBookies) {
                BookieNode node = createBookieNode(addr);
                topology.add(node);
                knownBookies.put(addr, node);
                String region = getLocalRegion(node);
                if (null == perRegionPlacement.get(region)) {
                    perRegionPlacement.put(region, new RackawareEnsemblePlacementPolicy().initialize(conf));
                }

                Set<InetSocketAddress> regionSet = perRegionClusterChange.get(region);
                if (null == regionSet) {
                    regionSet = new HashSet<InetSocketAddress>();
                    regionSet.add(addr);
                    perRegionClusterChange.put(region, regionSet);
                } else {
                    regionSet.add(addr);
                }

                if (LOG.isDebugEnabled()) {
                    LOG.debug("Cluster changed : bookie {} joined the cluster.", addr);
                }
            }

            for(String region: perRegionPlacement.keySet()) {
                perRegionPlacement.get(region).onClusterChangedInternal(leftBookies, perRegionClusterChange.get(region));
            }

            return deadBookies;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public RegionAwareEnsemblePlacementPolicy initialize(Configuration conf) {
        super.initialize(conf);
        this.conf = conf;
        myRegion = getLocalRegion(localNode);
        return this;
    }

    @Override
    public ArrayList<InetSocketAddress> newEnsemble(int ensembleSize, int writeQuorumSize,
                                                    Set<InetSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            RRTopologyAwareCoverageEnsemble ensemble = new RRTopologyAwareCoverageEnsemble(ensembleSize, writeQuorumSize, REGIONID_DISTANCE_FROM_LEAVES);
            BookieNode prevNode = null;
            int numRegions = perRegionPlacement.keySet().size();
            // If we were unable to get region information
            if (numRegions < 1) {
                List<BookieNode> bns = selectRandom(ensembleSize, excludeNodes,
                    EnsembleForReplacement.instance);
                ArrayList<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>(ensembleSize);
                for (BookieNode bn : bns) {
                    addrs.add(bn.getAddr());
                }
                return addrs;
            }

            // Single region, fall back to RackAwareEnsemblePlacement
            if (numRegions < 2) {
                perRegionPlacement.values().iterator().next().newEnsembleInternal(ensembleSize, writeQuorumSize, excludeBookies, ensemble);
            }

            int remainingEnsemble = ensembleSize;
            int remainingWriteQuorum = writeQuorumSize;

            // Equally distribute the nodes across all regions to whatever extent possible
            // with the hierarchy in mind
            // Try and place as many nodes in a region as possible, the ones that cannot be
            // accommodated are placed on other regions
            // Within each region try and follow rack aware placement
            for (String region: perRegionPlacement.keySet()) {
                RackawareEnsemblePlacementPolicy policyWithinRegion = perRegionPlacement.get(region);
                int targetEnsembleSize = Math.min(remainingEnsemble, (ensembleSize + numRegions - 1) / numRegions);
                while(targetEnsembleSize > 0) {
                    int targetWriteQuorum = Math.max(1, Math.min(remainingWriteQuorum, Math.round(1.0f * writeQuorumSize * targetEnsembleSize / ensembleSize)));
                    RRTopologyAwareCoverageEnsemble tempEnsemble = new RRTopologyAwareCoverageEnsemble(ensemble);
                    try {
                        policyWithinRegion.newEnsembleInternal(targetEnsembleSize, targetWriteQuorum, excludeBookies, tempEnsemble);
                        ensemble = tempEnsemble;
                        remainingEnsemble -= targetEnsembleSize;
                        remainingWriteQuorum -= writeQuorumSize;
                        LOG.trace("Allocated {} bookies in region {}", targetEnsembleSize, region);
                        break;
                    } catch (BKException.BKNotEnoughBookiesException exc) {
                        LOG.trace("Could not locate {} bookies in region {}", targetEnsembleSize, region);
                        targetEnsembleSize--;
                    }
                }
            }
            return ensemble.toList();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public InetSocketAddress replaceBookie(InetSocketAddress bookieToReplace,
                                           Set<InetSocketAddress> excludeBookies) throws BKException.BKNotEnoughBookiesException {
        rwLock.readLock().lock();
        try {
            BookieNode bn = knownBookies.get(bookieToReplace);
            if (null == bn) {
                bn = createBookieNode(bookieToReplace);
            }

            Set<Node> excludeNodes = convertBookiesToNodes(excludeBookies);
            // add the bookie to replace in exclude set
            excludeNodes.add(bn);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Try to choose a new bookie to replace {}, excluding {}.", bookieToReplace,
                    excludeNodes);
            }
            // pick a candidate from same rack to replace
            BookieNode candidate = replaceFromRack(bn, excludeNodes,
                TruePredicate.instance, EnsembleForReplacement.instance);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Bookie {} is chosen to replace bookie {}.", candidate, bn);
            }
            return candidate.getAddr();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    protected BookieNode replaceFromRack(BookieNode bn, Set<Node> excludeBookies, Predicate predicate,
                                         Ensemble ensemble) throws BKException.BKNotEnoughBookiesException {
        String region = getLocalRegion(bn);
        RackawareEnsemblePlacementPolicy regionPolicy = perRegionPlacement.get(region);
        if (null != regionPolicy) {
            try {
                // select one from local rack => it falls back to selecting a node from the region if
                // the rack does not have an available node
                return regionPolicy.selectFromRack(bn.getNetworkLocation(), excludeBookies, predicate, ensemble);
            } catch (BKException.BKNotEnoughBookiesException e) {
                LOG.warn("Failed to choose a bookie from {} : "
                    + "excluded {}, fallback to choose bookie randomly from the cluster.",
                    bn.getNetworkLocation(), excludeBookies);
            }
        }

        // randomly choose one from whole cluster, ignore the provided predicate.
        return selectRandom(1, excludeBookies, ensemble).get(0);
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<InetSocketAddress> ensemble, List<Integer> writeSet) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return writeSet;
        } else {
            List<Integer> finalList = new ArrayList<Integer>(writeSet.size());
            List<Integer> reorderList = new ArrayList<Integer>(writeSet.size());
            for (Integer idx : writeSet) {
                InetSocketAddress address = ensemble.get(idx);
                String region = getRegion(address);
                if (region.equals(myRegion)) {
                    finalList.add(idx);
                } else {
                    reorderList.add(idx);
                }
            }
            finalList.addAll(reorderList);
            return finalList;
        }
    }
}
