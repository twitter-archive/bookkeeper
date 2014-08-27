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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
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
    static final int REMOTE_NODE_IN_REORDER_SEQUENCE = 2;

    protected final Map<String, RackawareEnsemblePlacementPolicy> perRegionPlacement;
    protected final ConcurrentMap<InetSocketAddress, String> address2Region;
    protected String myRegion = null;

    RegionAwareEnsemblePlacementPolicy() {
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
    protected void handleBookiesThatLeft(Set<InetSocketAddress> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);

        for(RackawareEnsemblePlacementPolicy policy: perRegionPlacement.values()) {
            policy.handleBookiesThatLeft(leftBookies);
        }
    }

    @Override
    protected void handleBookiesThatJoined(Set<InetSocketAddress> joinedBookies) {
        Map<String, Set<InetSocketAddress>> perRegionClusterChange = new HashMap<String, Set<InetSocketAddress>>();

        // node joined
        for (InetSocketAddress addr : joinedBookies) {
            BookieNode node = createBookieNode(addr);
            topology.add(node);
            knownBookies.put(addr, node);
            String region = getLocalRegion(node);
            if (null == perRegionPlacement.get(region)) {
                perRegionPlacement.put(region, new RackawareEnsemblePlacementPolicy().initialize(dnsResolver));
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
            Set<InetSocketAddress> regionSet = perRegionClusterChange.get(region);
            if (null == regionSet) {
                regionSet = new HashSet<InetSocketAddress>();
            }
            perRegionPlacement.get(region).handleBookiesThatJoined(regionSet);
        }
    }

    @Override
    public RegionAwareEnsemblePlacementPolicy initialize(Configuration conf, Optional<DNSToSwitchMapping> optionalDnsResolver) {
        super.initialize(conf, optionalDnsResolver);
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
                return perRegionPlacement.values().iterator().next().newEnsembleInternal(ensembleSize, writeQuorumSize, excludeBookies, ensemble);
            }

            int remainingEnsemble = ensembleSize;
            int remainingWriteQuorum = writeQuorumSize;

            // Equally distribute the nodes across all regions to whatever extent possible
            // with the hierarchy in mind
            // Try and place as many nodes in a region as possible, the ones that cannot be
            // accommodated are placed on other regions
            // Within each region try and follow rack aware placement
            Set<String> regions = new HashSet<String>(perRegionPlacement.keySet());
            int remainingEnsembleBeforeIteration;
            do {
                remainingEnsembleBeforeIteration = remainingEnsemble;
                Set<String> regionsToExclude = new HashSet<String>();
                for (String region: regions) {
                    RackawareEnsemblePlacementPolicy policyWithinRegion = perRegionPlacement.get(region);
                    int targetEnsembleSize = Math.min(remainingEnsemble, (ensembleSize + numRegions - 1) / numRegions);
                    boolean success = false;
                    while(targetEnsembleSize > 0) {
                        int targetWriteQuorum = Math.max(1, Math.min(remainingWriteQuorum, Math.round(1.0f * writeQuorumSize * targetEnsembleSize / ensembleSize)));
                        RRTopologyAwareCoverageEnsemble tempEnsemble = new RRTopologyAwareCoverageEnsemble(ensemble);
                        try {
                            policyWithinRegion.newEnsembleInternal(targetEnsembleSize, targetWriteQuorum, excludeBookies, tempEnsemble);
                            ensemble = tempEnsemble;
                            remainingEnsemble -= targetEnsembleSize;
                            remainingWriteQuorum -= writeQuorumSize;
                            success = true;
                            LOG.info("Allocated {} bookies in region {} : {}",
                                    new Object[]{targetEnsembleSize, region, ensemble});
                            break;
                        } catch (BKException.BKNotEnoughBookiesException exc) {
                            LOG.warn("Could not allocate {} bookies in region {}, try allocating {} bookies",
                                     new Object[] { targetEnsembleSize, region, (targetEnsembleSize - 1) });
                            targetEnsembleSize--;
                        }
                    }
                    // we couldn't allocate bookies from a region, exclude it.
                    if (!success) {
                        regionsToExclude.add(region);
                    }
                }
                regions.removeAll(regionsToExclude);

                if (regions.isEmpty()) {
                    break;
                }
            } while ((remainingEnsemble > 0) && (remainingEnsemble < remainingEnsembleBeforeIteration));

            ArrayList<InetSocketAddress> bookieList = ensemble.toList();
            if (ensembleSize != bookieList.size()) {
                LOG.error("Not enough {} bookies are available to form an ensemble : {}.",
                          ensembleSize, bookieList);
                throw new BKException.BKNotEnoughBookiesException();
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
            return super.reorderReadSequence(ensemble, writeSet);
        } else {
            List<Integer> finalList = new ArrayList<Integer>(writeSet.size());
            List<Integer> localList = new ArrayList<Integer>(writeSet.size());
            List<Integer> remoteList = new ArrayList<Integer>(writeSet.size());
            List<Integer> readOnlyList = new ArrayList<Integer>(writeSet.size());
            List<Integer> unAvailableList = new ArrayList<Integer>(writeSet.size());
            for (Integer idx : writeSet) {
                InetSocketAddress address = ensemble.get(idx);
                String region = getRegion(address);
                if (null == knownBookies.get(address)) {
                    // there isn't too much differences between readonly bookies from unavailable bookies. since there
                    // is no write requests to them, so we shouldn't try reading from readonly bookie in prior to writable
                    // bookies.
                    if ((null == readOnlyBookies) || !readOnlyBookies.contains(address)) {
                        unAvailableList.add(idx);
                    } else {
                        readOnlyList.add(idx);
                    }
                } else if (region.equals(myRegion)) {
                    localList.add(idx);
                } else {
                    remoteList.add(idx);
                }
            }

            // Insert a node from the remote region at the specified location so we
            // try more than one region within the max allowed latency
            for (int i = 0; i < REMOTE_NODE_IN_REORDER_SEQUENCE; i++) {
                if (localList.size() > 0) {
                    finalList.add(localList.remove(0));
                } else {
                    break;
                }
            }

            if (remoteList.size() > 0) {
                finalList.add(remoteList.remove(0));
            }

            finalList.addAll(localList);
            finalList.addAll(remoteList);
            finalList.addAll(readOnlyList);
            finalList.addAll(unAvailableList);
            return finalList;
        }
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<InetSocketAddress> ensemble, List<Integer> writeSet) {
        if (UNKNOWN_REGION.equals(myRegion)) {
            return super.reorderReadLACSequence(ensemble, writeSet);
        }
        List<Integer> finalList = reorderReadSequence(ensemble, writeSet);

        if (finalList.size() < ensemble.size()) {
            for (int i = 0; i < ensemble.size(); i++) {
                if (!finalList.contains(i)) {
                    finalList.add(i);
                }
            }
        }
        return finalList;

    }
}
