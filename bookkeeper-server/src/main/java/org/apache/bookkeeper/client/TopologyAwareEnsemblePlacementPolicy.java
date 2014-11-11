package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.bookkeeper.net.NodeBase;
import org.apache.bookkeeper.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class TopologyAwareEnsemblePlacementPolicy implements EnsemblePlacementPolicy {
    static final Logger LOG = LoggerFactory.getLogger(TopologyAwareEnsemblePlacementPolicy.class);
    /**
     * Predicate used when choosing an ensemble.
     */
    protected static interface Predicate {
        boolean apply(BookieNode candidate, Ensemble chosenBookies);
    }

    /**
     * Ensemble used to hold the result of an ensemble selected for placement.
     */
    protected static interface Ensemble {

        /**
         * Append the new bookie node to the ensemble only if the ensemble doesnt
         * already contain the same bookie
         *
         * @param node
         *          new candidate bookie node.
         * @return
         *          true if the node was added
         */
        public boolean addBookie(BookieNode node);

        /**
         * @return list of addresses representing the ensemble
         */
        public ArrayList<InetSocketAddress> toList();

        /**
         * Validates if an ensemble is valid
         *
         * @return true if the ensemble is valid; false otherwise
         */
        public boolean validate();

    }

    protected static class TruePredicate implements Predicate {

        public static final TruePredicate instance = new TruePredicate();

        @Override
        public boolean apply(BookieNode candidate, Ensemble chosenNodes) {
            return true;
        }

    }

    protected static class EnsembleForReplacementWithNoConstraints implements Ensemble {

        public static final EnsembleForReplacementWithNoConstraints instance = new EnsembleForReplacementWithNoConstraints();
        static final ArrayList<InetSocketAddress> EMPTY_LIST = new ArrayList<InetSocketAddress>(0);

        @Override
        public boolean addBookie(BookieNode node) {
            // do nothing
            return true;
        }

        @Override
        public ArrayList<InetSocketAddress> toList() {
            return EMPTY_LIST;
        }

        /**
         * Validates if an ensemble is valid
         *
         * @return true if the ensemble is valid; false otherwise
         */
        @Override
        public boolean validate() {
            return true;
        }

    }

    protected static class BookieNode extends NodeBase {

        private final InetSocketAddress addr; // identifier of a bookie node.

        BookieNode(InetSocketAddress addr, String networkLoc) {
            super(StringUtils.addrToString(addr), networkLoc);
            this.addr = addr;
        }

        public InetSocketAddress getAddr() {
            return addr;
        }

        @Override
        public int hashCode() {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof BookieNode)) {
                return false;
            }
            BookieNode other = (BookieNode) obj;
            return getName().equals(other.getName());
        }

        @Override
        public String toString() {
            return String.format("<Bookie:%s>", name);
        }

    }

    /**
     * A predicate checking the rack coverage for write quorum in {@link RoundRobinDistributionSchedule},
     * which ensures that a write quorum should be covered by at least two racks.
     */
    protected static class RRTopologyAwareCoverageEnsemble implements Predicate, Ensemble {

        protected interface CoverageSet extends Cloneable {
            boolean apply(BookieNode candidate);
            void addBookie(BookieNode candidate);
            public CoverageSet clone();
        }

        protected class RackQuorumCoverageSet implements CoverageSet {
            HashSet<String> racksOrRegionsInQuorum = new HashSet<String>();
            int seenBookies = 0;

            @Override
            public boolean apply(BookieNode candidate) {
                // If we don't have sufficient members in the write quorum; then we cant enforce
                // rack/region diversity
                if (writeQuorumSize < 2) {
                    return true;
                }

                if (seenBookies + 1 == writeQuorumSize) {
                    return racksOrRegionsInQuorum.size() > (racksOrRegionsInQuorum.contains(candidate.getNetworkLocation(distanceFromLeaves)) ? 1 : 0);
                }
                return true;
            }

            @Override
            public void addBookie(BookieNode candidate) {
                ++seenBookies;
                racksOrRegionsInQuorum.add(candidate.getNetworkLocation(distanceFromLeaves));
            }

            @Override
            public RackQuorumCoverageSet clone() {
                RackQuorumCoverageSet ret = new RackQuorumCoverageSet();
                ret.racksOrRegionsInQuorum = (HashSet<String>)this.racksOrRegionsInQuorum.clone();
                ret.seenBookies = this.seenBookies;
                return ret;
            }
        }

        protected class RackOrRegionDurabilityCoverageSet implements CoverageSet {
            HashMap<String, Integer> allocationToRacksOrRegions = new HashMap<String, Integer>();

            RackOrRegionDurabilityCoverageSet() {
                for (String rackOrRegion: racksOrRegions) {
                    allocationToRacksOrRegions.put(rackOrRegion, 0);
                }
            }

            @Override
            public RackOrRegionDurabilityCoverageSet clone() {
                RackOrRegionDurabilityCoverageSet ret = new RackOrRegionDurabilityCoverageSet();
                ret.allocationToRacksOrRegions = (HashMap<String, Integer>)this.allocationToRacksOrRegions.clone();
                return ret;
            }

            private boolean checkSumOfSubsetWithinLimit(final Set<String> includedRacksOrRegions,
                            final Set<String> remainingRacksOrRegions,
                            int subsetSize,
                            int maxAllowedSum) {
                if (remainingRacksOrRegions.isEmpty() || (subsetSize <= 0)) {
                    if (maxAllowedSum < 0) {
                        LOG.trace("CHECK FAILED: RacksOrRegions Included {} Remaining {}, subsetSize {}, maxAllowedSum {}", new Object[]{
                            includedRacksOrRegions, remainingRacksOrRegions, subsetSize, maxAllowedSum
                        });
                    }
                    return (maxAllowedSum >= 0);
                }

                for(String rackOrRegion: remainingRacksOrRegions) {
                    if (allocationToRacksOrRegions.get(rackOrRegion) > maxAllowedSum) {
                        LOG.trace("CHECK FAILED: RacksOrRegions Included {} Candidate {}, subsetSize {}, maxAllowedSum {}", new Object[]{
                            includedRacksOrRegions, rackOrRegion, subsetSize, maxAllowedSum
                        });
                        return false;
                    } else {
                        Set<String> remainingElements = new HashSet<String>(remainingRacksOrRegions);
                        Set<String> includedElements = new HashSet<String>(includedRacksOrRegions);
                        includedElements.add(rackOrRegion);
                        remainingElements.remove(rackOrRegion);
                        if (!checkSumOfSubsetWithinLimit(includedElements,
                            remainingElements,
                            subsetSize - 1,
                            maxAllowedSum - allocationToRacksOrRegions.get(rackOrRegion))) {
                            return false;
                        }
                    }
                }

                return true;
            }

            @Override
            public boolean apply(BookieNode candidate) {
                String candidateRackOrRegion = candidate.getNetworkLocation(distanceFromLeaves);
                candidateRackOrRegion = candidateRackOrRegion.startsWith(NodeBase.PATH_SEPARATOR_STR) ? candidateRackOrRegion.substring(1) : candidateRackOrRegion;
                final Set<String> remainingRacksOrRegions = new HashSet<String>(racksOrRegions);
                remainingRacksOrRegions.remove(candidateRackOrRegion);
                final Set<String> includedRacksOrRegions = new HashSet<String>();
                includedRacksOrRegions.add(candidateRackOrRegion);

                // If minRacksOrRegionsForDurability are required for durability; we must ensure that
                // no subset of (minRacksOrRegionsForDurability - 1) regions have ackQuorumSize
                // We are only modifying candidateRackOrRegion if we accept this bookie, so lets only
                // find sets that contain this candidateRackOrRegion
                int inclusiveLimit = (ackQuorumSize - 1) - (allocationToRacksOrRegions.get(candidateRackOrRegion) + 1);
                return checkSumOfSubsetWithinLimit(includedRacksOrRegions,
                        remainingRacksOrRegions, minRacksOrRegionsForDurability - 2, inclusiveLimit);
            }

            @Override
            public void addBookie(BookieNode candidate) {
                String candidateRackOrRegion = candidate.getNetworkLocation(distanceFromLeaves);
                candidateRackOrRegion = candidateRackOrRegion.startsWith(NodeBase.PATH_SEPARATOR_STR) ? candidateRackOrRegion.substring(1) : candidateRackOrRegion;
                int oldCount = 0;
                if (null != allocationToRacksOrRegions.get(candidateRackOrRegion)) {
                    oldCount = allocationToRacksOrRegions.get(candidateRackOrRegion);
                }
                allocationToRacksOrRegions.put(candidateRackOrRegion, oldCount + 1);
            }
        }



        final int distanceFromLeaves;
        final int ensembleSize;
        final int writeQuorumSize;
        final int ackQuorumSize;
        final int minRacksOrRegionsForDurability;
        final ArrayList<BookieNode> chosenNodes;
        final Set<String> racksOrRegions;
        private final CoverageSet[] quorums;
        final RRTopologyAwareCoverageEnsemble parentEnsemble;

        protected RRTopologyAwareCoverageEnsemble(RRTopologyAwareCoverageEnsemble that) {
            this.distanceFromLeaves = that.distanceFromLeaves;
            this.ensembleSize = that.ensembleSize;
            this.writeQuorumSize = that.writeQuorumSize;
            this.ackQuorumSize = that.ackQuorumSize;
            this.chosenNodes = (ArrayList<BookieNode>)that.chosenNodes.clone();
            this.quorums = new CoverageSet[that.quorums.length];
            for (int i = 0; i < that.quorums.length; i++) {
                if (null != that.quorums[i]) {
                    this.quorums[i] = that.quorums[i].clone();
                } else {
                    this.quorums[i] = null;
                }
            }
            this.parentEnsemble = that.parentEnsemble;
            if (null != that.racksOrRegions) {
                this.racksOrRegions = new HashSet<String>(that.racksOrRegions);
            } else {
                this.racksOrRegions = null;
            }
            this.minRacksOrRegionsForDurability = that.minRacksOrRegionsForDurability;
        }

        protected RRTopologyAwareCoverageEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize, int distanceFromLeaves, Set<String> racksOrRegions, int minRacksOrRegionsForDurability) {
            this(ensembleSize, writeQuorumSize, ackQuorumSize, distanceFromLeaves, null, racksOrRegions, minRacksOrRegionsForDurability);
        }

        protected RRTopologyAwareCoverageEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize, int distanceFromLeaves, RRTopologyAwareCoverageEnsemble parentEnsemble) {
            this(ensembleSize, writeQuorumSize, ackQuorumSize, distanceFromLeaves, parentEnsemble, null, 0);
        }

        protected RRTopologyAwareCoverageEnsemble(int ensembleSize, int writeQuorumSize, int ackQuorumSize, int distanceFromLeaves, RRTopologyAwareCoverageEnsemble parentEnsemble, Set<String> racksOrRegions, int minRacksOrRegionsForDurability) {
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.ackQuorumSize = ackQuorumSize;
            this.distanceFromLeaves = distanceFromLeaves;
            this.chosenNodes = new ArrayList<BookieNode>(ensembleSize);
            if (minRacksOrRegionsForDurability > 0) {
                this.quorums = new RackOrRegionDurabilityCoverageSet[ensembleSize];
            } else {
                this.quorums = new RackQuorumCoverageSet[ensembleSize];
            }
            this.parentEnsemble = parentEnsemble;
            this.racksOrRegions = racksOrRegions;
            this.minRacksOrRegionsForDurability = minRacksOrRegionsForDurability;
        }

        @Override
        public boolean apply(BookieNode candidate, Ensemble ensemble) {
            if (ensemble != this) {
                return false;
            }

            // An ensemble cannot contain the same node twice
            if (chosenNodes.contains(candidate)) {
                return false;
            }

            // candidate position
            if ((ensembleSize == writeQuorumSize) && (minRacksOrRegionsForDurability > 0)) {
                if (null == quorums[0]) {
                    quorums[0] = new RackOrRegionDurabilityCoverageSet();
                }
                if (!quorums[0].apply(candidate)) {
                    return false;
                }
            } else {
                int candidatePos = chosenNodes.size();
                int startPos = candidatePos - writeQuorumSize + 1;
                for (int i = startPos; i <= candidatePos; i++) {
                    int idx = (i + ensembleSize) % ensembleSize;
                    if (null == quorums[idx]) {
                        if (minRacksOrRegionsForDurability > 0) {
                            quorums[idx] = new RackOrRegionDurabilityCoverageSet();
                        } else {
                            quorums[idx] = new RackQuorumCoverageSet();
                        }
                    }
                    if (!quorums[idx].apply(candidate)) {
                        return false;
                    }
                }
            }

            return ((null == parentEnsemble) || parentEnsemble.apply(candidate, parentEnsemble));
        }

        @Override
        public boolean addBookie(BookieNode node) {
            // An ensemble cannot contain the same node twice
            if (chosenNodes.contains(node)) {
                return false;
            }

            if ((ensembleSize == writeQuorumSize) && (minRacksOrRegionsForDurability > 0)) {
                if (null == quorums[0]) {
                    quorums[0] = new RackOrRegionDurabilityCoverageSet();
                }
                quorums[0].addBookie(node);
            } else {
                int candidatePos = chosenNodes.size();
                int startPos = candidatePos - writeQuorumSize + 1;
                for (int i = startPos; i <= candidatePos; i++) {
                    int idx = (i + ensembleSize) % ensembleSize;
                    if (null == quorums[idx]) {
                        if (minRacksOrRegionsForDurability > 0) {
                            quorums[idx] = new RackOrRegionDurabilityCoverageSet();
                        } else {
                            quorums[idx] = new RackQuorumCoverageSet();
                        }
                    }
                    quorums[idx].addBookie(node);
                }
            }
            chosenNodes.add(node);

            return ((null == parentEnsemble) || parentEnsemble.addBookie(node));
        }

        @Override
        public ArrayList<InetSocketAddress> toList() {
            ArrayList<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>(ensembleSize);
            for (BookieNode bn : chosenNodes) {
                addresses.add(bn.getAddr());
            }
            return addresses;
        }

        /**
         * Validates if an ensemble is valid
         *
         * @return true if the ensemble is valid; false otherwise
         */
        @Override
        public boolean validate() {
            HashSet<InetSocketAddress> addresses = new HashSet<InetSocketAddress>(ensembleSize);
            HashSet<String> racksOrRegions = new HashSet<String>();
            for (BookieNode bn : chosenNodes) {
                if (addresses.contains(bn.getAddr())) {
                    return false;
                }
                addresses.add(bn.getAddr());
                racksOrRegions.add(bn.getNetworkLocation(distanceFromLeaves));
            }

            return ((minRacksOrRegionsForDurability == 0) ||
                    (racksOrRegions.size() >= minRacksOrRegionsForDurability));
        }

        @Override
        public String toString() {
            return chosenNodes.toString();
        }
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<InetSocketAddress> ensemble, List<Integer> writeSet) {
        return writeSet;
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<InetSocketAddress> ensemble, List<Integer> writeSet) {
        List<Integer> retList = new ArrayList<Integer>(reorderReadSequence(ensemble, writeSet));
        if (retList.size() < ensemble.size()) {
            for (int i = 0; i < ensemble.size(); i++) {
                if (!retList.contains(i)) {
                    retList.add(i);
                }
            }
        }
        return retList;
    }
}
