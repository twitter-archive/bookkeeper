package org.apache.bookkeeper.client;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
    }

    protected static class TruePredicate implements Predicate {

        public static final TruePredicate instance = new TruePredicate();

        @Override
        public boolean apply(BookieNode candidate, Ensemble chosenNodes) {
            return true;
        }

    }

    protected static class EnsembleForReplacement implements Ensemble {

        public static final EnsembleForReplacement instance = new EnsembleForReplacement();
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

        protected class RackQuorumCoverageSet {
            Set<String> racksOrRegions = new HashSet<String>();
            int seenBookies = 0;

            boolean apply(BookieNode candidate) {
                if (seenBookies + 1 == writeQuorumSize) {
                    return racksOrRegions.size() > (racksOrRegions.contains(candidate.getNetworkLocation(distanceFromLeaves)) ? 1 : 0);
                }
                return true;
            }

            void addBookie(BookieNode candidate) {
                ++seenBookies;
                racksOrRegions.add(candidate.getNetworkLocation());
            }
        }

        final int distanceFromLeaves;
        final int ensembleSize;
        final int writeQuorumSize;
        final ArrayList<BookieNode> chosenNodes;
        private final RackQuorumCoverageSet[] quorums;
        final RRTopologyAwareCoverageEnsemble parentEnsemble;

        protected RRTopologyAwareCoverageEnsemble(RRTopologyAwareCoverageEnsemble that) {
            this.distanceFromLeaves = that.distanceFromLeaves;
            this.ensembleSize = that.ensembleSize;
            this.writeQuorumSize = that.writeQuorumSize;
            this.chosenNodes = (ArrayList<BookieNode>)that.chosenNodes.clone();
            this.quorums = that.quorums.clone();
            this.parentEnsemble = that.parentEnsemble;
        }

        protected RRTopologyAwareCoverageEnsemble(int ensembleSize, int writeQuorumSize, int distanceFromLeaves) {
            this(ensembleSize, writeQuorumSize, distanceFromLeaves, null);
        }

        protected RRTopologyAwareCoverageEnsemble(int ensembleSize, int writeQuorumSize, int distanceFromLeaves, RRTopologyAwareCoverageEnsemble parentEnsemble) {
            this.ensembleSize = ensembleSize;
            this.writeQuorumSize = writeQuorumSize;
            this.distanceFromLeaves = distanceFromLeaves;
            this.chosenNodes = new ArrayList<BookieNode>(ensembleSize);
            this.quorums = new RackQuorumCoverageSet[ensembleSize];
            this.parentEnsemble = parentEnsemble;
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
            int candidatePos = chosenNodes.size();
            int startPos = candidatePos - writeQuorumSize + 1;
            for (int i = startPos; i <= candidatePos; i++) {
                int idx = (i + ensembleSize) % ensembleSize;
                if (null == quorums[idx]) {
                    quorums[idx] = new RackQuorumCoverageSet();
                }
                if (!quorums[idx].apply(candidate)) {
                    return false;
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

            int candidatePos = chosenNodes.size();
            int startPos = candidatePos - writeQuorumSize + 1;
            for (int i = startPos; i <= candidatePos; i++) {
                int idx = (i + ensembleSize) % ensembleSize;
                if (null == quorums[idx]) {
                    quorums[idx] = new RackQuorumCoverageSet();
                }
                quorums[idx].addBookie(node);
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
