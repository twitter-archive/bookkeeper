package org.apache.bookkeeper.client;

import org.apache.bookkeeper.net.DNSToSwitchMapping;
import org.apache.bookkeeper.net.Node;
import org.apache.bookkeeper.stats.AlertStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.jboss.netty.util.HashedWheelTimer;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RackawareEnsemblePlacementPolicy extends RackawareEnsemblePlacementPolicyImpl
        implements ITopologyAwareEnsemblePlacementPolicy<TopologyAwareEnsemblePlacementPolicy.BookieNode> {

    RackawareEnsemblePlacementPolicyImpl slave = null;

    RackawareEnsemblePlacementPolicy() {
        super();
    }

    RackawareEnsemblePlacementPolicy(boolean enforceDurability) {
        super(enforceDurability);
    }

    @Override
    protected RackawareEnsemblePlacementPolicy initialize(DNSToSwitchMapping dnsResolver,
                                                          HashedWheelTimer timer,
                                                          boolean reorderReadsRandom,
                                                          int stabilizePeriodSeconds,
                                                          StatsLogger statsLogger,
                                                          AlertStatsLogger alertStatsLogger) {
        if (stabilizePeriodSeconds > 0) {
            super.initialize(dnsResolver, timer, reorderReadsRandom, 0, statsLogger, alertStatsLogger);
            slave = new RackawareEnsemblePlacementPolicyImpl(enforceDurability);
            slave.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds, statsLogger, alertStatsLogger);
        } else {
            super.initialize(dnsResolver, timer, reorderReadsRandom, stabilizePeriodSeconds, statsLogger, alertStatsLogger);
            slave = null;
        }
        return this;
    }

    @Override
    public void uninitalize() {
        super.uninitalize();
        if (null != slave) {
            slave.uninitalize();
        }
    }

    @Override
    public Set<InetSocketAddress> onClusterChanged(Set<InetSocketAddress> writableBookies, Set<InetSocketAddress> readOnlyBookies) {
        Set<InetSocketAddress> deadBookies = super.onClusterChanged(writableBookies, readOnlyBookies);
        if (null != slave) {
            deadBookies = slave.onClusterChanged(writableBookies, readOnlyBookies);
        }
        return deadBookies;
    }

    @Override
    public ArrayList<InetSocketAddress> newEnsemble(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Set<InetSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, excludeBookies);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize, excludeBookies);
            }
        }
    }

    @Override
    public InetSocketAddress replaceBookie(
            int ensembleSize,
            int writeQuorumSize,
            int ackQuorumSize,
            Collection<InetSocketAddress> currentEnsemble,
            InetSocketAddress bookieToReplace,
            Set<InetSocketAddress> excludeBookies)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,
                    currentEnsemble, bookieToReplace, excludeBookies);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.replaceBookie(ensembleSize, writeQuorumSize, ackQuorumSize,
                        currentEnsemble, bookieToReplace, excludeBookies);
            }
        }
    }

    @Override
    public List<Integer> reorderReadSequence(ArrayList<InetSocketAddress> ensemble,
                                             List<Integer> writeSet,
                                             Map<InetSocketAddress, Long> bookieFailureHistory) {
        return super.reorderReadSequence(ensemble, writeSet, bookieFailureHistory);
    }

    @Override
    public List<Integer> reorderReadLACSequence(ArrayList<InetSocketAddress> ensemble,
                                                List<Integer> writeSet,
                                                Map<InetSocketAddress, Long> bookieFailureHistory) {
        return super.reorderReadLACSequence(ensemble, writeSet, bookieFailureHistory);
    }

    @Override
    public ArrayList<InetSocketAddress> newEnsemble(int ensembleSize,
                                                    int writeQuorumSize,
                                                    int ackQuorumSize,
                                                    Set<InetSocketAddress> excludeBookies,
                                                    Ensemble<BookieNode> parentEnsemble,
                                                    Predicate<BookieNode> parentPredicate)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.newEnsemble(
                    ensembleSize,
                    writeQuorumSize,
                    ackQuorumSize,
                    excludeBookies,
                    parentEnsemble,
                    parentPredicate);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.newEnsemble(ensembleSize, writeQuorumSize, ackQuorumSize,
                        excludeBookies, parentEnsemble, parentPredicate);
            }
        }
    }

    @Override
    public BookieNode selectFromNetworkLocation(
            String networkLoc,
            Set<Node> excludeBookies,
            Predicate<BookieNode> predicate,
            Ensemble<BookieNode> ensemble)
            throws BKException.BKNotEnoughBookiesException {
        try {
            return super.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble);
        } catch (BKException.BKNotEnoughBookiesException bnebe) {
            if (slave == null) {
                throw bnebe;
            } else {
                return slave.selectFromNetworkLocation(networkLoc, excludeBookies, predicate, ensemble);
            }
        }
    }

    @Override
    public void handleBookiesThatLeft(Set<InetSocketAddress> leftBookies) {
        super.handleBookiesThatLeft(leftBookies);
        if (null != slave) {
            slave.handleBookiesThatLeft(leftBookies);
        }
    }

    @Override
    public void handleBookiesThatJoined(Set<InetSocketAddress> joinedBookies) {
        super.handleBookiesThatJoined(joinedBookies);
        if (null != slave) {
            slave.handleBookiesThatJoined(joinedBookies);
        }
    }
}
