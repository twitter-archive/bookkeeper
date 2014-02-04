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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.bookkeeper.client.BKException.BKNotEnoughBookiesException;
import org.apache.bookkeeper.net.NetworkTopology;
import org.apache.bookkeeper.util.StaticDNSResolver;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;
import static org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy.REPP_DNS_RESOLVER_CLASS;

public class TestRegionAwareEnsemblePlacementPolicy extends TestCase {

    static final Logger LOG = LoggerFactory.getLogger(TestRegionAwareEnsemblePlacementPolicy.class);

    RegionAwareEnsemblePlacementPolicy repp;
    Configuration conf = new CompositeConfiguration();

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        StaticDNSResolver.reset();
        StaticDNSResolver.addNodeToRack(InetAddress.getLocalHost().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack("127.0.0.1", NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack("localhost", NetworkTopology.DEFAULT_RACK);
        LOG.info("Set up static DNS Resolver.");
        conf.setProperty(REPP_DNS_RESOLVER_CLASS, StaticDNSResolver.class.getName());
        repp = new RegionAwareEnsemblePlacementPolicy();
        repp.initialize(conf);
    }

    @Override
    protected void tearDown() throws Exception {
        repp.uninitalize();
        super.tearDown();
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInSameRegion() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getAddress().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getAddress().getHostAddress(), "/region1/r1");
        StaticDNSResolver.addNodeToRack(addr3.getAddress().getHostAddress(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr4.getAddress().getHostAddress(), "/default-region/r3");
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        // replace node under r2
        InetSocketAddress replacedBookie = repp.replaceBookie(addr2, new HashSet<InetSocketAddress>());
        assertEquals(addr3, replacedBookie);
    }

    @Test
    public void testReplaceBookieWithEnoughBookiesInDifferentRegion() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getAddress().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getAddress().getHostAddress(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getAddress().getHostAddress(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getAddress().getHostAddress(), "/region3/r4");
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        // replace node under r2
        Set<InetSocketAddress> excludedAddrs = new HashSet<InetSocketAddress>();
        excludedAddrs.add(addr1);
        InetSocketAddress replacedBookie = repp.replaceBookie(addr2, excludedAddrs);

        assertFalse(addr1.equals(replacedBookie));
        assertTrue(addr3.equals(replacedBookie) || addr4.equals(replacedBookie));
    }

    @Test
    public void testReplaceBookieWithNotEnoughBookies() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getAddress().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getAddress().getHostAddress(), "/region2/r2");
        StaticDNSResolver.addNodeToRack(addr3.getAddress().getHostAddress(), "/region3/r3");
        StaticDNSResolver.addNodeToRack(addr4.getAddress().getHostAddress(), "/region4/r4");
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        // replace node under r2
        Set<InetSocketAddress> excludedAddrs = new HashSet<InetSocketAddress>();
        excludedAddrs.add(addr1);
        excludedAddrs.add(addr3);
        excludedAddrs.add(addr4);
        try {
            repp.replaceBookie(addr2, excludedAddrs);
            fail("Should throw BKNotEnoughBookiesException when there is not enough bookies");
        } catch (BKNotEnoughBookiesException bnebe) {
            // should throw not enou
        }
    }

    @Test
    public void testNewEnsembleWithSingleRegion() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        try {
            ArrayList<InetSocketAddress> ensemble = repp.newEnsemble(3, 2, new HashSet<InetSocketAddress>());
            assertEquals(0, getNumCoveredRegionsInWriteQuorum(ensemble, 2));
            ArrayList<InetSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<InetSocketAddress>());
            assertEquals(0, getNumCoveredRegionsInWriteQuorum(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithMultipleRegions() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getAddress().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getAddress().getHostAddress(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getAddress().getHostAddress(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr4.getAddress().getHostAddress(), "/region1/r2");
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        try {
            ArrayList<InetSocketAddress> ensemble = repp.newEnsemble(3, 2, new HashSet<InetSocketAddress>());
            int numCovered = getNumCoveredRegionsInWriteQuorum(ensemble, 2);
            assertTrue(numCovered >= 1);
            assertTrue(numCovered < 3);
            ArrayList<InetSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<InetSocketAddress>());
            numCovered = getNumCoveredRegionsInWriteQuorum(ensemble2, 2);
            assertTrue(numCovered >= 1 && numCovered < 3);
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    @Test
    public void testNewEnsembleWithEnoughRegions() throws Exception {
        InetSocketAddress addr1 = new InetSocketAddress("127.0.0.1", 3181);
        InetSocketAddress addr2 = new InetSocketAddress("127.0.0.2", 3181);
        InetSocketAddress addr3 = new InetSocketAddress("127.0.0.3", 3181);
        InetSocketAddress addr4 = new InetSocketAddress("127.0.0.4", 3181);
        InetSocketAddress addr5 = new InetSocketAddress("127.0.0.5", 3181);
        InetSocketAddress addr6 = new InetSocketAddress("127.0.0.6", 3181);
        InetSocketAddress addr7 = new InetSocketAddress("127.0.0.7", 3181);
        InetSocketAddress addr8 = new InetSocketAddress("127.0.0.8", 3181);
        // update dns mapping
        StaticDNSResolver.addNodeToRack(addr1.getAddress().getHostAddress(), NetworkTopology.DEFAULT_RACK);
        StaticDNSResolver.addNodeToRack(addr2.getAddress().getHostAddress(), "/region1/r2");
        StaticDNSResolver.addNodeToRack(addr3.getAddress().getHostAddress(), "/region2/r3");
        StaticDNSResolver.addNodeToRack(addr4.getAddress().getHostAddress(), "/region3/r4");
        StaticDNSResolver.addNodeToRack(addr5.getAddress().getHostAddress(), "/default-region/default-rack2");
        StaticDNSResolver.addNodeToRack(addr6.getAddress().getHostAddress(), "/region1/r12");
        StaticDNSResolver.addNodeToRack(addr7.getAddress().getHostAddress(), "/region2/r13");
        StaticDNSResolver.addNodeToRack(addr8.getAddress().getHostAddress(), "/region3/r14");
        // Update cluster
        Set<InetSocketAddress> addrs = new HashSet<InetSocketAddress>();
        addrs.add(addr1);
        addrs.add(addr2);
        addrs.add(addr3);
        addrs.add(addr4);
        addrs.add(addr5);
        addrs.add(addr6);
        addrs.add(addr7);
        addrs.add(addr8);
        repp.onClusterChanged(addrs, new HashSet<InetSocketAddress>());
        try {
            ArrayList<InetSocketAddress> ensemble1 = repp.newEnsemble(3, 2, new HashSet<InetSocketAddress>());
            assertEquals(3, getNumCoveredRegionsInWriteQuorum(ensemble1, 2));
            ArrayList<InetSocketAddress> ensemble2 = repp.newEnsemble(4, 2, new HashSet<InetSocketAddress>());
            assertEquals(4, getNumCoveredRegionsInWriteQuorum(ensemble2, 2));
        } catch (BKNotEnoughBookiesException bnebe) {
            fail("Should not get not enough bookies exception even there is only one rack.");
        }
    }

    private int getNumCoveredRegionsInWriteQuorum(ArrayList<InetSocketAddress> ensemble, int writeQuorumSize)
            throws Exception {
        int ensembleSize = ensemble.size();
        int numCoveredWriteQuorums = 0;
        for (int i = 0; i < ensembleSize; i++) {
            Set<String> regions = new HashSet<String>();
            for (int j = 0; j < writeQuorumSize; j++) {
                int bookieIdx = (i + j) % ensembleSize;
                InetSocketAddress addr = ensemble.get(bookieIdx);
                regions.add(StaticDNSResolver.getRegion(addr.getAddress().getHostAddress()));
            }
            numCoveredWriteQuorums += (regions.size() > 1 ? 1 : 0);
        }
        return numCoveredWriteQuorums;
    }


}
