package org.apache.bookkeeper.net;

import java.util.Set;

/**
 * Network Topology Interface
 */
public interface NetworkTopology {

    public final static String DEFAULT_REGION = "/default-region";
    public final static String DEFAULT_RACK = "/default-region/default-rack";

    /**
     * Add a node to the network topology
     *
     * @param node
     *          add the node to network topology
     */
    void add(Node node);

    /**
     * Remove a node from nework topology
     *
     * @param node
     *          remove the node from network topology
     */
    void remove(Node node);

    /**
     * Check if the tree contains node <i>node</i>.
     *
     * @param node
     *          node to check
     * @return true if <i>node</i> is already in the network topology, otherwise false.
     */
    boolean contains(Node node);

    /**
     * Retrieve a node from the network topology
     * @param loc
     * @return
     */
    Node getNode(String loc);

    /**
     * Returns number of racks in the network topology.
     *
     * @return number of racks in the network topology.
     */
    int getNumOfRacks();

    /**
     * Returns the nodes under a location.
     *
     * @param loc
     *      network location
     * @return nodes under a location
     */
    Set<Node> getLeaves(String loc);

}
