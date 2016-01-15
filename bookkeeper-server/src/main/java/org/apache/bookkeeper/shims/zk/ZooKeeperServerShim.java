package org.apache.bookkeeper.shims.zk;

import java.io.File;
import java.io.IOException;

/**
 * In order to be compatible with multiple versions of ZooKeeper.
 * All parts of the ZooKeeper Server that are not cross-version
 * compatible are encapsulated in an implementation of this class.
 */
public interface ZooKeeperServerShim {

    /**
     * Initialize zookeeper server.
     *
     * @param snapDir
     *          Snapshot Dir.
     * @param logDir
     *          Log Dir.
     * @param zkPort
     *          ZooKeeper Port.
     * @param maxCC
     *          Max Concurrency for Client.
     * @throws IOException when failed to initialize zookeeper server.
     */
    void initialize(File snapDir, File logDir, int zkPort, int maxCC) throws IOException;

    /**
     * Start the zookeeper server.
     *
     * @throws IOException when failed to start zookeeper server.
     */
    void start() throws IOException;

    /**
     * Stop the zookeeper server.
     */
    void stop();

}
