package org.apache.bookkeeper.shims.zk;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

class ZooKeeperServerShimImpl implements ZooKeeperServerShim {

    ZooKeeperServer zks = null;
    NIOServerCnxnFactory serverFactory = null;

    @Override
    public void initialize(File snapDir, File logDir, int zkPort, int maxCC) throws IOException {
        zks = new ZooKeeperServer(snapDir, logDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        serverFactory = new NIOServerCnxnFactory();
        serverFactory.configure(new InetSocketAddress(zkPort), maxCC);
    }

    @Override
    public void start() throws IOException {
        if (null == zks || null == serverFactory) {
            throw new IOException("Start zookeeper server before initialization.");
        }
        try {
            serverFactory.startup(zks);
        } catch (InterruptedException e) {
            throw new IOException("Interrupted when starting zookeeper server : ", e);
        }
    }

    @Override
    public void stop() {
        if (null != serverFactory) {
            serverFactory.shutdown();
        }
        if (null != zks) {
            zks.shutdown();
        }
    }
}
