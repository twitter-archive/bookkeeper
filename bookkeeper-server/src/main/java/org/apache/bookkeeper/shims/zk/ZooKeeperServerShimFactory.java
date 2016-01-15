package org.apache.bookkeeper.shims.zk;

import java.io.File;
import java.io.IOException;

public class ZooKeeperServerShimFactory {

    public static ZooKeeperServerShim createServer(File snapDir, File logDir, int zkPort, int maxCC)
        throws IOException {
        ZooKeeperServerShim server = new ZooKeeperServerShimImpl();
        server.initialize(snapDir, logDir, zkPort, maxCC);
        return server;
    }

}
