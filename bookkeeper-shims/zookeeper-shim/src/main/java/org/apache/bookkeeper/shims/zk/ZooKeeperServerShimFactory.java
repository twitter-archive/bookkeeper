package org.apache.bookkeeper.shims.zk;

import org.apache.bookkeeper.shims.ShimLoader;

import java.io.File;
import java.io.IOException;

public class ZooKeeperServerShimFactory {

    private static final ShimLoader<ZooKeeperServerShim> LOADER =
            new ShimLoader<ZooKeeperServerShim>("zk", ZooKeeperServerShim.class);

    public static ZooKeeperServerShim createServer(File snapDir, File logDir, int zkPort, int maxCC)
        throws IOException {
        ZooKeeperServerShim server = LOADER.load();
        server.initialize(snapDir, logDir, zkPort, maxCC);
        return server;
    }

}
