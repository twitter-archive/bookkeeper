package org.apache.bookkeeper.bookie;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ReadOnlyBookie extends Bookie {

    public ReadOnlyBookie(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super(conf, statsLogger);
        this.readOnly.set(true);
        LOG.info("Running bookie in readonly mode.");
    }

    @Override
    public void doTransitionToWritableMode() {
        // no-op
        LOG.info("Skip transition to writable mode for readonly bookie");
    }

    @Override
    public void doTransitionToReadOnlyMode() {
        // no-op
        LOG.info("Skip transition to readonly mode for readonly bookie");
    }
}
