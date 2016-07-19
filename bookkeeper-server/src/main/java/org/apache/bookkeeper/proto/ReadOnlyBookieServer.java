package org.apache.bookkeeper.proto;

import java.security.GeneralSecurityException;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ReadOnlyBookieServer extends BookieServer {

    public ReadOnlyBookieServer(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException, GeneralSecurityException {
        this(conf, NullStatsLogger.INSTANCE);
    }

    public ReadOnlyBookieServer(ServerConfiguration conf, StatsLogger statsLogger)
            throws IOException, KeeperException, InterruptedException, BookieException, GeneralSecurityException {
        super(conf, statsLogger);
    }

    @Override
    protected Bookie newBookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException, GeneralSecurityException {
        return new ReadOnlyBookie(conf, statsLogger);
    }
}
