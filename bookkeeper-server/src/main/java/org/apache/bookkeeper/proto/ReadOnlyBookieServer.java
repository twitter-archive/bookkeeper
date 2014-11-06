package org.apache.bookkeeper.proto;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.ReadOnlyBookie;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public class ReadOnlyBookieServer extends BookieServer {

    public ReadOnlyBookieServer(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        super(conf);
    }

    @Override
    protected Bookie newBookie(ServerConfiguration conf)
            throws IOException, KeeperException, InterruptedException, BookieException {
        return new ReadOnlyBookie(conf);
    }
}
