package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test reading an entry from replicas in sequence way.
 */
public class TestSequenceRead extends BookKeeperClusterTestCase {

    static final Logger logger = LoggerFactory.getLogger(TestSequenceRead.class);

    final DigestType digestType;
    final byte[] passwd = "sequence-read".getBytes();

    public TestSequenceRead() {
        super(5);
        this.digestType = DigestType.CRC32;
    }

    private LedgerHandle createLedgerWithDuplicatedBookies() throws Exception {
        final LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, passwd);
        // introduce duplicated bookies in an ensemble.
        SortedMap<Long, ArrayList<BookieSocketAddress>> ensembles = lh.getLedgerMetadata().getEnsembles();
        SortedMap<Long, ArrayList<BookieSocketAddress>> newEnsembles = new TreeMap<Long, ArrayList<BookieSocketAddress>>();
        for (Map.Entry<Long, ArrayList<BookieSocketAddress>> entry : ensembles.entrySet()) {
            ArrayList<BookieSocketAddress> newList = new ArrayList<BookieSocketAddress>(entry.getValue().size());
            BookieSocketAddress firstBookie = entry.getValue().get(0);
            for (BookieSocketAddress ignored : entry.getValue()) {
                newList.add(firstBookie);
            }
            newEnsembles.put(entry.getKey(), newList);
        }
        lh.getLedgerMetadata().setEnsembles(newEnsembles);
        // update the ledger metadata with duplicated bookies
        final CountDownLatch latch = new CountDownLatch(1);
        bkc.getLedgerManager().writeLedgerMetadata(lh.getId(), lh.getLedgerMetadata(), new BookkeeperInternalCallbacks.GenericCallback<Void>() {
            @Override
            public void operationComplete(int rc, Void result) {
                if (BKException.Code.OK == rc) {
                    latch.countDown();
                } else {
                    logger.error("Error on writing ledger metadata for ledger {} : ", lh.getId(), BKException.getMessage(rc));
                }
            }
        });
        latch.await();
        logger.info("Update ledger metadata with duplicated bookies for ledger {}.", lh.getId());
        return lh;
    }

    @Test(timeout = 60000)
    public void testSequenceReadOnDuplicatedBookies() throws Exception {
        final LedgerHandle lh = createLedgerWithDuplicatedBookies();

        // should be able to open the ledger even it has duplicated bookies
        final LedgerHandle readLh = bkc.openLedger(lh.getId(), digestType, passwd);
        assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
    }

    @Test(timeout = 60000)
    public void testSequenceReadLacAndEntryOnDuplicatedBookies() throws Exception {
        final LedgerHandle lh = createLedgerWithDuplicatedBookies();

        ClientConfiguration localConf = new ClientConfiguration();
        localConf.addConfiguration(baseClientConf);
        localConf.setFirstSpeculativeReadLACTimeout(100);
        localConf.setMaxSpeculativeReadLACTimeout(100);

        BookKeeper newBkc = new BookKeeper(localConf);
        try {
            LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, passwd);
            assertEquals(LedgerHandle.INVALID_ENTRY_ID, readLh.getLastAddConfirmed());
            final CountDownLatch doneLatch = new CountDownLatch(1);
            final AtomicInteger rcHolder = new AtomicInteger(-1234);
            readLh.asyncReadLastConfirmedAndEntry(0L, 100, false, new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
                @Override
                public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                    rcHolder.set(rc);
                    doneLatch.countDown();
                }
            }, null);
            doneLatch.await();
            assertEquals(BKException.Code.NoSuchLedgerExistsException, rcHolder.get());
            readLh.close();
        } finally {
            newBkc.close();
        }
        lh.close();
    }
}
