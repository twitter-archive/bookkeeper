package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Charsets.UTF_8;

public class TestReadLastConfirmedAndEntry extends BookKeeperClusterTestCase {

    final BookKeeper.DigestType digestType;

    public TestReadLastConfirmedAndEntry() {
        super(3);
        this.digestType = BookKeeper.DigestType.CRC32;
    }

    static class FakeBookie extends Bookie {

        final long expectedEntryToFail;
        final boolean stallOrRespondNull;

        public FakeBookie(ServerConfiguration conf, long expectedEntryToFail, boolean stallOrRespondNull)
                throws InterruptedException, BookieException, KeeperException, IOException {
            super(conf);
            this.expectedEntryToFail = expectedEntryToFail;
            this.stallOrRespondNull = stallOrRespondNull;
        }

        @Override
        public ByteBuffer readEntry(long ledgerId, long entryId)
                throws IOException, NoLedgerException {
            if (entryId == expectedEntryToFail) {
                if (stallOrRespondNull) {
                    try {
                        Thread.sleep(600000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                } else {
                    throw new NoEntryException(ledgerId, entryId);
                }
            }
            return super.readEntry(ledgerId, entryId);
        }
    }

    @Test(timeout = 60000)
    public void testAdvancedLacWithEmptyResponse() throws Exception {
        byte[] passwd = "advanced-lac-with-empty-response".getBytes(UTF_8);

        ClientConfiguration newConf = new ClientConfiguration();
        newConf.addConfiguration(baseClientConf);
        newConf.setAddEntryTimeout(9999999);
        newConf.setReadEntryTimeout(9999999);
        newConf.setReadTimeout(9999999);

        // stop existing bookies
        stopAllBookies();
        // add fake bookies
        long expectedEntryIdToFail = 2;
        for (int i = 0; i < numBookies; i++) {
            ServerConfiguration conf = newServerConfiguration();
            Bookie b = new FakeBookie(conf, expectedEntryIdToFail, i != 0);
            bs.add(startBookie(conf, b));
            bsConfs.add(conf);
        }

        // create bookkeeper
        BookKeeper newBk = new BookKeeper(newConf);
        // create ledger to write some data
        LedgerHandle lh = newBk.createLedger(3, 3, 2, digestType, passwd);
        for (int i = 0; i <= expectedEntryIdToFail; i++) {
            lh.addEntry("test".getBytes(UTF_8));
        }

        // open ledger to tail reading
        LedgerHandle newLh = newBk.openLedgerNoRecovery(lh.getId(), digestType, passwd);
        long lac = newLh.readLastConfirmed();
        assertEquals(expectedEntryIdToFail - 1, lac);
        Enumeration<LedgerEntry> entries = newLh.readEntries(0, lac);

        int numReads = 0;
        long expectedEntryId = 0L;
        while (entries.hasMoreElements()) {
            LedgerEntry entry = entries.nextElement();
            assertEquals(expectedEntryId++, entry.getEntryId());
            ++numReads;
        }
        assertEquals(lac + 1, numReads);

        final AtomicInteger rcHolder = new AtomicInteger(-12345);
        final AtomicLong lacHolder = new AtomicLong(lac);
        final AtomicReference<LedgerEntry> entryHolder = new AtomicReference<LedgerEntry>(null);
        final CountDownLatch latch = new CountDownLatch(1);

        newLh.asyncReadLastConfirmedAndEntry(99999, new AsyncCallback.ReadLastConfirmedAndEntryCallback() {
            @Override
            public void readLastConfirmedAndEntryComplete(int rc, long lastConfirmed, LedgerEntry entry, Object ctx) {
                rcHolder.set(rc);
                lacHolder.set(lastConfirmed);
                entryHolder.set(entry);
                latch.countDown();
            }
        }, null);

        lh.addEntry("another test".getBytes(UTF_8));

        latch.await();
        assertEquals(expectedEntryIdToFail, lacHolder.get());
        assertNull(entryHolder.get());
        assertEquals(BKException.Code.OK, rcHolder.get());
    }
}
