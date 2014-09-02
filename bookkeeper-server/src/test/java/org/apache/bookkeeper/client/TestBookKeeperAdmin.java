package org.apache.bookkeeper.client;

import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Charsets.UTF_8;

/**
 * This unit test test bookkeeper admin
 */
public class TestBookKeeperAdmin extends BookKeeperClusterTestCase {

    BookKeeper.DigestType digestType;

    public TestBookKeeperAdmin() {
        super(1);
        this.digestType = BookKeeper.DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testOpenLedgerForceRecovery() throws Exception {
        // Create a ledger
        byte[] passwd = "open-ledger-force-recovery".getBytes();
        LedgerHandle lh = bkc.createLedger(1, 1, digestType, passwd);

        final long ledgerId = lh.getId();
        final int numEntries = 30;
        for (int i = 0; i < numEntries; i++) {
            lh.addEntry(("" + i).getBytes(UTF_8));
        }

        lh.close();

        // modify the ledger metadata to simulate a wrong last entry id.
        final CountDownLatch doneLatch = new CountDownLatch(1);
        bkc.ledgerManager.readLedgerMetadata(ledgerId, new BookkeeperInternalCallbacks.GenericCallback<LedgerMetadata>() {
            @Override
            public void operationComplete(int rc, LedgerMetadata metadata) {
                if (BKException.Code.OK != rc) {
                    return;
                }
                // modify the metadata
                metadata.close(0L);
                bkc.ledgerManager.writeLedgerMetadata(ledgerId, metadata,
                        new BookkeeperInternalCallbacks.GenericCallback<Void>() {
                            @Override
                            public void operationComplete(int rc, Void result) {
                                if (BKException.Code.OK != rc) {
                                    return;
                                }
                                doneLatch.countDown();
                            }
                        });
            }
        });

        doneLatch.await();

        LedgerHandle readLh = bkc.openLedger(ledgerId, digestType, passwd);
        assertEquals("Last Entry ID should be zero", 0L, readLh.getLastAddConfirmed());
        readLh.close();

        BookKeeperAdmin bka = new BookKeeperAdmin(bkc);
        LedgerHandle adminLh = bka.openLedger(ledgerId, true);
        assertEquals("Last Entry ID should be fixed to be " + (numEntries - 1), (long) (numEntries - 1), adminLh.getLastAddConfirmed());
        adminLh.close();
    }
}
