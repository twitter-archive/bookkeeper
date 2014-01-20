package org.apache.bookkeeper.client;

import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Enumeration;

public class TestPiggybackLAC extends BookKeeperClusterTestCase {

    static Logger LOG = LoggerFactory.getLogger(TestPiggybackLAC.class);

    final DigestType digestType;

    public TestPiggybackLAC() {
        super(3);
        this.digestType = DigestType.CRC32;
    }

    @Test(timeout = 60000)
    public void testPiggybackLAC() throws Exception {
        int numEntries = 10;
        LedgerHandle lh = bkc.createLedger(3, 3, 3, digestType, "".getBytes());
        // tried to add entries
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + i).getBytes());
            LOG.info("Added entry {}.", i);
        }
        LedgerHandle readLh = bkc.openLedgerNoRecovery(lh.getId(), digestType, "".getBytes());
        long lastLAC = readLh.getLastAddConfirmed();
        assertEquals(numEntries - 2, lastLAC);
        // write add entries
        for (int i=0; i<numEntries; i++) {
            lh.addEntry(("data" + (i + numEntries)).getBytes());
            LOG.info("Added entry {}.", (i + numEntries));
        }
        int numReads = 0;
        int i = 0;
        while (true) {
            if (i > readLh.getLastAddConfirmed()) {
                break;
            }
            Enumeration<LedgerEntry> data = readLh.readEntries(i, i);
            while (data.hasMoreElements()) {
                LedgerEntry entry = data.nextElement();
                assertEquals("data" + i, new String(entry.getEntry()));
                ++numReads;
            }
            i++;
        }
        assertEquals(2 * numEntries - 1, numReads);
        assertEquals(2 * numEntries - 2, readLh.getLastAddConfirmed());
        readLh.close();
        lh.close();
    }
}
