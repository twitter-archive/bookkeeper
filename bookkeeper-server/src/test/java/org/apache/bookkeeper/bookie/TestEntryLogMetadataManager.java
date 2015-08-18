package org.apache.bookkeeper.bookie;

import com.google.common.collect.Sets;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Test Case for {@link org.apache.bookkeeper.bookie.EntryLogMetadataManager}
 */
public class TestEntryLogMetadataManager {

    @Test(timeout = 60000)
    public void testAddEntryLogMetadata() {
        EntryLogMetadataManager manager = new EntryLogMetadataManager(NullStatsLogger.INSTANCE);
        manager.addEntryLogMetadata(new EntryLogMetadataManager.EntryLogMetadata(1L));
        assertTrue(manager.containsEntryLog(1L));
    }

    @Test(timeout = 60000)
    public void testRemoveEntryLogMetadata() {
        EntryLogMetadataManager manager = new EntryLogMetadataManager(NullStatsLogger.INSTANCE);
        manager.addEntryLogMetadata(new EntryLogMetadataManager.EntryLogMetadata(2L));
        assertTrue(manager.containsEntryLog(2L));
        manager.removeEntryLogMetadata(2L);
        assertFalse(manager.containsEntryLog(2L));
    }

    @Test(timeout = 60000)
    public void testGetEntryLogs() {
        EntryLogMetadataManager manager = new EntryLogMetadataManager(NullStatsLogger.INSTANCE);
        int numLogs = 5;
        Set<Long> logs = new HashSet<Long>();
        for (int i = 0; i < numLogs; i++) {
            manager.addEntryLogMetadata(new EntryLogMetadataManager.EntryLogMetadata(i));
            logs.add((long)i);
        }
        assertTrue(Sets.difference(logs, manager.getEntryLogs()).isEmpty());
    }
}
