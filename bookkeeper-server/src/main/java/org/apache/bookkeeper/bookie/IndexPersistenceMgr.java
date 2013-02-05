/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.bookkeeper.bookie;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.bookie.LedgerDirsManager.LedgerDirsListener;
import org.apache.bookkeeper.bookie.LedgerDirsManager.NoWritableLedgerDirException;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;

public class IndexPersistenceMgr {
    private final static Logger LOG = LoggerFactory.getLogger(IndexPersistenceMgr.class);
    ConcurrentMap<Long, FileInfo> fileInfoCache = new ConcurrentHashMap<Long, FileInfo>();
    final int openFileLimit;
    final int pageSize;
    final int entriesPerPage;

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final ActiveLedgerManager activeLedgerManager;
    private LedgerDirsManager ledgerDirsManager;
    LinkedList<Long> openLedgers = new LinkedList<Long>();
    final private AtomicBoolean shouldRelocateIndexFile = new AtomicBoolean(false);

    public IndexPersistenceMgr (int pageSize,
                                int entriesPerPage,
                                ServerConfiguration conf,
                                ActiveLedgerManager activeLedgerManager,
                                LedgerDirsManager ledgerDirsManager) throws IOException {
        this.openFileLimit = conf.getOpenFileLimit();
        this.activeLedgerManager = activeLedgerManager;
        this.ledgerDirsManager = ledgerDirsManager;
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        // Retrieve all of the active ledgers.
        getActiveLedgers();
        ledgerDirsManager.addLedgerDirsListener(getLedgerDirsListener());

        LOG.info("openFileLimit is " + openFileLimit);

        Stats.export(new SampledStat<Integer>(ServerStatsProvider
            .getStatsLoggerInstance().getStatName(BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType
                .NUM_OPEN_LEDGERS), 0) {
            @Override
            public Integer doSample() {
                synchronized (openLedgers) {
                    return openLedgers.size();
                }
            }
        });
    }

    FileInfo getFileInfo(Long ledger, byte masterKey[]) throws IOException {
        FileInfo fi = fileInfoCache.get(ledger);
        if (null == fi) {
            boolean createdNewFile = false;
            File lf = null;
            synchronized (this) {
                // Check if the index file exists on disk.
                lf = findIndexFile(ledger);
                if (null == lf) {
                    if (null == masterKey) {
                        throw new Bookie.NoLedgerException(ledger);
                    }
                    // We don't have a ledger index file on disk, so create it.
                    lf = getNewLedgerIndexFile(ledger, null);
                    createdNewFile = true;
                }
            }
            fi = new FileInfo(lf, masterKey);
            File dir = lf.getParentFile().getParentFile().getParentFile();
            if (ledgerDirsManager.isDirFull(dir)) {
                moveLedgerIndexFile(ledger, fi, dir);
            }
            FileInfo oldFi = fileInfoCache.putIfAbsent(ledger, fi);
            if (null != oldFi) {
                // Some other thread won the race. We should delete our file if we created
                // a new one and the paths are different.
                if (createdNewFile && !oldFi.isSameFile(lf)) {
                    fi.delete();
                }
                fi = oldFi;
            } else {
                if (createdNewFile) {
                    // Else, we won and the active ledger manager should know about this.
                    activeLedgerManager.addActiveLedger(ledger, true);
                }
                // Evict cached items from the file info cache if necessary
                evictFileInfoIfNecessary();
                synchronized (openLedgers) {
                    openLedgers.offer(ledger);
                }
            }
        }

        assert null != fi;
        fi.use();
        return fi;
    }

    private File getNewLedgerIndexFile(Long ledger, File dirExcl) throws NoWritableLedgerDirException {
        File dir = ledgerDirsManager.pickRandomWritableDir(dirExcl);
        String ledgerName = LedgerCacheImpl.getLedgerName(ledger);
        return new File(dir, ledgerName);
    }

    /**
     * This method will look within the ledger directories for the ledger index
     * files. That will comprise the set of active ledgers this particular
     * BookieServer knows about that have not yet been deleted by the BookKeeper
     * Client. This is called only once during initialization.
     */
    private void getActiveLedgers() throws IOException {
        // Ledger index files are stored in a file hierarchy with a parent and
        // grandParent directory. We'll have to go two levels deep into these
        // directories to find the index files.
        for (File ledgerDirectory : ledgerDirsManager.getAllLedgerDirs()) {
            for (File grandParent : ledgerDirectory.listFiles()) {
                if (grandParent.isDirectory()) {
                    for (File parent : grandParent.listFiles()) {
                        if (parent.isDirectory()) {
                            for (File index : parent.listFiles()) {
                                if (!index.isFile() ||
                                        (!index.getName().endsWith(LedgerCacheImpl.IDX) &&
                                                !index.getName().endsWith(LedgerCacheImpl.RLOC))) {
                                    continue;
                                }
                                // We've found a ledger index file. The file
                                // name is the HexString representation of the
                                // ledgerId.
                                String ledgerIdInHex = index.getName().replace(LedgerCacheImpl.RLOC, "")
                                        .replace(LedgerCacheImpl.IDX, "");
                                if (index.getName().endsWith(LedgerCacheImpl.RLOC)) {
                                    if (findIndexFile(Long.parseLong(ledgerIdInHex)) != null) {
                                        if (!index.delete()) {
                                            LOG.warn("Deleting the rloc file " + index + " failed");
                                        }
                                        continue;
                                    } else {
                                        File dest = new File(index.getParentFile(), ledgerIdInHex + LedgerCacheImpl.IDX);
                                        if (!index.renameTo(dest)) {
                                            throw new IOException("Renaming rloc file " + index
                                                    + " to index file has failed");
                                        }
                                    }
                                }
                                activeLedgerManager.addActiveLedger(Long.parseLong(ledgerIdInHex, 16), true);
                            }
                        }
                    }
                }
            }
        }
    }

    public void removeLedger(long ledgerId) throws IOException {
        // Delete the ledger's index file and close the FileInfo
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            fi.close(false);
            fi.delete();
        } finally {
            // should release use count
            // otherwise the file channel would not be closed.
            if (null != fi) {
                fi.release();
            }
        }

        // Remove it from the active ledger manager
        activeLedgerManager.removeActiveLedger(ledgerId);
        // Now remove it from all the other lists and maps.
        fileInfoCache.remove(ledgerId);
        synchronized (openLedgers) {
            openLedgers.remove(ledgerId);
        }
    }

    private File findIndexFile(long ledgerId) throws IOException {
        String ledgerName = LedgerCacheImpl.getLedgerName(ledgerId);
        for(File d: ledgerDirsManager.getAllLedgerDirs()) {
            File lf = new File(d, ledgerName);
            if (lf.exists()) {
                return lf;
            }
        }
        return null;
    }

    public boolean ledgerExists(long ledgerId) throws IOException {
        FileInfo fi = fileInfoCache.get(ledgerId);
        if (fi == null) {
            File lf = findIndexFile(ledgerId);
            if (lf == null) {
                return false;
            }
        }
        return true;
    }

    public int getNumOpenLedgers() {
        return openLedgers.size();
    }

    // evict file info if necessary
    private void evictFileInfoIfNecessary() throws IOException {
        if (openLedgers.size() > openFileLimit) {
            Long ledgerToRemove;
            synchronized (openLedgers) {
                ledgerToRemove = openLedgers.poll();
            }
            if (null == ledgerToRemove) {
                // Should not reach here. We probably cleared this while the thread
                // was executing.
                return;
            }
            LOG.info("Ledger {} is evicted from file info cache.",
                ledgerToRemove);
            FileInfo fi = fileInfoCache.remove(ledgerToRemove);
            if (null == fi) {
                // Seems like someone else already closed the file.
                return;
            }
            fi.close(true);
        }
    }

    public void close() throws IOException {
        for (Map.Entry<Long, FileInfo> fileInfo : fileInfoCache.entrySet()) {
            FileInfo value = fileInfo.getValue();
            if (value != null) {
                value.close(true);
            }
        }
        fileInfoCache.clear();
    }

    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            if (null == fi) {
                throw new IOException("Exception while reading master key for ledger:" + ledgerId);
            }
            return fi.getMasterKey();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    public void setMasterKey(long ledgerId, byte[] masterKey) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, masterKey);
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean setFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            if (null != fi) {
                return fi.setFenced();
            }
            return false;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    boolean isFenced(long ledgerId) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            if (null != fi) {
                return fi.isFenced();
            }
            return false;
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    public int getOpenFileLimit() {
        return openFileLimit;
    }

    private LedgerDirsListener getLedgerDirsListener() {
        return new LedgerDirsListener() {
            @Override
            public void diskFull(File disk) {
                // If the current entry log disk is full, then create new entry
                // log.
                shouldRelocateIndexFile.set(true);
            }

            @Override
            public void diskFailed(File disk) {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void allDisksFull() {
                // Nothing to handle here. Will be handled in Bookie
            }

            @Override
            public void fatalError() {
                // Nothing to handle here. Will be handled in Bookie
            }
        };
    }

    public void relocateIndexFileIfDirFull(Collection<Long> dirtyLedgers) throws IOException {
       if (shouldRelocateIndexFile.get()) {
            // if some new dir detected as full, then move all corresponding
            // open index files to new location
            for (Long l : dirtyLedgers) {
                FileInfo fi = null;
                try {
                    fi = getFileInfo(l, null);
                    File currentDir = fi.getLf().getParentFile().getParentFile().getParentFile();
                    if (ledgerDirsManager.isDirFull(currentDir)) {
                        moveLedgerIndexFile(l, fi, currentDir);
                    }
                } finally {
                    if (null != fi) {
                        fi.release();
                    }
                }
            }
            shouldRelocateIndexFile.set(false);
        }
    }

    private void moveLedgerIndexFile(Long l, FileInfo fi, File dirExcl) throws NoWritableLedgerDirException, IOException {
        File newLedgerIndexFile = getNewLedgerIndexFile(l, dirExcl);
        fi.moveToNewLocation(newLedgerIndexFile, fi.getSizeSinceLastwrite());
    }

    /**
     * flush ledger index header, if necessary
     */
    void flushLedgerHeader(long ledger) throws IOException {
        FileInfo fi = null;
        try {
            fi = getFileInfo(ledger, null);
            fi.flushHeader();
        } finally {
            if (null != fi) {
                fi.release();
            }
        }
    }

    public void flushLedgerEntries(Long ledger,
                                   List<LedgerEntryPage> entries) throws IOException {
        FileInfo fi = null;
        try {
            //TODO(Aniruddha): Move this comparator to a better place.
            Collections.sort(entries, new Comparator<LedgerEntryPage>() {
                @Override
                public int compare(LedgerEntryPage o1, LedgerEntryPage o2) {
                    return (int) (o1.getFirstEntry() - o2.getFirstEntry());
                }
            });

            //ArrayList<Integer> versions = new ArrayList<Integer>(entries.size());
            int[] versions = new int[entries.size()];
            fi = getFileInfo(ledger, null);
            int start = 0;
            long lastOffset = -1;
            for(int i = 0; i < entries.size(); i++) {
                versions[i] = entries.get(i).getVersion();
                if (lastOffset != -1 && (entries.get(i).getFirstEntry() - lastOffset) != entriesPerPage) {
                    // send up a sequential list
                    int count = i - start;
                    if (count == 0) {
                        LOG.warn("Count cannot possibly be zero!");
                    }
                    writeBuffers(ledger, entries, fi, start, count);
                    start = i;
                }
                lastOffset = entries.get(i).getFirstEntry();
            }
            if (entries.size()-start == 0 && entries.size() != 0) {
                LOG.warn("Nothing to write, but there were entries!");
            }
            writeBuffers(ledger, entries, fi, start, entries.size()-start);
            for(int i = 0; i < entries.size(); i++) {
                LedgerEntryPage lep = entries.get(i);
                lep.setClean(versions[i]);
            }
        }
        finally {
            if (fi != null) {
                fi.release();
            }
        }
    }

    private void writeBuffers(Long ledger,
                              List<LedgerEntryPage> entries, FileInfo fi,
                              int start, int count) throws IOException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Writing " + count + " buffers of " + Long.toHexString(ledger));
        }
        if (count == 0) {
            return;
        }
        ByteBuffer buffs[] = new ByteBuffer[count];
        for(int j = 0; j < count; j++) {
            buffs[j] = entries.get(start+j).getPageToWrite();
            if (entries.get(start+j).getLedger() != ledger) {
                throw new IOException("Writing to " + ledger + " but page belongs to "
                    + entries.get(start+j).getLedger());
            }
        }
        long totalWritten = 0;
        while(buffs[buffs.length-1].remaining() > 0) {
            long rc = fi.write(buffs, entries.get(start+0).getFirstEntry()*8);
            if (rc <= 0) {
                throw new IOException("Short write to ledger " + ledger + " rc = " + rc);
            }
            totalWritten += rc;
        }
        if (totalWritten != (long)count * (long)pageSize) {
            throw new IOException("Short write to ledger " + ledger + " wrote " + totalWritten
                + " expected " + count * pageSize);
        }
    }

    public void updatePage(LedgerEntryPage lep) throws IOException {
        if (!lep.isClean()) {
            throw new IOException("Trying to update a dirty page");
        }
        FileInfo fi = null;
        try {
            fi = getFileInfo(lep.getLedger(), null);
            long pos = lep.getFirstEntryPosition();
            if (pos >= fi.size()) {
                lep.zeroPage();
            } else {
                lep.readPage(fi);
            }
        } finally {
            if (fi != null) {
                fi.release();
            }
        }
    }

    public long getPersistEntryBeyondInMem (long ledgerId, long lastEntryInMem) throws IOException {
        FileInfo fi = null;
        long lastEntry = lastEntryInMem;
        try {
            fi = getFileInfo(ledgerId, null);
            long size = fi.size();
            // make sure the file size is aligned with index entry size
            // otherwise we may read incorret data
            if (0 != size % LedgerEntryPage.getIndexEntrySize()) {
                LOG.warn("Index file of ledger {} is not aligned with index entry size.", ledgerId);
                size = size - size % LedgerEntryPage.getIndexEntrySize();
            }
            // we may not have the last entry in the cache
            if (size > lastEntryInMem*LedgerEntryPage.getIndexEntrySize()) {
                ByteBuffer bb = ByteBuffer.allocate(pageSize);
                long position = size - pageSize;
                if (position < 0) {
                    position = 0;
                }
                fi.read(bb, position);
                bb.flip();
                long startingEntryId = position/LedgerEntryPage.getIndexEntrySize();
                for(int i = entriesPerPage-1; i >= 0; i--) {
                    if (bb.getLong(i*LedgerEntryPage.getIndexEntrySize()) != 0) {
                        if (lastEntry < startingEntryId+i) {
                            lastEntry = startingEntryId+i;
                        }
                        break;
                    }
                }
            }
        } finally {
            if (fi != null) {
                fi.release();
            }
        }
        return lastEntry;
    }
}
