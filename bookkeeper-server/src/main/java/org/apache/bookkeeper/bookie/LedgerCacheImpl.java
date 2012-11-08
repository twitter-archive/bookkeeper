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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;
import org.apache.bookkeeper.meta.ActiveLedgerManager;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of LedgerCache interface.
 * This class serves two purposes.
 */
public class LedgerCacheImpl implements LedgerCache {
    private final static Logger LOG = LoggerFactory.getLogger(LedgerDescriptor.class);
    private final static ConcurrentHashMap<Long, LedgerEntryPage> EMPTY_PAGE_MAP
            = new ConcurrentHashMap<Long, LedgerEntryPage>();
    final File ledgerDirectories[];

    public LedgerCacheImpl(ServerConfiguration conf, ActiveLedgerManager alm) {
        this.ledgerDirectories = Bookie.getCurrentDirectories(conf.getLedgerDirs());
        this.openFileLimit = conf.getOpenFileLimit();
        this.pageSize = conf.getPageSize();
        this.entriesPerPage = pageSize / 8;

        if (conf.getPageLimit() <= 0) {
            // allocate half of the memory to the page cache
            this.pageLimit = (int)((Runtime.getRuntime().maxMemory() / 3) / this.pageSize);
        } else {
            this.pageLimit = conf.getPageLimit();
        }
        LOG.info("maxMemory = " + Runtime.getRuntime().maxMemory());
        LOG.info("openFileLimit is " + openFileLimit + ", pageSize is " + pageSize + ", pageLimit is " + pageLimit);
        activeLedgerManager = alm;
        // Retrieve all of the active ledgers.
        getActiveLedgers();

        // Export sampled stats for index pages, ledgers.
        Stats.export(new SampledStat<Integer>(ServerStatsProvider
                .getStatsLoggerInstance().getStatName(BookkeeperServerSimpleStatType
                .NUM_INDEX_PAGES), 0) {
            @Override
            public Integer doSample() {
                return getNumUsedPages();
            }
        });

        Stats.export(new SampledStat<Integer>(ServerStatsProvider
                .getStatsLoggerInstance().getStatName(BookkeeperServerSimpleStatType
                        .NUM_OPEN_LEDGERS), 0) {
            @Override
            public Integer doSample() {
                synchronized (openLedgers) {
                    return openLedgers.size();
                }
            }
        });
    }
    /**
     * the set of potentially clean ledgers
     */
    ConcurrentLinkedQueue<Long> cleanLedgers = new ConcurrentLinkedQueue<Long>();

    /**
     * the list of potentially dirty ledgers
     */
    ConcurrentLinkedQueue<Long> dirtyLedgers = new ConcurrentLinkedQueue<Long>();

    ConcurrentMap<Long, FileInfo> fileInfoCache = new ConcurrentHashMap<Long, FileInfo>();

    LinkedList<Long> openLedgers = new LinkedList<Long>();

    // Manage all active ledgers in LedgerManager
    // so LedgerManager has knowledge to garbage collect inactive/deleted ledgers
    final ActiveLedgerManager activeLedgerManager;

    final int openFileLimit;
    final int pageSize;
    final int pageLimit;
    final int entriesPerPage;

    /**
     * @return page size used in ledger cache
     */
    public int getPageSize() {
        return pageSize;
    }

    /**
     * @return entries per page used in ledger cache
     */
    public int getEntriesPerPage() {
        return entriesPerPage;
    }

    /**
     * @return page limitation in ledger cache
     */
    public int getPageLimit() {
        return pageLimit;
    }

    // The number of pages that have actually been used
    private AtomicInteger pageCount = new AtomicInteger(0);
    ConcurrentMap<Long, ConcurrentMap<Long,LedgerEntryPage>> pages
            = new ConcurrentHashMap<Long, ConcurrentMap<Long,LedgerEntryPage>>();

    /**
     * @return number of page used in ledger cache
     */
    public int getNumUsedPages() {
        return pageCount.get();
    }

    private LedgerEntryPage putIntoTable(ConcurrentMap<Long, ConcurrentMap<Long,LedgerEntryPage>> table, LedgerEntryPage lep) {
        // Do a get here to avoid too many new ConcurrentHashMaps() as putIntoTable is called fequently.
        ConcurrentMap<Long, LedgerEntryPage> map = table.get(lep.getLedger());
        if (null == map) {
            ConcurrentMap<Long, LedgerEntryPage> mapToPut = new ConcurrentHashMap<Long, LedgerEntryPage>();
            map = table.putIfAbsent(lep.getLedger(), mapToPut);
            if (null == map) {
                map = mapToPut;
            }
        }
        LedgerEntryPage oldPage = map.putIfAbsent(lep.getFirstEntry(), lep);
        if (null == oldPage) {
            oldPage = lep;
        }
        return oldPage;
    }

    private static LedgerEntryPage getFromTable(ConcurrentMap<Long, ConcurrentMap<Long,LedgerEntryPage>> table,
                                                Long ledger, Long firstEntry) {
        ConcurrentMap<Long, LedgerEntryPage> map = table.get(ledger);
        if (null != map) {
            return map.get(firstEntry);
        }
        return null;
    }

    private LedgerEntryPage getLedgerEntryPage(Long ledger, Long firstEntry, boolean onlyDirty) {
        LedgerEntryPage lep = getFromTable(pages, ledger, firstEntry);
        if (onlyDirty && null != lep && lep.isClean()) {
            return null;
        }
        if (null != lep) {
            lep.usePage();
        }
        return lep;
    }

    /**
     * Grab ledger entry page whose first entry is <code>pageEntry</code>.
     *
     * If the page doesn't existed before, we allocate a memory page.
     * Otherwise, we grab a clean page and read it from disk.
     *
     * @param ledger
     *          Ledger Id
     * @param pageEntry
     *          Start entry of this entry page.
     */
    private LedgerEntryPage grabLedgerEntryPage(long ledger, long pageEntry) throws IOException {
        LedgerEntryPage lep = grabCleanPage(ledger, pageEntry);
        try {
            // should update page before we put it into table
            // otherwise we would put an empty page in it
            updatePage(lep);
            LedgerEntryPage oldLep;
            if (lep != (oldLep = putIntoTable(pages, lep))) {
                lep.releasePage();
                // Decrement the page count because we couldn't put this lep in the page cache.
                pageCount.decrementAndGet();
                // Increment the use count of the old lep because this is unexpected
                oldLep.usePage();
                lep = oldLep;
            }
        } catch (IOException ie) {
            // if we grab a clean page, but failed to update the page
            // we are exhausting the count of ledger entry pages.
            // since this page will be never used, so we need to decrement
            // page count of ledger cache.
            lep.releasePage();
            pageCount.decrementAndGet();
            throw ie;
        }
        return lep;
    }

    @Override
    public void putEntryOffset(long ledger, long entry, long offset) throws IOException {
        int offsetInPage = (int) (entry % entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry-offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        if (lep == null) {
            lep = grabLedgerEntryPage(ledger, pageEntry);
        }
        assert lep != null;
        lep.setOffset(offset, offsetInPage*8);
        lep.releasePage();
    }

    @Override
    public long getEntryOffset(long ledger, long entry) throws IOException {
        int offsetInPage = (int) (entry%entriesPerPage);
        // find the id of the first entry of the page that has the entry
        // we are looking for
        long pageEntry = entry-offsetInPage;
        LedgerEntryPage lep = getLedgerEntryPage(ledger, pageEntry, false);
        try {
            if (lep == null) {
                lep = grabLedgerEntryPage(ledger, pageEntry);
            }
            return lep.getOffset(offsetInPage*8);
        } finally {
            if (lep != null) {
                lep.releasePage();
            }
        }
    }

    static final String getLedgerName(long ledgerId) {
        int parent = (int) (ledgerId & 0xff);
        int grandParent = (int) ((ledgerId & 0xff00) >> 8);
        StringBuilder sb = new StringBuilder();
        sb.append(Integer.toHexString(grandParent));
        sb.append('/');
        sb.append(Integer.toHexString(parent));
        sb.append('/');
        sb.append(Long.toHexString(ledgerId));
        sb.append(".idx");
        return sb.toString();
    }

    static final private Random rand = new Random();

    static final private File pickDirs(File dirs[]) {
        return dirs[rand.nextInt(dirs.length)];
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
                    File dir = pickDirs(ledgerDirectories);
                    String ledgerName = getLedgerName(ledger);
                    lf = new File(dir, ledgerName);
                    createdNewFile = true;
                }
            }
            fi = new FileInfo(lf, masterKey);
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
        if (null != fi) {
            fi.use();
        }
        return fi;
    }

    private void updatePage(LedgerEntryPage lep) throws IOException {
        if (!lep.isClean()) {
            throw new IOException("Trying to update a dirty page");
        }
        FileInfo fi = null;
        try {
            fi = getFileInfo(lep.getLedger(), null);
            long pos = lep.getFirstEntry()*8;
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

    @Override
    public void flushLedger(boolean doAll) throws IOException {
        synchronized (dirtyLedgers) {
            if (dirtyLedgers.isEmpty()) {
                dirtyLedgers.addAll(pages.keySet());
            }
        }
        Long potentiallyDirtyLedger = null;
        while (null != (potentiallyDirtyLedger = dirtyLedgers.poll())) {
            flushLedger(potentiallyDirtyLedger);
            if (!doAll) {
                break;
            }
        }
    }

    /**
     * Flush a specified ledger
     *
     * @param ledger
     *          Ledger Id
     * @throws IOException
     */
    private void flushLedger(long ledger) throws IOException {
        LinkedList<Long> firstEntryList;
        ConcurrentMap<Long, LedgerEntryPage> pageMap = pages.get(ledger);
        if (pageMap == null || pageMap.isEmpty()) {
            return;
        }
        firstEntryList = new LinkedList<Long>();
        for(ConcurrentMap.Entry<Long, LedgerEntryPage> entry: pageMap.entrySet()) {
            LedgerEntryPage lep = entry.getValue();
            if (lep.isClean()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Page is clean " + lep);
                }
                continue;
            }
            firstEntryList.add(lep.getFirstEntry());
        }

        if (firstEntryList.size() == 0) {
            LOG.debug("Nothing to flush for ledger {}.", ledger);
            // nothing to do
            return;
        }

        // Now flush all the pages of a ledger
        List<LedgerEntryPage> entries = new ArrayList<LedgerEntryPage>(firstEntryList.size());
        FileInfo fi = null;
        try {
            for(Long firstEntry: firstEntryList) {
                LedgerEntryPage lep = getLedgerEntryPage(ledger, firstEntry, true);
                if (lep != null) {
                    entries.add(lep);
                }
            }
            //TODO(Aniruddha): Move this comparator to a better place.
            Collections.sort(entries, new Comparator<LedgerEntryPage>() {
                    @Override
                    public int compare(LedgerEntryPage o1, LedgerEntryPage o2) {
                    return (int)(o1.getFirstEntry()-o2.getFirstEntry());
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
        } finally {
            for(LedgerEntryPage lep: entries) {
                lep.releasePage();
            }
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

    private LedgerEntryPage grabCleanPage(long ledger, long entry) throws IOException {
        if (entry % entriesPerPage != 0) {
            throw new IllegalArgumentException(entry + " is not a multiple of " + entriesPerPage);
        }

        while(true) {
            boolean canAllocate = false;
            if (pageCount.incrementAndGet() < pageLimit) {
                canAllocate = true;
            } else {
                pageCount.decrementAndGet();
            }
            if (canAllocate) {
                LedgerEntryPage lep = new LedgerEntryPage(pageSize, entriesPerPage);
                lep.setLedger(ledger);
                lep.setFirstEntry(entry);
                lep.usePage();
                return lep;
            }

            // If the clean ledgers list is empty, attempt to flush a ledger
            // and populate it.
            synchronized (cleanLedgers) {
                if (cleanLedgers.isEmpty()) {
                    flushLedger(false);
                    cleanLedgers.addAll(pages.keySet());
                }
            }

            Long potentiallyCleanLedger = null;

            while (null != (potentiallyCleanLedger = cleanLedgers.peek())) {
                ConcurrentMap<Long, LedgerEntryPage> pageMap = pages.get(potentiallyCleanLedger);
                if (null == pageMap) {
                    // This ledger doesn't have any LEPs mapped, so remove it from the list
                    if (!cleanLedgers.remove(potentiallyCleanLedger)) {
                        // Something changed and the head of the queue was already removed. We
                        // should retry.
                        break;
                    } else {
                        continue;
                    }
                }
                // Try to find a clean page from this pageMap and return it.
                for (ConcurrentMap.Entry<Long, LedgerEntryPage> pageEntry : pageMap.entrySet()) {
                    LedgerEntryPage lep = pageEntry.getValue();
                    Long startEntry = pageEntry.getKey();
                    if (lep.inUse() || !lep.isClean()) {
                        // We don't want to reclaim any leps that are in use or that need to be
                        // flushed.
                        continue;
                    }
                    // Remove from map only if nothing has changed since we checked this lep.
                    if (pageMap.remove(startEntry, lep)) {
                        if (!lep.isClean()) {
                            // Someone wrote to this page while we were reclaiming it.
                            pageMap.put(startEntry, lep);
                            continue;
                        }
                        // Do some bookkeeping on the page table
                        pages.remove(potentiallyCleanLedger, EMPTY_PAGE_MAP);
                        // We can now safely reset this lep and return it.
                        lep.usePage();
                        lep.zeroPage();
                        lep.setLedger(ledger);
                        lep.setFirstEntry(entry);
                        return lep;
                    }
                }

                // If we reached here, we weren't able to find a clean lep in this ledger. So, remove it.
                if (!cleanLedgers.remove(potentiallyCleanLedger)) {
                    // Something changed, so we will retry everything.
                    break;
                }
            }
        }
    }

    @Override
    public long getLastEntry(long ledgerId) throws IOException {
        long lastEntry = 0;
        // Find the last entry in the cache
        ConcurrentMap<Long, LedgerEntryPage> map = pages.get(ledgerId);
        if (map != null) {
            for(LedgerEntryPage lep: map.values()) {
                if (lep.getFirstEntry() + entriesPerPage < lastEntry) {
                    continue;
                }
                lep.usePage();
                long highest = lep.getLastEntry();
                if (highest > lastEntry) {
                    lastEntry = highest;
                }
                lep.releasePage();
            }
        }

        FileInfo fi = null;
        try {
            fi = getFileInfo(ledgerId, null);
            long size = fi.size();
            // make sure the file size is aligned with index entry size
            // otherwise we may read incorret data
            if (0 != size % 8) {
                LOG.warn("Index file of ledger {} is not aligned with index entry size.", ledgerId);
                size = size - size % 8;
            }
            // we may not have the last entry in the cache
            if (size > lastEntry*8) {
                ByteBuffer bb = ByteBuffer.allocate(getPageSize());
                long position = size - getPageSize();
                if (position < 0) {
                    position = 0;
                }
                fi.read(bb, position);
                bb.flip();
                long startingEntryId = position/8;
                for(int i = getEntriesPerPage()-1; i >= 0; i--) {
                    if (bb.getLong(i*8) != 0) {
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

    /**
     * This method will look within the ledger directories for the ledger index
     * files. That will comprise the set of active ledgers this particular
     * BookieServer knows about that have not yet been deleted by the BookKeeper
     * Client. This is called only once during initialization.
     */
    private void getActiveLedgers() {
        // Ledger index files are stored in a file hierarchy with a parent and
        // grandParent directory. We'll have to go two levels deep into these
        // directories to find the index files.
        for (File ledgerDirectory : ledgerDirectories) {
            for (File grandParent : ledgerDirectory.listFiles()) {
                if (grandParent.isDirectory()) {
                    for (File parent : grandParent.listFiles()) {
                        if (parent.isDirectory()) {
                            for (File index : parent.listFiles()) {
                                if (!index.isFile() || !index.getName().endsWith(".idx")) {
                                    continue;
                                }
                                // We've found a ledger index file. The file name is the
                                // HexString representation of the ledgerId.
                                String ledgerIdInHex = index.getName().substring(0, index.getName().length() - 4);
                                activeLedgerManager.addActiveLedger(Long.parseLong(ledgerIdInHex, 16), true);
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * This method is called whenever a ledger is deleted by the BookKeeper Client
     * and we want to remove all relevant data for it stored in the LedgerCache.
     */
    @Override
    public void deleteLedger(long ledgerId) throws IOException {
        if (LOG.isDebugEnabled())
            LOG.debug("Deleting ledgerId: " + ledgerId);

        // remove pages first to avoid page flushed when deleting file info
        ConcurrentMap<Long, LedgerEntryPage> lpages = pages.remove(ledgerId);
        if (null != lpages) {
            if (pageCount.addAndGet(-lpages.size()) < 0) {
                throw new RuntimeException("Page count of ledger cache has been decremented to be less than zero.");
            }
        }
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
        cleanLedgers.remove(ledgerId);
        dirtyLedgers.remove(ledgerId);
        synchronized (openLedgers) {
            openLedgers.remove(ledgerId);
        }
    }

    private File findIndexFile(long ledgerId) throws IOException {
        String ledgerName = getLedgerName(ledgerId);
        for(File d: ledgerDirectories) {
            File lf = new File(d, ledgerName);
            if (lf.exists()) {
                return lf;
            }
        }
        return null;
    }

    @Override
    public byte[] readMasterKey(long ledgerId) throws IOException, BookieException {
        FileInfo fi = getFileInfo(ledgerId, null);
        if (null == fi) {
            throw new IOException("Exception while reading master key for ledger:" + ledgerId);
        }
        return fi.getMasterKey();
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

    @Override
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

    @Override
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

    @Override
    public LedgerCacheBean getJMXBean() {
        return new LedgerCacheBean() {
            @Override
            public String getName() {
                return "LedgerCache";
            }

            @Override
            public boolean isHidden() {
                return false;
            }

            @Override
            public int getPageCount() {
                return LedgerCacheImpl.this.getNumUsedPages();
            }

            @Override
            public int getPageSize() {
                return LedgerCacheImpl.this.getPageSize();
            }

            @Override
            public int getOpenFileLimit() {
                return openFileLimit;
            }

            @Override
            public int getPageLimit() {
                return LedgerCacheImpl.this.getPageLimit();
            }

            @Override
            public int getNumCleanLedgers() {
                return cleanLedgers.size();
            }

            @Override
            public int getNumDirtyLedgers() {
                return dirtyLedgers.size();
            }

            @Override
            public int getNumOpenLedgers() {
                return openLedgers.size();
            }
        };
    }

    @Override
    public void close() throws IOException {
        for (Entry<Long, FileInfo> fileInfo : fileInfoCache.entrySet()) {
            FileInfo value = fileInfo.getValue();
            if (value != null) {
                value.close(true);
            }
        }
        fileInfoCache.clear();
    }
}
