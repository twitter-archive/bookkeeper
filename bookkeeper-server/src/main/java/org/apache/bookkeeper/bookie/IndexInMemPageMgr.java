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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.stats.BookkeeperServerStatsLogger;
import org.apache.bookkeeper.stats.ServerStatsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.common.stats.SampledStat;
import com.twitter.common.stats.Stats;

public class IndexInMemPageMgr {
    private final static Logger LOG = LoggerFactory.getLogger(IndexInMemPageMgr.class);
    private final static ConcurrentHashMap<Long, LedgerEntryPage> EMPTY_PAGE_MAP
        = new ConcurrentHashMap<Long, LedgerEntryPage>();

    final int pageSize;
    final int entriesPerPage;
    final int pageLimit;

    // The number of pages that have actually been used
    private AtomicInteger pageCount = new AtomicInteger(0);
    ConcurrentMap<Long, ConcurrentMap<Long,LedgerEntryPage>> pages
        = new ConcurrentHashMap<Long, ConcurrentMap<Long,LedgerEntryPage>>();

    // The persistence manager that this page manager uses to
    // flush and read pages
    private IndexPersistenceMgr indexPersistenceManager;

    /**
     * the set of potentially clean ledgers
     */
    ConcurrentLinkedQueue<Long> cleanLedgers = new ConcurrentLinkedQueue<Long>();

    /**
     * the list of potentially dirty ledgers
     */
    ConcurrentLinkedQueue<Long> dirtyLedgers = new ConcurrentLinkedQueue<Long>();

    public IndexInMemPageMgr(int pageSize,
                             int entriesPerPage,
                             ServerConfiguration conf,
                             IndexPersistenceMgr indexPersistenceManager) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        this.indexPersistenceManager = indexPersistenceManager;

        if (conf.getPageLimit() <= 0) {
            // allocate half of the memory to the page cache
            this.pageLimit = (int)((Runtime.getRuntime().maxMemory() / 3) / this.pageSize);
        } else {
            this.pageLimit = conf.getPageLimit();
        }

        // Export sampled stats for index pages, ledgers.
        Stats.export(new SampledStat<Integer>(ServerStatsProvider
            .getStatsLoggerInstance().getStatName(BookkeeperServerStatsLogger.BookkeeperServerSimpleStatType
                .NUM_INDEX_PAGES), 0) {
            @Override
            public Integer doSample() {
                return getNumUsedPages();
            }
        });
    }

    /**
     * @return number of page used in ledger cache
     */
    public int getNumUsedPages() {
        return pageCount.get();
    }

    private LedgerEntryPage putIntoTable(ConcurrentMap<Long, ConcurrentMap<Long,LedgerEntryPage>> table, LedgerEntryPage lep) {
        // Do a get here to avoid too many new ConcurrentHashMaps() as putIntoTable is called frequently.
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

    public LedgerEntryPage getLedgerEntryPage(Long ledger, Long firstEntry, boolean onlyDirty) {
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
    public LedgerEntryPage grabLedgerEntryPage(long ledger, long pageEntry) throws IOException {
        LedgerEntryPage lep = grabCleanPage(ledger, pageEntry);
        try {
            // should get the up to date page from the persistence manager
            // before we put it into table otherwise we would put
            // an empty page in it
            indexPersistenceManager.updatePage(lep);
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

    public void removePagesForLedger(long ledgerId) {
        // remove pages first to avoid page flushed when deleting file info
        ConcurrentMap<Long, LedgerEntryPage> lpages = pages.remove(ledgerId);
        if (null != lpages) {
            if (pageCount.addAndGet(-lpages.size()) < 0) {
                throw new RuntimeException("Page count of ledger cache has been decremented to be less than zero.");
            }
        }
        cleanLedgers.remove(ledgerId);
        dirtyLedgers.remove(ledgerId);
    }

    public long getLastEntryInMem(long ledgerId)
    {
        long lastEntry = 0;
        // Find the last entry in the cache
        ConcurrentMap<Long, LedgerEntryPage> map = pages.get(ledgerId);
        if (map != null) {
            for(LedgerEntryPage lep: map.values()) {
                if (lep.getMaxPossibleEntry() < lastEntry) {
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
        return lastEntry;
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
                    flushOneOrMoreLedgers(false);
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

    public int getNumCleanLedgers() {
        return cleanLedgers.size();
    }

    public int getNumDirtyLedgers() {
        return dirtyLedgers.size();
    }

    public void flushOneOrMoreLedgers(boolean doAll) throws IOException {
        synchronized (dirtyLedgers) {
            if (dirtyLedgers.isEmpty()) {
                dirtyLedgers.addAll(pages.keySet());
            }
            indexPersistenceManager.relocateIndexFileIfDirFull(dirtyLedgers);
        }
        Long potentiallyDirtyLedger = null;
        while (null != (potentiallyDirtyLedger = dirtyLedgers.poll())) {
            flushSpecificLedger(potentiallyDirtyLedger);
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
    private void flushSpecificLedger(long ledger) throws IOException {
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
        try {
            for(Long firstEntry: firstEntryList) {
                LedgerEntryPage lep = getLedgerEntryPage(ledger, firstEntry, true);
                if (lep != null) {
                    entries.add(lep);
                }
            }
            indexPersistenceManager.flushLedgerEntries(ledger, entries);
        } finally {
            for(LedgerEntryPage lep: entries) {
                lep.releasePage();
            }
        }
    }

}
