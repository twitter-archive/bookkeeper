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
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.proto.BookieProtocol;

/**
 * This is a page in the LedgerCache. It holds the locations
 * (entrylogfile, offset) for entry ids.
 */
public class LedgerEntryPage {
    private final static int indexEntrySize = 8;
    private final int pageSize;
    private final int entriesPerPage;
    volatile private long ledger = -1;
    volatile private long firstEntry = BookieProtocol.INVALID_ENTRY_ID;
    private final ByteBuffer page;
    volatile private boolean clean = true;
    volatile private boolean pinned = false;
    private final AtomicInteger useCount = new AtomicInteger();
    volatile private int version;
    private final LEPStateChangeCallback callback;

    public static int getIndexEntrySize() {
        return indexEntrySize;
    }

    public LedgerEntryPage(int pageSize, int entriesPerPage, LEPStateChangeCallback callback) {
        this.pageSize = pageSize;
        this.entriesPerPage = entriesPerPage;
        page = ByteBuffer.allocateDirect(pageSize);
        this.callback = callback;
        if (null != this.callback) {
            callback.onResetInUse(this);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getLedger());
        sb.append('@');
        sb.append(getFirstEntry());
        sb.append(clean ? " clean " : " dirty ");
        sb.append(useCount.get());
        return sb.toString();
    }
    public void usePage() {
        int oldVal = useCount.getAndIncrement();
        if ((0 == oldVal) && (null != callback)) {
            callback.onSetInUse(this);
        }
    }
    synchronized public void pin() {
        pinned = true;
    }
    synchronized public void unpin() {
        pinned = false;
    }
    synchronized public boolean isPinned() {
        return pinned;
    }
    public void releasePage() {
        int newUseCount = useCount.decrementAndGet();
        if (newUseCount < 0) {
            throw new IllegalStateException("Use count has gone below 0");
        }
        if ((null != callback) && (newUseCount == 0)) {
            callback.onResetInUse(this);
        }
    }
    private void checkPage() {
        if (useCount.get() <= 0) {
            throw new IllegalStateException("Page not marked in use");
        }
    }
    @Override
    public boolean equals(Object other) {
        if (other instanceof LedgerEntryPage) {
            LedgerEntryPage otherLEP = (LedgerEntryPage) other;
            return otherLEP.getLedger() == getLedger() && otherLEP.getFirstEntry() == getFirstEntry();
        } else {
            return false;
        }
    }
    @Override
    public int hashCode() {
        return (int)getLedger() ^ (int)(getFirstEntry());
    }
    void setClean(int versionOfCleaning) {
        this.clean = (versionOfCleaning == version);

        if ((null != callback) && clean) {
            callback.onSetClean(this);
        }
    }

    boolean isClean() {
        return clean;
    }

    public void setOffset(long offset, int position) {
        checkPage();
        page.putLong(position, offset);
        version++;
        this.clean = false;

        if (null != callback) {
            callback.onSetDirty(this);
        }
    }

    public long getOffset(int position) {
        checkPage();
        return page.getLong(position);
    }
    static final byte zeroPage[] = new byte[64*1024];
    public void zeroPage() {
        checkPage();
        page.clear();
        page.put(zeroPage, 0, page.remaining());
        clean = true;
    }
    public void readPage(FileInfo fi) throws IOException {
        checkPage();
        page.clear();
        while(page.remaining() != 0) {
            if (fi.read(page, getFirstEntryPosition()) <= 0) {
                throw new IOException("Short page read of ledger " + getLedger() + " tried to get " + page.capacity() + " from position " + getFirstEntryPosition() + " still need " + page.remaining());
            }
        }
        clean = true;
    }
    public ByteBuffer getPageToWrite() {
        checkPage();
        page.clear();
        return page;
    }
    long getLedger() {
        return ledger;
    }
    int getVersion() {
        return version;
    }

    void setLedgerAndFirstEntry(long ledgerId, long firstEntry) {
        if (firstEntry % entriesPerPage != 0) {
            throw new IllegalArgumentException(firstEntry + " is not a multiple of " + entriesPerPage);
        }
        this.firstEntry = firstEntry;
        this.ledger = ledgerId;
    }

    long getFirstEntry() {
        return firstEntry;
    }
    long getMaxPossibleEntry() {
        return firstEntry+entriesPerPage;
    }
    long getFirstEntryPosition() {
        return firstEntry*indexEntrySize;
    }
    public boolean inUse() {
        return useCount.get() > 0;
    }
    public long getLastEntry() {
        for(int i = entriesPerPage - 1; i >= 0; i--) {
            if (getOffset(i*indexEntrySize) > 0) {
                return i + firstEntry;
            }
        }
        return 0;
    }
}
