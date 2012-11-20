package org.apache.bookkeeper.bookie;

public interface LEPStateChangeCallback {
    public void onSetInUse(LedgerEntryPage lep);
    public void onResetInUse(LedgerEntryPage lep);
    public void onSetClean(LedgerEntryPage lep);
    public void onSetDirty(LedgerEntryPage lep);
}
