package org.apache.bookkeeper.bookie;

/**
 * Listener on ledger storage changes
 */
public interface LedgerStorageListener {

    /**
     * Triggered on when ledger <i>ledgerId</i> is deleted.
     *
     * @param ledgerId ledger id
     */
    void onLedgerDeleted(long ledgerId);
}
