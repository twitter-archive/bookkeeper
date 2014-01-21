package org.apache.bookkeeper.meta;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerMetadata;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.GenericCallback;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.LedgerMetadataListener;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.Processor;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks.TimedGenericCallback;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.AsyncCallback.VoidCallback;

import java.io.IOException;

public class TimedLedgerManager implements LedgerManager {

    public static TimedLedgerManager of(LedgerManager underlying, StatsLogger statsLogger) {
        return new TimedLedgerManager(underlying, statsLogger);
    }

    private final LedgerManager underlying;
    private final OpStatsLogger createStats;
    private final OpStatsLogger deleteStats;
    private final OpStatsLogger readStats;
    private final OpStatsLogger writeStats;

    private TimedLedgerManager(LedgerManager underlying, StatsLogger statsLogger) {
        this.underlying = underlying;
        // stats
        StatsLogger scopedStatsLogger = statsLogger.scope("lm");
        createStats = scopedStatsLogger.getOpStatsLogger("create_ledger");
        deleteStats = scopedStatsLogger.getOpStatsLogger("delete_ledger");
        readStats = scopedStatsLogger.getOpStatsLogger("read_metadata");
        writeStats = scopedStatsLogger.getOpStatsLogger("write_metadata");
    }

    public LedgerManager getUnderlying() {
        return underlying;
    }

    @Override
    public void createLedger(LedgerMetadata metadata, GenericCallback<Long> cb) {
        underlying.createLedger(metadata, new TimedGenericCallback<Long>(cb, BKException.Code.OK, createStats));
    }

    @Override
    public void deleteLedger(long ledgerId, GenericCallback<Void> cb) {
        underlying.deleteLedger(ledgerId, new TimedGenericCallback<Void>(cb, BKException.Code.OK, deleteStats));
    }

    @Override
    public void readLedgerMetadata(long ledgerId, GenericCallback<LedgerMetadata> readCb) {
        underlying.readLedgerMetadata(ledgerId,
                new TimedGenericCallback<LedgerMetadata>(readCb, BKException.Code.OK, readStats));
    }

    @Override
    public void writeLedgerMetadata(long ledgerId, LedgerMetadata metadata, GenericCallback<Void> cb) {
        underlying.writeLedgerMetadata(ledgerId, metadata,
                new TimedGenericCallback<Void>(cb, BKException.Code.OK, writeStats));
    }

    @Override
    public void registerLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        underlying.registerLedgerMetadataListener(ledgerId, listener);
    }

    @Override
    public void unregisterLedgerMetadataListener(long ledgerId, LedgerMetadataListener listener) {
        underlying.unregisterLedgerMetadataListener(ledgerId, listener);
    }

    @Override
    public void asyncProcessLedgers(Processor<Long> processor, VoidCallback finalCb, Object context,
                                    int successRc, int failureRc) {
        underlying.asyncProcessLedgers(processor, finalCb, context, successRc, failureRc);
    }

    @Override
    public void close() throws IOException {
        underlying.close();
    }
}
