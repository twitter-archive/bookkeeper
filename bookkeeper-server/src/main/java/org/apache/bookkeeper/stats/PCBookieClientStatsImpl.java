package org.apache.bookkeeper.stats;

public class PCBookieClientStatsImpl extends BaseStatsImpl implements PCBookieClientStatsLogger {
    public PCBookieClientStatsImpl(String name) {
        super(name, PCBookieClientOp.values(), PCBookieSimpleStatType.values());
    }
}
