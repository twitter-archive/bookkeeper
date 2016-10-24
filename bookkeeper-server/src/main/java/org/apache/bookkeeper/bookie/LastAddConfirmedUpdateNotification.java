package org.apache.bookkeeper.bookie;

public class LastAddConfirmedUpdateNotification {
    public long lastAddConfirmed;
    public long timestamp;

    public LastAddConfirmedUpdateNotification(long lastAddConfirmed) {
        this.lastAddConfirmed = lastAddConfirmed;
        this.timestamp = System.currentTimeMillis();
    }
}
