package org.apache.bookkeeper.util;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.StatsLogger;

import static org.apache.bookkeeper.bookie.BookKeeperServerStats.PENDINGS;

public class MonitoredThreadPoolExecutor extends ThreadPoolExecutor {

    public MonitoredThreadPoolExecutor(int numThreads,
                                       String nameFormat,
                                       StatsLogger statsLogger,
                                       String scope) {
        super(numThreads,
            numThreads,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setNameFormat(nameFormat).build());

        // outstanding requests
        statsLogger.scope(scope).registerGauge(PENDINGS, new Gauge<Number>() {
            @Override
            public Number getDefaultValue() {
                return 0;
            }

            @Override
            public Number getSample() {
                try {
                    return getTaskCount() - getCompletedTaskCount();
                } catch (RuntimeException exc) {
                    return 0;
                }
            }
        });
    }
}
