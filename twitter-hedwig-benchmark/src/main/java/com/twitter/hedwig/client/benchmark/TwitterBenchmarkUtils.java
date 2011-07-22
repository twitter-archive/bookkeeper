package com.twitter.hedwig.client.benchmark;

public  class TwitterBenchmarkUtils {
    private TwitterBenchmarkUtils() {}

    /**
     * cause this thread to sleep no less than the number of milliseconds specified by the caller,
     * and the caller doesn't need to worry about this thread being interrupted while sleeping
     *
     * @param timeInMillSecs
     */
    public static void sleep(long timeInMillSecs) {
        long t1 = System.currentTimeMillis();
        long nextSleepTimeInMilliSecs = timeInMillSecs;
        while(true) {
            try {
                Thread.sleep(nextSleepTimeInMilliSecs);
                break;
            } catch(InterruptedException e) {
                nextSleepTimeInMilliSecs = System.currentTimeMillis() - t1;
                if (nextSleepTimeInMilliSecs > 0) {
                    continue;
                }
            }
        }
    }
}
