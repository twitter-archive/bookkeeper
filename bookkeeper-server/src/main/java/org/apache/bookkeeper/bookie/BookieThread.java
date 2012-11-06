package org.apache.bookkeeper.bookie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* Wrapper that wraps bookie threads
* Any common handing that we require for all bookie threads
* should be implemented here
*/
public class BookieThread extends Thread {

    private static class BookieUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
        static Logger logger = LoggerFactory.getLogger(BookieUncaughtExceptionHandler.class);

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Uncaught exception in thread " + t.getName(), e);
            Runtime.getRuntime().exit(1);
        }

    }

    public BookieThread (String name) {
        super(name);
        setUncaughtExceptionHandler(new BookieUncaughtExceptionHandler());
    }
}
