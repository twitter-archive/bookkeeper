package org.apache.bookkeeper.bookie;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
* Wrapper that wraps bookie threads
* Any common handing that we require for all bookie threads
* should be implemented here
*/
public class BookieThread extends Thread implements
        Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(BookieThread.class);

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        handleException(t, e);
    }

    public BookieThread(String name) {
        super(name);
        setUncaughtExceptionHandler(this);
    }

    public BookieThread(Runnable thread, String name) {
        super(thread, name);
        setUncaughtExceptionHandler(this);
    }

    /**
     * Handles uncaught exception occurred in thread
     */
    protected void handleException(Thread t, Throwable e) {
        LOG.error("Uncaught exception in thread {}", t.getName(), e);
        if (e instanceof VirtualMachineError) {
            // if any virtual machine error, shutdown the bookie immediately
            Runtime.getRuntime().exit(ExitCode.BOOKIE_EXCEPTION);
        }
    }
}
