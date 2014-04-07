package com.indeed.lsmtree.recordlog;

import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author jplaisance
 */
public final class ConsistencyException extends IOException {

    private static final Logger log = Logger.getLogger(ConsistencyException.class);

    public ConsistencyException() {
        super();
    }

    public ConsistencyException(final String message) {
        super(message);
    }

    public ConsistencyException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public ConsistencyException(final Throwable cause) {
        super(cause);
    }
}
