package com.indeed.lsmtree.core;

import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class RuntimeIOException extends RuntimeException {
    private static final Logger log = Logger.getLogger(RuntimeIOException.class);

    public RuntimeIOException() {
    }

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuntimeIOException(Throwable cause) {
        super(cause);
    }
}
