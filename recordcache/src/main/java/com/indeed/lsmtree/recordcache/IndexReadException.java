package com.indeed.lsmtree.recordcache;

/**
* @author jplaisance
*/
public final class IndexReadException extends Exception {

    public IndexReadException() {
    }

    public IndexReadException(final String message) {
        super(message);
    }

    public IndexReadException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public IndexReadException(final Throwable cause) {
        super(cause);
    }
}
