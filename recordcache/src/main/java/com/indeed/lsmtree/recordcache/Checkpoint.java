package com.indeed.lsmtree.recordcache;

/**
 * @author jplaisance
 */
public final class Checkpoint implements Operation {

    private final long timestamp;

    public Checkpoint(final long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
