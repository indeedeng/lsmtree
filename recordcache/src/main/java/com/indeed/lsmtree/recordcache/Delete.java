package com.indeed.lsmtree.recordcache;

import org.apache.log4j.Logger;

import java.util.Collection;

/**
 * @author jplaisance
 */
public final class Delete<K> implements Operation {

    private static final Logger log = Logger.getLogger(Delete.class);

    private final Collection<K> keys;

    public Delete(Collection<K> keys) {
        this.keys = keys;
    }

    public Collection<K> getKeys() {
        return keys;
    }
}
