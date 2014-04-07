package com.indeed.lsmtree.recordcache;

import fj.P1;
import org.apache.log4j.Logger;

/**
 * @author jplaisance
 */
public final class Put<K,V> implements Operation {

    private static final Logger log = Logger.getLogger(Put.class);

    private final K key;

    private final P1<V> value;

    public Put(K key, P1<V> value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public V getValue() {
        return value._1();
    }
}
