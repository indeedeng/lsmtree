package com.indeed.lsmtree.recordcache;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;

/**
 * @author jplaisance
 */
public interface RecordCache<K,V> extends Closeable {
    public V get(K key, CacheStats cacheStats);

    public Map<K, V> getAll(Collection<K> keys, CacheStats cacheStats);

    public RecordLogDirectoryPoller.Functions getFunctions();
}
