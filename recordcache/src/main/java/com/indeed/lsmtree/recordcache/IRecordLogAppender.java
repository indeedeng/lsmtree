package com.indeed.lsmtree.recordcache;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author jchien
 */
public interface IRecordLogAppender<K, V> {
    long getLastPosition();

    int getMaxSegment();

    File getSegmentPath(int segment);

    void deleteDocs(final Collection<K> ids) throws IOException;

    long write(final K key, final V value) throws IOException;

    void putCheckpoint(long timestamp) throws IOException;

    void flushWriter(Map<String, String> metadata) throws IOException;
}
