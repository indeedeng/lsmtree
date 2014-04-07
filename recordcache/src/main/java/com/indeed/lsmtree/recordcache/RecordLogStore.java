package com.indeed.lsmtree.recordcache;

import java.io.Closeable;
import java.io.IOException;
import com.google.common.io.Closer;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;

/**
 * todo need better name because it's not a store itself
 * @author jchien
 */
public class RecordLogStore<K, V> implements Closeable {
    private final Store<K, V> store;

    private final RecordLogDirectoryPoller recordLogDirectoryPoller;

    private final RecordLogDirectory recordLogDirectory;

    private final Closer closer = Closer.create();

    public RecordLogStore(Store<K, V> store, RecordLogDirectoryPoller recordLogDirectoryPoller, RecordLogDirectory recordLogDirectory) {
        this.store = closer.register(store);
        this.recordLogDirectory = closer.register(recordLogDirectory);
        this.recordLogDirectoryPoller = closer.register(recordLogDirectoryPoller);
    }

    public Store<K, V> getStore() {
        return store;
    }

    public RecordLogDirectoryPoller getRecordLogDirectoryPoller() {
        return recordLogDirectoryPoller;
    }

    public RecordLogDirectory getRecordLogDirectory() {
        return recordLogDirectory;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
