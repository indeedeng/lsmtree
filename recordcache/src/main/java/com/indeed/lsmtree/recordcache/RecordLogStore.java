/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.lsmtree.recordcache;

import java.io.Closeable;
import java.io.IOException;
import com.google.common.io.Closer;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;

/**
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
