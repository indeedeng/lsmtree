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
 package com.indeed.lsmtree.core;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.io.BufferedFileDataOutputStream;
import com.indeed.util.serialization.Serializer;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * @author jplaisance
 */
public final class VolatileGeneration<K, V> implements Generation<K,V> {

    private static final Logger log = Logger.getLogger(VolatileGeneration.class);

    private final TransactionLog.Writer transactionLog;

    private final Object deleted;

    private final ConcurrentSkipListMap<K, Object> map;

    private final File logPath;

    private final Serializer<K> keySerializer;

    private final Serializer<V> valueSerializer;
    
    private final Ordering<K> ordering;

    private final SharedReference<Closeable> stuffToClose;

    public VolatileGeneration(File logPath, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator) throws IOException {
        this(logPath, keySerializer, valueSerializer, comparator, false);
    }

    public VolatileGeneration(File logPath, Serializer<K> keySerializer, Serializer<V> valueSerializer, Comparator<K> comparator, boolean loadExistingReadOnly) throws IOException {
        this.ordering = Ordering.from(comparator);
        map = new ConcurrentSkipListMap(comparator);
        this.logPath = logPath;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        deleted = new Object();
        if (loadExistingReadOnly) {
            if (!logPath.exists()) throw new IllegalArgumentException(logPath.getAbsolutePath()+" does not exist");
            transactionLog = null;
            replayTransactionLog(logPath, true);
        } else {
            if (logPath.exists()) throw new IllegalArgumentException("to load existing logs set loadExistingReadOnly to true or create a new log and use replayTransactionLog");
            transactionLog = new TransactionLog.Writer(logPath, keySerializer, valueSerializer, false);
        }
        stuffToClose = SharedReference.create((Closeable)new Closeable() {
            public void close() throws IOException {
                closeWriter();
            }
        });
    }

    public void replayTransactionLog(File path) throws IOException {
        replayTransactionLog(path, false);
    }

    private void replayTransactionLog(File path, boolean readOnly) throws IOException {
        final TransactionLog.Reader<K, V> reader = new TransactionLog.Reader(path, keySerializer, valueSerializer);
        try {
            while (reader.next()) {
                final K key = reader.getKey();
                switch (reader.getType()) {
                    case PUT:
                        final V value = reader.getValue();
                        if (!readOnly) transactionLog.put(key, value);
                        map.put(key, value);
                        break;
                    case DELETE:
                        if (!readOnly) transactionLog.delete(key);
                        map.put(key, deleted);
                        break;
                }
            }
        } catch (TransactionLog.LogClosedException e) {
            //shouldn't happen here ever
            log.error("log is closed and it shouldn't be", e);
            throw new IOException(e);
        } finally {
            reader.close();
            if (!readOnly) transactionLog.sync();
        }
    }

    public void put(K key, V value) throws IOException, TransactionLog.LogClosedException {
        transactionLog.put(key, value);
        map.put(key, value);
    }

    public void delete(K key) throws IOException, TransactionLog.LogClosedException {
        transactionLog.delete(key);
        map.put(key, deleted);
    }

    @Override
    public Entry<K, V> get(final K key) {
        final Object value = map.get(key);
        if (value == null) return null;
        if (value == deleted) return Entry.createDeleted(key);
        return Entry.create(key, (V)value);
    }

    @Override
    public Boolean isDeleted(final K key) {
        final Entry<K, V> result = get(key);
        return result == null ? null : (result.isDeleted() ? Boolean.TRUE : Boolean.FALSE);
    }

    @Override
    public Generation<K, V> head(K endKey, boolean inclusive) {
        return new FilteredGeneration<K, V>(this, stuffToClose.copy(), null, false, endKey, inclusive);
    }

    @Override
    public Generation<K, V> tail(K startKey, boolean inclusive) {
        return new FilteredGeneration<K, V>(this, stuffToClose.copy(), startKey, inclusive, null, false);
    }

    @Override
    public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive) {
        return new FilteredGeneration<K, V>(this, stuffToClose.copy(), start, startInclusive, end, endInclusive);
    }

    @Override
    public Generation<K, V> reverse() {
        return new ReverseGeneration<K, V>(this, stuffToClose.copy());
    }

    @Override
    public long size() {
        return map.size();
    }

    @Override
    public long sizeInBytes() throws IOException {
        return transactionLog == null ? 0 : transactionLog.sizeInBytes();
    }

    @Override
    public boolean hasDeletions() {
        return true;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return iterator(null, false);
    }

    @Override
    public Iterator<Entry<K, V>> iterator(final @Nullable K start, final boolean startInclusive) {
        return new AbstractIterator<Entry<K, V>>() {

            boolean initialized = false;
            K key;

            @Override
            protected Entry<K, V> computeNext() {
                final Map.Entry<K, Object> entry;
                if (!initialized) {
                    initialized = true;
                    if (start == null) {
                        entry = map.firstEntry();
                    } else if (startInclusive) {
                        entry = map.ceilingEntry(start);
                    } else {
                        entry = map.higherEntry(start);
                    }
                } else {
                    entry = map.higherEntry(key);
                }
                if (entry == null) {
                    return endOfData();
                }
                key = entry.getKey();
                final Object value = entry.getValue();
                if (value == deleted) return Entry.createDeleted(key);
                return Entry.create(key, (V)value);
            }
        };
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator() {
        return reverseIterator(null, false);
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator(final @Nullable K start, final boolean startInclusive) {
        return new AbstractIterator<Entry<K, V>>() {

            boolean initialized = false;
            K key;

            @Override
            protected Entry<K, V> computeNext() {
                final Map.Entry<K, Object> entry;
                if (!initialized) {
                    initialized = true;
                    if (start == null) {
                        entry = map.lastEntry();
                    } else if (startInclusive) {
                        entry = map.floorEntry(start);
                    } else {
                        entry = map.lowerEntry(start);
                    }
                } else {
                    entry = map.lowerEntry(key);
                }
                if (entry == null) {
                    return endOfData();
                }
                key = entry.getKey();
                final Object value = entry.getValue();
                if (value == deleted) return Entry.createDeleted(key);
                return Entry.create(key, (V)value);
            }
        };
    }

    @Override
    public Comparator<K> getComparator() {
        return ordering;
    }

    public void sync() throws IOException {
        if (transactionLog != null) transactionLog.sync();
    }

    public void closeWriter() throws IOException {
        if (transactionLog != null) {
            transactionLog.close();
        }
    }

    @Override
    public void close() throws IOException {
        stuffToClose.close();
    }

    @Override
    public void delete() throws IOException {
        log.info("deleting "+getPath());
        getPath().delete();
    }

    @Override
    public File getPath() {
        return logPath;
    }

    @Override
    public void checkpoint(final File checkpointPath) throws IOException {
        BufferedFileDataOutputStream out = null;
        InputStream in = null;
        try {
            out = new BufferedFileDataOutputStream(new File(checkpointPath, logPath.getName()));
            in = new BufferedInputStream(new FileInputStream(logPath), 65536);
            ByteStreams.copy(in, out);
            out.sync();
        } finally {
            if (out != null) out.close();
            if (in != null) in.close();
        }
    }
}
