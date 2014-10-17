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

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jplaisance
 */
public interface Generation<K,V> extends Closeable {
    public @Nullable Entry<K,V> get(K key);

    public @Nullable Boolean isDeleted(K key);

    public Generation<K, V> head(K end, boolean inclusive);

    public Generation<K, V> tail(K start, boolean inclusive);
    
    public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive);
    
    public Generation<K, V> reverse();

    public Iterator<Entry<K, V>> iterator();

    public Iterator<Entry<K, V>> iterator(K start, boolean startInclusive);

    public Iterator<Entry<K, V>> reverseIterator();

    public Iterator<Entry<K, V>> reverseIterator(K start, boolean startInclusive);

    public Comparator<K> getComparator();

    public long size() throws IOException;

    public long sizeInBytes() throws IOException;

    public boolean hasDeletions();

    public File getPath();

    public void checkpoint(File checkpointPath) throws IOException;

    public void delete() throws IOException;

    public static final class Entry<K, V> {

        public static <K,V> Entry<K,V> createDeleted(K key) {
            return new Entry<K, V>(key, null);
        }

        public static <K,V> Entry<K,V> create(K key, V value) {
            return new Entry<K, V>(key, value);
        }

        private final K key;
        private final V value;

        private Entry(final K key, final @Nullable V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return Preconditions.checkNotNull(value);
        }

        public boolean isDeleted() {
            return value == null;
        }

        @Override
        public String toString() {
            return "Entry{" +
                    "key=" + key +
                    ", value=" + value +
                    ", isDeleted=" + isDeleted() +
                    '}';
        }
    }
}
