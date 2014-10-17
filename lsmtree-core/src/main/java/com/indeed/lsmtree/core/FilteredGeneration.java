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

import com.google.common.collect.Ordering;
import com.indeed.util.core.reference.SharedReference;
import fj.F;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jplaisance
 */
public final class FilteredGeneration<K, V> implements Generation<K, V> {
    private static final Logger log = Logger.getLogger(FilteredGeneration.class);

    private final Generation<K, V> generation;
    private final SharedReference<Closeable> closeable;
    private final K minKey;
    private final boolean minInclusive;
    private final K maxKey;
    private final boolean maxInclusive;
    private final Ordering<K> ordering;
    private final F<Entry<K,V>,Boolean> checkMax;
    private final F<Entry<K,V>,Boolean> checkMin;

    public FilteredGeneration(Generation<K, V> generation, SharedReference<Closeable> closeable, @Nullable K minKey, boolean minInclusive, @Nullable K maxKey, boolean maxInclusive) {
        this.generation = generation;
        this.closeable = closeable;
        this.minKey = minKey;
        this.minInclusive = minInclusive;
        this.maxKey = maxKey;
        this.maxInclusive = maxInclusive;
        this.ordering = Ordering.from(generation.getComparator());
        checkMax = new F<Entry<K, V>, Boolean>() {
            @Override
            public Boolean f(Entry<K, V> kvEntry) {
                final int cmp = ordering.compare(kvEntry.getKey(), FilteredGeneration.this.maxKey);
                return (cmp <= 0) & ((cmp != 0) | FilteredGeneration.this.maxInclusive);
            }
        };
        checkMin = new F<Entry<K, V>, Boolean>() {
            @Override
            public Boolean f(Entry<K, V> kvEntry) {
                final int cmp = ordering.compare(kvEntry.getKey(), FilteredGeneration.this.minKey);
                return (cmp >= 0) & ((cmp != 0) | FilteredGeneration.this.minInclusive);
            }
        };
    }
    
    private boolean checkRange(K key) {
        final int cmpMin = minKey == null ? 1 : ordering.compare(key, minKey);
        if (cmpMin <= 0) {
            if (!minInclusive || cmpMin < 0) {
                return false;
            }
        }
        final int cmpMax = maxKey == null ? -1 : ordering.compare(key, maxKey);
        if (cmpMax >= 0) {
            if (!maxInclusive || cmpMax > 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Entry get(K key) {
        if (!checkRange(key)) return null;
        return generation.get(key);
    }

    public Boolean isDeleted(final K key) {
        if (!checkRange(key)) return null;
        return generation.isDeleted(key);
    }

    @Override
    public Generation<K, V> head(K end, boolean inclusive) {
        return new FilteredGeneration(this, closeable.copy(), null, false, end, inclusive);
    }

    @Override
    public Generation<K, V> tail(K start, boolean inclusive) {
        return new FilteredGeneration(this, closeable.copy(), start, inclusive, null, false);
    }

    @Override
    public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive) {
        return new FilteredGeneration(this, closeable.copy(), start, startInclusive, end, endInclusive);
    }

    @Override
    public Generation<K, V> reverse() {
        return new ReverseGeneration<K, V>(this, closeable.copy());
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        final Iterator<Entry<K, V>> it = minKey == null ? generation.iterator() : generation.iterator(minKey, minInclusive);
        return maxKey == null ? it : ItUtil.span(checkMax, it)._1();
    }

    @Override
    public Iterator<Entry<K, V>> iterator(K start, boolean startInclusive) {
        if (minKey != null) {
            if (start == null) {
                start = minKey;
                startInclusive = minInclusive;
            } else {
                final int cmp = ordering.compare(start, minKey);
                start = cmp < 0 ? minKey : start;
                startInclusive = (cmp == 0) ? (minInclusive & startInclusive) : ((cmp < 0) ? minInclusive : startInclusive);
            }
        } else if (start == null) {
            return maxKey == null ? generation.iterator() : ItUtil.span(checkMax, generation.iterator())._1();
        }
        final Iterator<Entry<K, V>> it = generation.iterator(start, startInclusive);
        return maxKey == null ? it : ItUtil.span(checkMax, it)._1();
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator() {
        final Iterator<Entry<K, V>> it = maxKey == null ? generation.reverseIterator() : generation.reverseIterator(maxKey, maxInclusive);
        return minKey == null ? it : ItUtil.span(checkMin, it)._1();
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator(K start, boolean startInclusive) {
        if (maxKey != null) {
            if (start == null) {
                start = maxKey;
                startInclusive = maxInclusive;
            } else {
                final int cmp = ordering.compare(start, maxKey);
                start = cmp > 0 ? maxKey : start;
                startInclusive = (cmp == 0) ? (maxInclusive & startInclusive) : ((cmp > 0) ? maxInclusive : startInclusive);
            }
        } else if (start == null) {
            return minKey == null ? generation.reverseIterator() : ItUtil.span(checkMin, generation.reverseIterator())._1();
        }
        final Iterator<Entry<K, V>> it = generation.reverseIterator(start, startInclusive);
        return minKey == null ? it : ItUtil.span(checkMin, it)._1();
    }

    @Override
    public Comparator<K> getComparator() {
        return ordering;
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }

    @Override
    public void delete() throws IOException {
        generation.delete();
    }

    @Override
    public void checkpoint(File checkpointPath) throws IOException {
        generation.checkpoint(checkpointPath);
    }

    @Override
    public File getPath() {
        return generation.getPath();
    }

    @Override
    public boolean hasDeletions() {
        return generation.hasDeletions();
    }

    @Override
    public long sizeInBytes() throws IOException {
        return generation.sizeInBytes();
    }

    @Override
    public long size() throws IOException {
        return generation.size();
    }
}
