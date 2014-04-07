package com.indeed.lsmtree.core;

import com.google.common.collect.Ordering;
import com.indeed.util.core.reference.SharedReference;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

/**
 * @author jplaisance
 */
public final class ReverseGeneration<K, V> implements Generation<K, V> {
    private static final Logger log = Logger.getLogger(ReverseGeneration.class);

    private final Generation<K, V> wrapped;
    private final SharedReference<Closeable> closeable;

    public ReverseGeneration(Generation<K, V> wrapped, SharedReference<Closeable> closeable) {
        this.wrapped = wrapped;
        this.closeable = closeable;
    }

    @Override
    public Entry<K, V> get(K key) {
        return wrapped.get(key);
    }

    public Boolean isDeleted(final K key) {
        return wrapped.isDeleted(key);
    }

    @Override
    public Generation<K, V> head(K end, boolean inclusive) {
        return new FilteredGeneration<K, V>(this, closeable.copy(), null, false, end, inclusive);
    }

    @Override
    public Generation<K, V> tail(K start, boolean inclusive) {
        return new FilteredGeneration<K, V>(this, closeable.copy(), start, inclusive, null, false);
    }

    @Override
    public Generation<K, V> slice(K start, boolean startInclusive, K end, boolean endInclusive) {
        return new FilteredGeneration<K, V>(this, closeable.copy(), start, startInclusive, end, endInclusive);
    }

    @Override
    public Generation<K, V> reverse() {
        return wrapped;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return wrapped.reverseIterator();
    }

    @Override
    public Iterator<Entry<K, V>> iterator(K start, boolean startInclusive) {
        return wrapped.reverseIterator(start, startInclusive);
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator() {
        return wrapped.iterator();
    }

    @Override
    public Iterator<Entry<K, V>> reverseIterator(K start, boolean startInclusive) {
        return wrapped.iterator(start, startInclusive);
    }

    @Override
    public Comparator<K> getComparator() {
        return Ordering.from(wrapped.getComparator()).reverse();
    }

    @Override
    public long size() throws IOException {
        return wrapped.size();
    }

    @Override
    public long sizeInBytes() throws IOException {
        return wrapped.sizeInBytes();
    }

    @Override
    public boolean hasDeletions() {
        return wrapped.hasDeletions();
    }

    @Override
    public File getPath() {
        return wrapped.getPath();
    }

    @Override
    public void checkpoint(File checkpointPath) throws IOException {
        wrapped.checkpoint(checkpointPath);
    }

    @Override
    public void delete() throws IOException {
        wrapped.delete();
    }

    @Override
    public void close() throws IOException {
        closeable.close();
    }
}
