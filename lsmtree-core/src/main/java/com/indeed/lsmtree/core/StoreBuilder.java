package com.indeed.lsmtree.core;

import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.serialization.Serializer;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;

/**
 * @author jplaisance
 */
public final class StoreBuilder<K,V> {

    private static final Logger log = Logger.getLogger(StoreBuilder.class);

    private final File root;
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private Comparator<K> comparator = new ComparableComparator();
    private long maxVolatileGenerationSize = 8*1024*1024;
    private StorageType storageType = StorageType.INLINE;
    private CompressionCodec codec = null;
    private boolean readOnly = false;
    private boolean dedicatedPartition = false;
    private long reservedSpaceThreshold = 1024*1024*256;
    private boolean mlockFiles = false;
    private boolean mlockBloomFilters = false;
    private long bloomFilterMemory = 1024*1024*32;

    public StoreBuilder(final File root, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
        this.root = root;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public Comparator<K> getComparator() {
        return comparator;
    }

    public StoreBuilder<K,V> setComparator(final Comparator<K> comparator) {
        this.comparator = comparator;
        return this;
    }

    public long getMaxVolatileGenerationSize() {
        return maxVolatileGenerationSize;
    }

    public StoreBuilder<K,V> setMaxVolatileGenerationSize(final long maxVolatileGenerationSize) {
        this.maxVolatileGenerationSize = maxVolatileGenerationSize;
        return this;
    }

    public StorageType getStorageType() {
        return storageType;
    }

    public StoreBuilder<K,V> setStorageType(final StorageType storageType) {
        this.storageType = storageType;
        return this;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public StoreBuilder<K,V> setCodec(final CompressionCodec codec) {
        this.codec = codec;
        return this;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public StoreBuilder<K,V> setReadOnly(final boolean readOnly) {
        this.readOnly = readOnly;
        return this;
    }

    public boolean isDedicatedPartition() {
        return dedicatedPartition;
    }

    public StoreBuilder<K,V> setDedicatedPartition(final boolean dedicatedPartition) {
        this.dedicatedPartition = dedicatedPartition;
        return this;
    }

    public long getReservedSpaceThreshold() {
        return reservedSpaceThreshold;
    }

    public StoreBuilder<K,V> setReservedSpaceThreshold(final long reservedSpaceThreshold) {
        this.reservedSpaceThreshold = reservedSpaceThreshold;
        return this;
    }

    public boolean isMlockFiles() {
        return mlockFiles;
    }

    public StoreBuilder<K,V> setMlockFiles(final boolean mlockFiles) {
        this.mlockFiles = mlockFiles;
        return this;
    }

    public boolean isMlockBloomFilters() {
        return mlockBloomFilters;
    }

    public StoreBuilder<K,V> setMlockBloomFilters(final boolean mlockBloomFilters) {
        this.mlockBloomFilters = mlockBloomFilters;
        return this;
    }

    public long getBloomFilterMemory() {
        return bloomFilterMemory;
    }

    public StoreBuilder<K,V> setBloomFilterMemory(final long bloomFilterMemory) {
        this.bloomFilterMemory = bloomFilterMemory;
        return this;
    }

    public Store<K,V> build() throws IOException {
        return new Store<K, V>(
                root,
                keySerializer,
                valueSerializer,
                comparator,
                maxVolatileGenerationSize,
                storageType,
                codec,
                readOnly,
                dedicatedPartition,
                reservedSpaceThreshold,
                mlockFiles,
                bloomFilterMemory,
                mlockBloomFilters
        );
    }
}
