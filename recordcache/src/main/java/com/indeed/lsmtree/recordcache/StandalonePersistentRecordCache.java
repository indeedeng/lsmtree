package com.indeed.lsmtree.recordcache;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.core.Either;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.varexport.Export;
import fj.P;
import fj.P2;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.indeed.util.core.Either.Left;
import static com.indeed.util.core.Either.Right;

/**
 * This class creates an LSM-tree based cache from recordLog files. We call it "standalone" because
 * it does not need to access the recordLog files after initially loading them.
 *
 * This cache stores the <V> value in the underlying cache directly. This contrasts with
 * {@link PersistentRecordCache}, which creates an underlying cache
 * of key -> (segment number of the recordLog file where the value is located) and then looks up the
 * value in the relevant recordLog file using the segment number.
 *
 * NOTE: Since the dataset will occasionally double in size during compaction, this class is only
 * recommended for use on recordLog datasets of under ~100GB.
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 *
 * @author jplaisance
 * @author dcahill
 */
public final class StandalonePersistentRecordCache<K, V> implements RecordCache<K, V> {

    private static final Logger log = Logger.getLogger(StandalonePersistentRecordCache.class);

    private final Store<K, V> index;

    private final RecordLogDirectoryPoller.Functions indexUpdateFunctions;

    private StandalonePersistentRecordCache(
            final Store<K, V> index,
            final File checkpointDir
    ) throws IOException {
        this.index = index;
        indexUpdateFunctions = new RecordLogDirectoryPoller.Functions() {

            AtomicLong indexPutTime = new AtomicLong(0);

            AtomicLong indexDeleteTime = new AtomicLong(0);

            AtomicInteger indexPuts = new AtomicInteger(0);

            AtomicInteger indexDeletes = new AtomicInteger(0);

            AtomicInteger count = new AtomicInteger(0);

            @Override
            public void process(final long position, Operation op) throws IOException {

                count.incrementAndGet();
                if (count.get() % 1000 == 0) {
                    final int puts = indexPuts.get();
                    if (log.isDebugEnabled() && puts > 0) {
                        log.debug("avg index put time: " + indexPutTime.get() / puts / 1000d + " us");
                    }
                    final int deletes = indexDeletes.get();
                    if (log.isDebugEnabled() && deletes > 0) {
                        log.debug("avg index delete time: " + indexDeleteTime.get() / deletes / 1000d + " us");
                    }
                }

                if (op.getClass() == Put.class) {
                    final Put<K,V> put = (Put)op;
                    final long start = System.nanoTime();
                    synchronized (index) {
                        index.put(put.getKey(), put.getValue());
                    }
                    indexPutTime.addAndGet(System.nanoTime() - start);
                    indexPuts.incrementAndGet();
                } else if (op.getClass() == Delete.class) {
                    final Delete<K> delete = (Delete)op;
                    for (K k : delete.getKeys()) {
                        final long start = System.nanoTime();
                        synchronized (index) {
                            index.delete(k);
                        }
                        indexDeleteTime.addAndGet(System.nanoTime() - start);
                        indexDeletes.incrementAndGet();
                    }
                } else if (op.getClass() == Checkpoint.class) {
                    final Checkpoint checkpoint = (Checkpoint)op;
                    if (checkpointDir != null) {
                        sync();
                        index.checkpoint(new File(checkpointDir, String.valueOf(checkpoint.getTimestamp())));
                    }
                } else {
                    log.warn("operation class unknown");
                }
            }

            @Override
            public void sync() throws IOException {
                final long start = System.nanoTime();
                index.sync();
                log.debug("sync time: " + (System.nanoTime() - start) / 1000d + " us");
            }
        };
    }

    @Export(name = "index-active-space-usage")
    public long getIndexActiveSpaceUsage() throws IOException {
        return index.getActiveSpaceUsage();
    }

    @Export(name = "index-total-space-usage")
    public long getIndexTotalSpaceUsage() throws IOException {
        return index.getTotalSpaceUsage();
    }

    @Export(name = "index-reserverd-space-usage")
    public long getIndexReservedSpaceUsage() {
        return index.getReservedSpaceUsage();
    }

    @Export(name = "index-free-space")
    public long getIndexFreeSpace() throws IOException {
        return index.getFreeSpace();
    }

    public V get(K key, CacheStats cacheStats) {
        final Map<K, V> results = getAll(Collections.singleton(key), cacheStats);
        if (results.size() > 0) {
            return results.get(key);
        }
        return null;
    }

    public Map<K, V> getAll(Collection<K> keys, CacheStats cacheStats) {
        final Map<K, V> results = Maps.newHashMap();
        for (K key : keys) {
            final long start = System.nanoTime();

            try {
                V value = index.get(key);
                results.put(key, value);
            } catch (Exception e) {
                log.error("index read error while fetching key " + key, e);
                cacheStats.indexReadErrors++;
            }
            cacheStats.indexTime += System.nanoTime() - start;
        }
        cacheStats.misses = keys.size() - results.size();
        log.debug("misses: " + (keys.size() - results.size()));
        return results;
    }

    public Iterator<Either<Exception, P2<K,V>>> getStreaming(Iterator<K> keys, AtomicInteger progress, AtomicInteger skipped) {
        log.info("starting store lookups");
        final List<Either<Exception, P2<K,V>>> ret = Lists.newArrayList();
        int notFound = 0;
        while (keys.hasNext()) {
            final K key = keys.next();
            final V value;
            try {
                value = index.get(key);
            } catch (IOException e) {
                log.error("error", e);
                return Iterators.singletonIterator(Left.<Exception, P2<K,V>>of(new IndexReadException(e)));
            }
            if (value != null) {
                ret.add(Right.<Exception, P2<K,V>>of(P.p(key, value)));
            } else {
                notFound++;
            }
        }
        if (progress != null) progress.addAndGet(notFound);
        if (skipped != null) skipped.addAndGet(notFound);
        log.info("store lookups complete");

        return ret.iterator();
    }

    @Override
    public RecordLogDirectoryPoller.Functions getFunctions() {
        return indexUpdateFunctions;
    }

    @Override
    public void close() throws IOException {
        index.close();
    }

    public void waitForCompactions() throws InterruptedException {
        index.waitForCompactions();
    }

    public static class Builder<K,V> {
        private File indexDir;
        private File checkpointDir;
        private Serializer<K> keySerializer;
        private Serializer<V> valueSerializer;
        private boolean dedicatedIndexPartition;

        private Comparator<K> comparator = new ComparableComparator();

        private boolean mlockIndex = false;
        private boolean mlockBloomFilters = false;
        private long bloomFilterMemory = -1;

        public StandalonePersistentRecordCache<K,V> build() throws IOException {
            if (indexDir == null) throw new IllegalArgumentException("indexDir must be set");
            if (keySerializer == null) throw new IllegalArgumentException("keySerializer must be set");
            if (valueSerializer == null) throw new IllegalArgumentException("valueSerializer must be set");
            SnappyCodec codec = new SnappyCodec();
            StoreBuilder<K, V> indexBuilder = new StoreBuilder<K, V>(indexDir, keySerializer, valueSerializer);
            indexBuilder.setMaxVolatileGenerationSize(8*1024*1024);
            indexBuilder.setCodec(codec);
            indexBuilder.setStorageType(StorageType.BLOCK_COMPRESSED);
            indexBuilder.setComparator(comparator);
            indexBuilder.setDedicatedPartition(dedicatedIndexPartition);
            indexBuilder.setMlockFiles(mlockIndex);
            indexBuilder.setMlockBloomFilters(mlockBloomFilters);
            if (bloomFilterMemory >= 0) indexBuilder.setBloomFilterMemory(bloomFilterMemory);
            final Store<K, V> index = indexBuilder.build();

            return new StandalonePersistentRecordCache<K, V>(index, checkpointDir);
        }

        public Builder<K,V> setIndexDir(final File indexDir) {
            this.indexDir = indexDir;
            return this;
        }

        public Builder<K,V> setKeySerializer(final Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K,V> setValueSerializer(final Serializer<V> valueSerializer) {
            this.valueSerializer = valueSerializer;
            return this;
        }

        public Builder<K,V> setComparator(final Comparator<K> comparator) {
            this.comparator = comparator;
            return this;
        }

        public Builder<K,V> setCheckpointDir(final File checkpointDir) {
            this.checkpointDir = checkpointDir;
            return this;
        }

        public boolean isDedicatedIndexPartition() {
            return dedicatedIndexPartition;
        }

        public Builder<K,V> setDedicatedIndexPartition(final boolean dedicatedIndexPartition) {
            this.dedicatedIndexPartition = dedicatedIndexPartition;
            return this;
        }

        public boolean isMlockIndex() {
            return mlockIndex;
        }

        public Builder<K,V> setMlockIndex(final boolean mlockIndex) {
            this.mlockIndex = mlockIndex;
            return this;
        }

        public boolean isMlockBloomFilters() {
            return mlockBloomFilters;
        }

        public Builder<K,V> setMlockBloomFilters(final boolean mlockBloomFilters) {
            this.mlockBloomFilters = mlockBloomFilters;
            return this;
        }

        public long getBloomFilterMemory() {
            return bloomFilterMemory;
        }

        public Builder<K,V> setBloomFilterMemory(final long bloomFilterMemory) {
            this.bloomFilterMemory = bloomFilterMemory;
            return this;
        }
    }
}
