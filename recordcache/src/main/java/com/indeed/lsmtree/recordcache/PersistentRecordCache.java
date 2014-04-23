package com.indeed.lsmtree.recordcache;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.indeed.util.core.Either;
import com.indeed.util.core.threads.NamedThreadFactory;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.varexport.Export;
import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.lsmtree.recordlog.RecordFile;
import com.indeed.lsmtree.recordlog.RecordLogDirectory;
import fj.P;
import fj.P2;
import fj.data.Option;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.indeed.util.core.Either.Left;
import static com.indeed.util.core.Either.Right;

/**
 * @author jplaisance
 */
public final class PersistentRecordCache<K, V> implements RecordCache<K, V> {

    private static final Logger log = Logger.getLogger(PersistentRecordCache.class);

    private final Store<K, Long> index;

    private final RecordLogDirectory<Operation> recordLogDirectory;

    private final RecordLogDirectoryPoller.Functions indexUpdateFunctions;

    private final AtomicInteger repairedSegments = new AtomicInteger(0);

    private final Comparator<K> comparator;

    private PersistentRecordCache(
            final Store<K, Long> index,
            final RecordLogDirectory<Operation> recordLogDirectory,
            final File checkpointDir
    ) throws IOException {
        this.index = index;
        this.comparator = index.getComparator();
        this.recordLogDirectory = recordLogDirectory;
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
                    if (puts > 0) log.debug("avg index put time: " + indexPutTime.get() / puts / 1000d + " us");
                    final int deletes = indexDeletes.get();
                    if (deletes > 0) log.debug("avg index delete time: " + indexDeleteTime.get() / deletes / 1000d + " us");
                }

                if (op.getClass() == Put.class) {
                    final Put<K,V> put = (Put)op;
                    final long start = System.nanoTime();
                    synchronized (index) {
                        index.put(put.getKey(), position);
                    }
                    indexPutTime.addAndGet(System.nanoTime()-start);
                    indexPuts.incrementAndGet();
                } else if (op.getClass() == Delete.class) {
                    final Delete<K> delete = (Delete)op;
                    for (K k : delete.getKeys()) {
                        final long start = System.nanoTime();
                        synchronized (index) {
                            index.delete(k);
                        }
                        indexDeleteTime.addAndGet(System.nanoTime()-start);
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

    @Export(name = "repaired-segments")
    public int getRepairedSegments() {
        return repairedSegments.get();
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
            try {
                final long start = System.nanoTime();
                Long position;
                try {
                    position = index.get(key);
                } catch (Exception e) {
                    log.error("index read error while fetching key "+key, e);
                    cacheStats.indexReadErrors++;
                    throw e;
                }
                cacheStats.indexTime += System.nanoTime() - start;
                if (position != null) {
                    Put<K, V> put;
                    try {
                        put = lookupAddress(cacheStats, position);
                        if (comparator.compare(put.getKey(), key) != 0) throw new IOException("keys do not match - expected: "+key+" actual: "+put.getKey());
                    } catch (Exception e) {
                        log.info("exception looking up key: "+key+", attempting repair ", e);
                        try {
                            reindex(position);
                        } catch (IndexReadException e1) {
                            log.error("index read error while fetching key "+key, e1);
                            cacheStats.indexReadErrors++;
                            throw e1;
                        }
                        try {
                            position = index.get(key);
                        } catch (Exception e1) {
                            log.error("index read error while fetching key "+key, e1);
                            cacheStats.indexReadErrors++;
                            throw e1;
                        }
                        put = lookupAddress(cacheStats, position);
                        log.info("reindex successful");
                    }
                    results.put(key, put.getValue());
                }
            } catch (Exception e) {
                log.error("error fetching key: "+key, e);
                cacheStats.recordLogReadErrors++;
            }
        }
        cacheStats.persistentStoreHits = results.size();
        log.debug("persistent store hits: " + (results.size()));
        cacheStats.misses = keys.size()-results.size();
        log.debug("misses: "+(keys.size()-results.size()));
        return results;
    }

    private Put<K, V> lookupAddress(@Nullable final CacheStats cacheStats, final Long position)
            throws IOException {
        final long start1 = System.nanoTime();
        final Operation op = recordLogDirectory.get(position);
        if (cacheStats != null) cacheStats.recordLogTime+=System.nanoTime()-start1;
        if (op.getClass() != Put.class) throw new IOException("class is not Put");
        final Put<K,V> put = (Put) op;
        put.getValue();
        return put;
    }

    public Iterator<Either<Exception, P2<K,V>>> getStreaming(Iterator<K> keys, AtomicInteger progress, AtomicInteger skipped) {
        log.info("starting store lookups");
        LongArrayList addressList = new LongArrayList();
        int notFound = 0;
        while (keys.hasNext()) {
            final K key = keys.next();
            final Long address;
            try {
                address = index.get(key);
            } catch (IOException e) {
                log.error("error", e);
                return Iterators.singletonIterator(Left.<Exception, P2<K,V>>of(new IndexReadException(e)));
            }
            if (address != null) {
                addressList.add(address);
            } else {
                notFound++;
            }
        }
        if (progress != null) progress.addAndGet(notFound);
        if (skipped != null) skipped.addAndGet(notFound);
        log.info("store lookups complete, sorting addresses");

        final long[] addresses = addressList.elements();
        Arrays.sort(addresses, 0, addressList.size());

        log.info("initializing store lookup iterator");
        final BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<Runnable>(100);
        final Iterator<List<Long>> iterable = Iterators.partition(addressList.iterator(), 1000);
        final ExecutorService primerThreads = new ThreadPoolExecutor(
                10,
                10,
                0L,
                TimeUnit.MILLISECONDS,
                taskQueue,
                new NamedThreadFactory("store priming thread", true, log),
                new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                        try {
                            taskQueue.put(r);
                        } catch (InterruptedException e) {
                            log.error("error", e);
                            throw new RuntimeException(e);
                        }
                    }
                }
        );
        final BlockingQueue<List<Either<Exception, P2<K,V>>>> completionQueue = new ArrayBlockingQueue<List<Either<Exception, P2<K,V>>>>(10);
        final AtomicLong runningTasks = new AtomicLong(0);
        final AtomicBoolean taskSubmitterRunning = new AtomicBoolean(true);
        
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (iterable.hasNext()) {
                    runningTasks.incrementAndGet();
                    final List<Long> addressesSublist = iterable.next();
                    primerThreads.submit(new FutureTask<List<Either<Exception, P2<K,V>>>>(new RecordLookupTask(addressesSublist)) {
                        @Override
                        protected void done() {
                            try {
                                completionQueue.put(get());
                            } catch (InterruptedException e) {
                                log.error("error", e);
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                log.error("error", e);
                                throw new RuntimeException(e);
                            }
                        }
                    });
                }
                taskSubmitterRunning.set(false);
            }
        }, "RecordLookupTaskSubmitterThread").start();

        return new Iterator<Either<Exception, P2<K,V>>>() {

            Iterator<Either<Exception, P2<K,V>>> currentIterator;

            @Override
            public boolean hasNext() {
                if (currentIterator != null && currentIterator.hasNext()) return true;
                while (taskSubmitterRunning.get() || runningTasks.get() > 0) {
                    try {
                        final List<Either<Exception, P2<K,V>>> list = completionQueue.poll(1, TimeUnit.SECONDS);
                        if (list != null) {
                            log.debug("remaining: " + runningTasks.decrementAndGet());
                            currentIterator = list.iterator();
                            if (currentIterator.hasNext()) return true;
                        }
                    } catch (InterruptedException e) {
                        log.error("error", e);
                        throw new RuntimeException(e);
                    }
                }
                primerThreads.shutdown();
                return false;
            }

            @Override
            public Either<Exception, P2<K,V>> next() {
                return currentIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private final class RecordLookupTask implements Callable<List<Either<Exception, P2<K,V>>>> {

        private final List<Long> addresses;

        private RecordLookupTask(List<Long> addresses) {
            this.addresses = addresses;
        }

        @Override
        public List<Either<Exception, P2<K,V>>> call() {
            final List<Either<Exception, P2<K,V>>> ret = Lists.newArrayList();
            for (Long address : addresses) {
                try {
                    Put<K, V> put = null;
                    try {
                        put = lookupAddress(null, address);
                    } catch (Exception e) {
                        log.info("exception looking up address: " + address + ", attempting repair", e);
                        reindex(address);
                        log.info("reindex successful");
                    }
                    if (put != null) {
                        ret.add(Right.<Exception, P2<K,V>>of(P.p(put.getKey(), put.getValue())));
                    } else {
                        throw new IOException("record for address " + address + " does not exist for some reason");
                    }
                } catch (Exception e) {
                    ret.add(Left.<Exception, P2<K, V>>of(e));
                }
            }
            return ret;
        }
    }

    private void reindex(long address) throws IndexReadException {
        final int segmentNum = recordLogDirectory.getSegmentNum(address);
        try {
            final
            Option<RecordFile.Reader<Operation>> option = recordLogDirectory.getFileReader(segmentNum);
            for (RecordFile.Reader<Operation> reader : option) {
                try {
                    while (reader.next()) {
                        final Operation op = reader.get();
                        if (op.getClass() == Put.class) {
                            final Put<K, V> put = (Put<K, V>)op;
                            final K key = put.getKey();
                            final long position = reader.getPosition();
                            synchronized (index) {
                                final Long currentAddress;
                                try {
                                    currentAddress = index.get(key);
                                } catch (Exception e) {
                                    throw new IndexReadException(e);
                                }
                                if (currentAddress == null || currentAddress == position) {
                                    continue;
                                }
                                final int currentSegment = recordLogDirectory.getSegmentNum(currentAddress);
                                if (currentSegment == segmentNum) {
                                    index.put(key, position);
                                }
                            }
                        }
                    }
                } finally {
                    reader.close();
                }
            }
        } catch (IndexReadException e) {
            log.error("error", e);
            throw e;
        } catch (Exception e) {
            log.error("error reindexing segment number "+segmentNum, e);
        }
        repairedSegments.incrementAndGet();
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
        private boolean dedicatedIndexPartition;

        private Comparator<K> comparator = new ComparableComparator();
        private RecordLogDirectory<Operation> recordLogDirectory;

        private boolean mlockIndex = false;
        private boolean mlockBloomFilters = false;
        private long bloomFilterMemory = -1;

        public PersistentRecordCache<K,V> build() throws IOException {
            if (indexDir == null) throw new IllegalArgumentException("indexDir must be set");
            if (recordLogDirectory == null) throw new IllegalArgumentException("fileCache must be set");
            if (keySerializer == null) throw new IllegalArgumentException("keySerializer must be set");
            StoreBuilder<K, Long> indexBuilder = new StoreBuilder(indexDir, keySerializer, new LongSerializer());
            indexBuilder.setMaxVolatileGenerationSize(8*1024*1024);
            indexBuilder.setStorageType(StorageType.INLINE);
            indexBuilder.setComparator(comparator);
            indexBuilder.setDedicatedPartition(dedicatedIndexPartition);
            indexBuilder.setMlockFiles(mlockIndex);
            indexBuilder.setMlockBloomFilters(mlockBloomFilters);
            if (bloomFilterMemory >= 0) indexBuilder.setBloomFilterMemory(bloomFilterMemory);
            final Store<K, Long> index = indexBuilder.build();

            return new PersistentRecordCache<K, V>(index, recordLogDirectory, checkpointDir);
        }

        public Builder<K,V> setIndexDir(final File indexDir) {
            this.indexDir = indexDir;
            return this;
        }

        public Builder<K,V> setKeySerializer(final Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
            return this;
        }

        public Builder<K,V> setComparator(final Comparator<K> comparator) {
            this.comparator = comparator;
            return this;
        }

        public Builder<K,V> setRecordLogDirectory(final RecordLogDirectory<Operation> recordLogDirectory) {
            this.recordLogDirectory = recordLogDirectory;
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
